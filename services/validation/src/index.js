'use strict'

const { createLogger, sendMessage, createSQSWorker, enrichLead, isApprovedOrder } = require('@gex/shared')
const { Pool } = require('pg')

const logger = createLogger('validation')

const RAW_QUEUE_URL       = process.env.SQS_RAW_QUEUE_URL
const VALID_QUEUE_URL     = process.env.SQS_VALID_QUEUE_URL
const DISCARDED_QUEUE_URL = process.env.SQS_DISCARDED_QUEUE_URL

const db = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL }) : null

// ─── Audit trail ─────────────────────────────────────────────────

async function auditEvent(orderId, correlationId, eventType, payload, errorDetail = null, durationMs = null) {
  if (!db) return
  try {
    await db.query(
      `INSERT INTO lead_audit_trail (order_id, correlation_id, event_type, service, payload, error_detail, duration_ms)
       VALUES ($1,$2,$3,'validation',$4,$5,$6)`,
      [orderId, correlationId, eventType, JSON.stringify(payload), errorDetail, durationMs]
    )
  } catch (err) {
    logger.warn({ err: err.message }, 'Audit trail write failed (non-critical)')
  }
}

// ─── Handler principal ───────────────────────────────────────────

async function handleEvent(event, { messageId, attempt }) {
  const startMs = Date.now()
  const orderId  = event.order_id
  const corrId   = event.correlation_id

  logger.info({ order_id: orderId, correlation_id: corrId, attempt, messageId }, 'Validating event')

  // 1. Filtrar apenas order.approved + payment approved
  if (!isApprovedOrder(event)) {
    const reason = `payment_status=${event.payment_status} | event=${event.event}`
    const discarded = {
      ...event,
      discard_reason: reason,
      processed_at: new Date().toISOString(),
    }

    await sendMessage(DISCARDED_QUEUE_URL, discarded)
    await auditEvent(orderId, corrId, 'discarded', discarded, reason)
    logger.info({ order_id: orderId, reason }, 'Event discarded — not approved')
    return
  }

  // 2. Enriquecer e validar campos
  const enriched = enrichLead(event)

  // 3. E-mail inválido → descartado (não pode enviar para plataforma de marketing sem email)
  if (!enriched.email_valid) {
    const reason = `invalid_email: "${enriched.email}"`
    const discarded = { ...enriched, discard_reason: reason }

    await sendMessage(DISCARDED_QUEUE_URL, discarded)
    await auditEvent(orderId, corrId, 'discarded', discarded, reason)
    logger.warn({ order_id: orderId, email: enriched.email }, 'Event discarded — invalid email')
    return
  }

  // 4. Válido → enfileira para delivery
  // JobId garante idempotência: mesmo order_id não vai duplicar
  await sendMessage(VALID_QUEUE_URL, enriched, {
    messageGroupId:         orderId,
    messageDeduplicationId: `valid-${orderId}`,
  })

  const durationMs = Date.now() - startMs
  await auditEvent(orderId, corrId, 'validated', enriched, null, durationMs)
  logger.info({ order_id: orderId, email: enriched.email, durationMs }, 'Lead validated → delivery queue')
}

// ─── Start ────────────────────────────────────────────────────────

const worker = createSQSWorker({
  queueUrl:    RAW_QUEUE_URL,
  concurrency: 10,
  handler:     handleEvent,
  logger,
})

logger.info({ queue: RAW_QUEUE_URL }, '🔍 Validation worker starting...')
worker.start().catch((err) => {
  logger.error({ err }, 'Fatal error in validation worker')
  process.exit(1)
})

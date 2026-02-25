'use strict'

const { createLogger, sendMessage, createSQSWorker, enrichLead, isApprovedOrder, metrics, startMetricsServer } = require('@gex/shared')
const { Pool } = require('pg')

const logger = createLogger('validation')

const RAW_QUEUE_URL       = process.env.SQS_RAW_QUEUE_URL
const VALID_QUEUE_URL     = process.env.SQS_VALID_QUEUE_URL
const DISCARDED_QUEUE_URL = process.env.SQS_DISCARDED_QUEUE_URL

const db = process.env.DATABASE_URL ? new Pool({ connectionString: process.env.DATABASE_URL }) : null

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

async function handleEvent(event, { messageId, attempt }) {
  const end = metrics.validationDuration.startTimer()
  const startMs = Date.now()
  const orderId = event.order_id
  const corrId  = event.correlation_id

  metrics.activeWorkers.set({ service: 'validation' }, 1)
  logger.info({ order_id: orderId, correlation_id: corrId, attempt, messageId }, 'Validating event')

  if (!isApprovedOrder(event)) {
    const reason = `payment_status=${event.payment_status} | event=${event.event}`
    const discarded = { ...event, discard_reason: reason, processed_at: new Date().toISOString() }

    await sendMessage(DISCARDED_QUEUE_URL, discarded)
    await auditEvent(orderId, corrId, 'discarded', discarded, reason)
    metrics.eventsDiscarded.inc({ reason: 'not_approved' })
    end()
    logger.info({ order_id: orderId, reason }, 'Event discarded — not approved')
    return
  }

  const enriched = enrichLead(event)

  if (!enriched.email_valid) {
    const reason = `invalid_email: "${enriched.email}"`
    const discarded = { ...enriched, discard_reason: reason }

    await sendMessage(DISCARDED_QUEUE_URL, discarded)
    await auditEvent(orderId, corrId, 'discarded', discarded, reason)
    metrics.eventsDiscarded.inc({ reason: 'invalid_email' })
    end()
    logger.warn({ order_id: orderId, email: enriched.email }, 'Event discarded — invalid email')
    return
  }

  await sendMessage(VALID_QUEUE_URL, enriched, {
    messageGroupId:         orderId,
    messageDeduplicationId: `valid-${orderId}`,
  })

  const durationMs = Date.now() - startMs
  await auditEvent(orderId, corrId, 'validated', enriched, null, durationMs)
  metrics.eventsValidated.inc()
  end()
  logger.info({ order_id: orderId, email: enriched.email, durationMs }, 'Lead validated → delivery queue')
}

// Servidor de métricas standalone (validation não tem HTTP)
startMetricsServer(parseInt(process.env.METRICS_PORT || '9100', 10))

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
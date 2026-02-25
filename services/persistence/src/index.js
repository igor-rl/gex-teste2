'use strict'

const { Pool } = require('pg')
const { createLogger, createSQSWorker } = require('@gex/shared')

const logger = createLogger('persistence')

const RESULTS_QUEUE_URL   = process.env.SQS_RESULTS_QUEUE_URL
const DISCARDED_QUEUE_URL = process.env.SQS_DISCARDED_QUEUE_URL

const db = new Pool({
  connectionString: process.env.DATABASE_URL,
  max:              20,
  idleTimeoutMillis: 30000,
})

// ─── Upsert idempotente ───────────────────────────────────────────
// ON CONFLICT garante que reprocessamento não cria duplicatas

async function upsertLead(data) {
  await db.query(`
    INSERT INTO lead_control
      (order_id, correlation_id, email, phone, full_name, product_name, product_niche,
       country, order_value, bottles_qty, funnel_source, purchase_date,
       status, discard_reason, error_message, email_valid, phone_valid,
       attempt_count, processed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
    ON CONFLICT (order_id) DO UPDATE SET
      status        = EXCLUDED.status,
      error_message = EXCLUDED.error_message,
      attempt_count = lead_control.attempt_count + 1,
      processed_at  = EXCLUDED.processed_at,
      updated_at    = NOW()
  `, [
    data.order_id,
    data.correlation_id,
    data.email,
    data.phone,
    data.full_name       || null,
    data.product_name    || null,
    data.product_niche   || null,
    data.country         || null,
    data.order_value     || null,
    data.bottles_qty     || null,
    data.funnel_source   || null,
    data.purchase_date   || null,
    data.status,
    data.discard_reason  || null,
    data.error_message   || null,
    data.email_valid     ?? false,
    data.phone_valid     ?? false,
    1,
    data.processed_at || new Date().toISOString(),
  ])
}

async function insertAudit(data, eventType) {
  try {
    await db.query(`
      INSERT INTO lead_audit_trail
        (order_id, correlation_id, event_type, service, payload, error_detail, duration_ms)
      VALUES ($1,$2,$3,'persistence',$4,$5,$6)
    `, [
      data.order_id,
      data.correlation_id,
      eventType,
      JSON.stringify({ status: data.status, product: data.product_name }),
      data.error_message || null,
      data.duration_ms   || null,
    ])
  } catch (err) {
    logger.warn({ err: err.message }, 'Audit trail insert failed (non-critical)')
  }
}

// ─── Worker: resultados de delivery ─────────────────────────────

async function handleDeliveryResult(data, { messageId }) {
  logger.info({ order_id: data.order_id, status: data.status, messageId }, 'Persisting delivery result')
  await upsertLead(data)
  await insertAudit(data, data.status === 'enviado' ? 'sent' : 'failed')
  logger.info({ order_id: data.order_id, status: data.status }, 'Lead persisted')
}

// ─── Worker: leads descartados ───────────────────────────────────

async function handleDiscarded(data, { messageId }) {
  const record = {
    order_id:       data.order_id,
    correlation_id: data.correlation_id,
    email:          data.email || data.customer_email,
    phone:          data.phone || data.customer_phone,
    full_name:      data.full_name,
    product_name:   data.product_name,
    product_niche:  data.product_niche,
    country:        data.country || data.customer_country,
    order_value:    data.order_value,
    bottles_qty:    data.bottles_qty,
    funnel_source:  data.funnel_source,
    purchase_date:  data.purchase_date || data.created_at,
    status:         'descartado',
    discard_reason: data.discard_reason,
    email_valid:    data.email_valid ?? false,
    phone_valid:    data.phone_valid ?? false,
    processed_at:   data.processed_at || new Date().toISOString(),
  }

  logger.info({ order_id: record.order_id, reason: record.discard_reason, messageId }, 'Persisting discarded lead')
  await upsertLead(record)
  await insertAudit(record, 'discarded')
}

// ─── Start ────────────────────────────────────────────────────────

const deliveryWorker = createSQSWorker({
  queueUrl:    RESULTS_QUEUE_URL,
  concurrency: 20,
  handler:     handleDeliveryResult,
  logger,
})

const discardedWorker = createSQSWorker({
  queueUrl:    DISCARDED_QUEUE_URL,
  concurrency: 20,
  handler:     handleDiscarded,
  logger,
})

logger.info({ results_queue: RESULTS_QUEUE_URL, discarded_queue: DISCARDED_QUEUE_URL }, '💾 Persistence workers starting...')

Promise.all([
  deliveryWorker.start(),
  discardedWorker.start(),
]).catch((err) => {
  logger.error({ err }, 'Fatal error in persistence service')
  process.exit(1)
})

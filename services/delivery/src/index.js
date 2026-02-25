'use strict'

const { fetch } = require('undici')
const { createLogger, sendMessage, createSQSWorker, metrics, startMetricsServer } = require('@gex/shared')

const logger = createLogger('delivery')

const VALID_QUEUE_URL   = process.env.SQS_VALID_QUEUE_URL
const RESULTS_QUEUE_URL = process.env.SQS_RESULTS_QUEUE_URL
const WEBHOOK_URL       = process.env.WEBHOOK_URL

const MAX_RETRIES   = 3
const BASE_DELAY_MS = 1000

async function withExponentialBackoff(fn, maxRetries, baseDelayMs) {
  let lastErr
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastErr = err
      if (attempt < maxRetries) {
        const delay = baseDelayMs * Math.pow(2, attempt) + Math.random() * 200
        metrics.deliveryRetries.inc()
        logger.warn({ attempt: attempt + 1, maxRetries, delay, err: err.message }, 'Retrying after backoff')
        await sleep(delay)
      }
    }
  }
  throw lastErr
}

async function sendToWebhook(lead) {
  const payload = {
    order_id:      lead.order_id,
    email:         lead.email,
    full_name:     lead.full_name,
    phone:         lead.phone,
    country:       lead.country,
    product_name:  lead.product_name,
    product_niche: lead.product_niche,
    order_value:   lead.order_value,
    bottles_qty:   lead.bottles_qty,
    purchase_date: lead.purchase_date,
    funnel_source: lead.funnel_source,
    tags:          lead.tags || [],
  }

  return withExponentialBackoff(async () => {
    const controller = new AbortController()
    const timer      = setTimeout(() => controller.abort(), 10_000)
    try {
      const res = await fetch(WEBHOOK_URL, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload),
        signal:  controller.signal,
      })
      if (!res.ok) throw new Error(`HTTP ${res.status}: ${res.statusText}`)
      return { success: true, status: res.status, payload }
    } finally {
      clearTimeout(timer)
    }
  }, MAX_RETRIES, BASE_DELAY_MS)
}

async function handleLead(lead, { messageId, attempt }) {
  const end = metrics.deliveryDuration.startTimer()
  const startMs = Date.now()
  metrics.activeWorkers.set({ service: 'delivery' }, 1)
  logger.info({ order_id: lead.order_id, correlation_id: lead.correlation_id, attempt, messageId }, 'Delivering lead')

  let result

  try {
    await sendToWebhook(lead)
    const durationMs = Date.now() - startMs

    result = {
      order_id:       lead.order_id,
      correlation_id: lead.correlation_id,
      email:          lead.email,
      phone:          lead.phone,
      full_name:      lead.full_name,
      product_name:   lead.product_name,
      email_valid:    lead.email_valid,
      phone_valid:    lead.phone_valid,
      status:         'enviado',
      error_message:  null,
      processed_at:   new Date().toISOString(),
      duration_ms:    durationMs,
    }

    metrics.leadsDelivered.inc()
    end()
    logger.info({ order_id: lead.order_id, durationMs }, '✅ Lead delivered')

  } catch (err) {
    const durationMs = Date.now() - startMs
    result = {
      order_id:       lead.order_id,
      correlation_id: lead.correlation_id,
      email:          lead.email,
      phone:          lead.phone,
      full_name:      lead.full_name,
      product_name:   lead.product_name,
      email_valid:    lead.email_valid,
      phone_valid:    lead.phone_valid,
      status:         'erro',
      error_message:  err.message,
      processed_at:   new Date().toISOString(),
      duration_ms:    durationMs,
    }

    metrics.leadsFailed.inc()
    end()
    logger.error({ order_id: lead.order_id, err: err.message, durationMs }, '❌ Delivery failed after all retries')
  }

  await sendMessage(RESULTS_QUEUE_URL, result)
}

// Servidor de métricas standalone
startMetricsServer(parseInt(process.env.METRICS_PORT || '9101', 10))

const worker = createSQSWorker({
  queueUrl:    VALID_QUEUE_URL,
  concurrency: 5,
  handler:     handleLead,
  logger,
})

logger.info({ queue: VALID_QUEUE_URL, webhook: WEBHOOK_URL }, '📤 Delivery worker starting...')
worker.start().catch((err) => {
  logger.error({ err }, 'Fatal error in delivery worker')
  process.exit(1)
})

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }
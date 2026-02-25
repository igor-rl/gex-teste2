'use strict'

const Fastify        = require('fastify')
const { Pool }       = require('pg')
const cors           = require('@fastify/cors')
const { createLogger, getQueueAttributes, metrics, metricsHandler } = require('@gex/shared')

const logger = createLogger('observability')
const db = new Pool({ connectionString: process.env.DATABASE_URL })

const QUEUE_URLS = {
  raw:       process.env.SQS_RAW_QUEUE_URL,
  valid:     process.env.SQS_VALID_QUEUE_URL,
  discarded: process.env.SQS_DISCARDED_QUEUE_URL,
  results:   process.env.SQS_RESULTS_QUEUE_URL,
  dlq:       process.env.SQS_DLQ_URL,
}

async function getQueueMetrics(name, url) {
  if (!url) return null
  try {
    const attrs = await getQueueAttributes(url)
    const available = parseInt(attrs.ApproximateNumberOfMessages || '0', 10)
    const inflight  = parseInt(attrs.ApproximateNumberOfMessagesNotVisible || '0', 10)
    const delayed   = parseInt(attrs.ApproximateNumberOfMessagesDelayed || '0', 10)

    // Atualiza gauges do Prometheus
    metrics.queueMessagesAvailable.set({ queue: name }, available)
    metrics.queueMessagesInflight.set({ queue: name }, inflight)

    return { name, url, messages_available: available, messages_inflight: inflight, messages_delayed: delayed }
  } catch (err) {
    return { name, url, error: err.message }
  }
}

// Atualiza gauges a cada 5s em background
async function pollQueues() {
  while (true) {
    await Promise.all(Object.entries(QUEUE_URLS).map(([name, url]) => getQueueMetrics(name, url)))
    await sleep(5000)
  }
}

async function start() {
  const app = Fastify({ logger: false })
  await app.register(cors, { origin: '*' })

  // ─── Prometheus metrics ──────────────────────────────────────
  app.get('/prometheus', metricsHandler())

  // ─── Health ──────────────────────────────────────────────────
  app.get('/health', async () => {
    let dbOk = false
    try { await db.query('SELECT 1'); dbOk = true } catch {}
    return { status: dbOk ? 'ok' : 'degraded', service: 'observability', db: dbOk, ts: new Date().toISOString() }
  })

  // ─── Queue metrics ────────────────────────────────────────────
  app.get('/metrics/queues', async () => {
    const m = await Promise.all(Object.entries(QUEUE_URLS).map(([name, url]) => getQueueMetrics(name, url)))
    return { queues: m.filter(Boolean), ts: new Date().toISOString() }
  })

  // ─── DB summary ───────────────────────────────────────────────
  app.get('/metrics/summary', async () => {
    const { rows } = await db.query(`
      SELECT status,
        COUNT(*) AS total,
        COUNT(*) FILTER (WHERE email_valid = FALSE) AS invalid_email,
        COUNT(*) FILTER (WHERE phone_valid = FALSE) AS invalid_phone,
        ROUND(AVG(order_value), 2) AS avg_order_value,
        ROUND(SUM(order_value), 2) AS total_revenue
      FROM lead_control GROUP BY status ORDER BY status
    `)
    const summary = {}
    for (const r of rows) {
      summary[r.status] = {
        total: +r.total, invalid_email: +r.invalid_email, invalid_phone: +r.invalid_phone,
        avg_order_value: +r.avg_order_value, total_revenue: +r.total_revenue,
      }
    }
    const { rows: totals } = await db.query('SELECT COUNT(*) AS total FROM lead_control')
    return { summary, total_records: +totals[0].total, ts: new Date().toISOString() }
  })

  app.get('/metrics/latency', async () => {
    const { rows } = await db.query(`
      SELECT
        ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60), 2) AS avg_minutes,
        ROUND(MIN(EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60), 2) AS min_minutes,
        ROUND(MAX(EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60), 2) AS max_minutes,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60) AS p95_minutes,
        COUNT(*) AS sample_size
      FROM lead_control
      WHERE status = 'enviado' AND purchase_date IS NOT NULL AND processed_at > NOW() - INTERVAL '24 hours'
    `)
    return { latency: rows[0], ts: new Date().toISOString() }
  })

  app.get('/metrics/error-rate', async () => {
    const { rows } = await db.query(`
      SELECT DATE_TRUNC('hour', processed_at) AS hour_bucket, COUNT(*) AS total,
        COUNT(*) FILTER (WHERE status = 'erro') AS errors,
        ROUND(COUNT(*) FILTER (WHERE status = 'erro') * 100.0 / NULLIF(COUNT(*),0), 2) AS error_rate_pct
      FROM lead_control WHERE processed_at > NOW() - INTERVAL '24 hours'
      GROUP BY 1 ORDER BY 1 DESC LIMIT 24
    `)
    const critical = rows.find(r => +r.error_rate_pct > 5)
    return { hourly: rows, critical_alert: !!critical, ts: new Date().toISOString() }
  })

  app.get('/metrics/reconciliation', async () => {
    const [queueM, { rows: dbRows }] = await Promise.all([
      getQueueMetrics('raw', QUEUE_URLS.raw),
      db.query(`SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE status='enviado') AS sent,
        COUNT(*) FILTER (WHERE status='erro') AS errors, COUNT(*) FILTER (WHERE status='descartado') AS discarded
        FROM lead_control`),
    ])
    return { db: { total: +dbRows[0].total, ...dbRows[0] }, queue_inflight: queueM?.messages_inflight || 0, queue_waiting: queueM?.messages_available || 0, ts: new Date().toISOString() }
  })

  app.get('/metrics/recent-errors', async () => {
    const { rows } = await db.query(`SELECT order_id, email, product_name, error_message, attempt_count, processed_at
      FROM lead_control WHERE status = 'erro' ORDER BY processed_at DESC LIMIT 50`)
    return { errors: rows, count: rows.length }
  })

  app.get('/audit/:orderId', async (req) => {
    const { rows } = await db.query(`SELECT event_type, service, payload, error_detail, duration_ms, ts
      FROM lead_audit_trail WHERE order_id = $1 ORDER BY ts ASC`, [req.params.orderId])
    return { order_id: req.params.orderId, trail: rows }
  })

  app.get('/metrics/dlq', async () => {
    const m = await getQueueMetrics('dlq', QUEUE_URLS.dlq)
    return { dlq: m, alert: (m?.messages_available || 0) > 0, ts: new Date().toISOString() }
  })

  app.get('/metrics/pipeline-health', async () => {
    const { rows } = await db.query(`SELECT MAX(processed_at) AS last_processed,
      EXTRACT(EPOCH FROM (NOW() - MAX(processed_at))) / 60 AS minutes_since_last,
      COUNT(*) FILTER (WHERE processed_at > NOW() - INTERVAL '1 hour') AS last_hour_count FROM lead_control`)
    const minutesSince = +rows[0].minutes_since_last
    return { last_processed: rows[0].last_processed, minutes_since_last: Math.round(minutesSince),
      last_hour_count: +rows[0].last_hour_count, pipeline_stalled: minutesSince > 30, alert: minutesSince > 30, ts: new Date().toISOString() }
  })

  const PORT = parseInt(process.env.PORT || '3005', 10)
  await app.listen({ port: PORT, host: '::' })
  logger.info({ port: PORT }, '📊 Observability service started')

  // Inicia polling de filas em background
  pollQueues().catch(err => logger.error({ err }, 'Queue polling error'))
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

start().catch((err) => {
  logger.error({ err }, 'Fatal error in observability service')
  process.exit(1)
})
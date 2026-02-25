'use strict'

const client = require('prom-client')

// Habilita métricas padrão do Node.js (CPU, memória, event loop, GC, etc.)
const register = new client.Registry()
client.collectDefaultMetrics({ register, prefix: 'nodejs_' })

// ─── Contadores de negócio ────────────────────────────────────────

const eventsIngested = new client.Counter({
  name: 'gex_events_ingested_total',
  help: 'Total de eventos recebidos pelo ingestion',
  registers: [register],
})

const eventsValidated = new client.Counter({
  name: 'gex_events_validated_total',
  help: 'Total de eventos validados e enviados para delivery',
  registers: [register],
})

const eventsDiscarded = new client.Counter({
  name: 'gex_events_discarded_total',
  help: 'Total de eventos descartados na validação',
  labelNames: ['reason'],
  registers: [register],
})

const leadsDelivered = new client.Counter({
  name: 'gex_leads_delivered_total',
  help: 'Total de leads entregues com sucesso ao webhook',
  registers: [register],
})

const leadsFailed = new client.Counter({
  name: 'gex_leads_failed_total',
  help: 'Total de leads que falharam na entrega',
  registers: [register],
})

const leadsPersisted = new client.Counter({
  name: 'gex_leads_persisted_total',
  help: 'Total de leads persistidos no banco',
  labelNames: ['status'],
  registers: [register],
})

// ─── Histogramas de latência ──────────────────────────────────────

const deliveryDuration = new client.Histogram({
  name: 'gex_delivery_duration_ms',
  help: 'Duração da entrega ao webhook em ms',
  buckets: [50, 100, 250, 500, 1000, 2000, 5000, 10000],
  registers: [register],
})

const validationDuration = new client.Histogram({
  name: 'gex_validation_duration_ms',
  help: 'Duração da validação de um evento em ms',
  buckets: [1, 5, 10, 25, 50, 100, 250],
  registers: [register],
})

const ingestionDuration = new client.Histogram({
  name: 'gex_ingestion_duration_ms',
  help: 'Duração do processamento de ingestion em ms',
  buckets: [5, 10, 25, 50, 100, 250, 500],
  registers: [register],
})

// ─── Gauges de filas ─────────────────────────────────────────────

const queueMessagesAvailable = new client.Gauge({
  name: 'gex_queue_messages_available',
  help: 'Mensagens disponíveis em cada fila SQS',
  labelNames: ['queue'],
  registers: [register],
})

const queueMessagesInflight = new client.Gauge({
  name: 'gex_queue_messages_inflight',
  help: 'Mensagens em processamento (inflight) em cada fila SQS',
  labelNames: ['queue'],
  registers: [register],
})

// ─── Gauge de workers ativos ──────────────────────────────────────

const activeWorkers = new client.Gauge({
  name: 'gex_active_workers',
  help: 'Workers SQS ativos processando mensagens',
  labelNames: ['service'],
  registers: [register],
})

// ─── Métricas de retry ────────────────────────────────────────────

const deliveryRetries = new client.Counter({
  name: 'gex_delivery_retries_total',
  help: 'Total de retentativas de entrega',
  registers: [register],
})

// ─── Helper: expor /metrics em qualquer servidor HTTP ────────────

function metricsHandler() {
  return async (req, reply) => {
    reply.header('Content-Type', register.contentType)
    return register.metrics()
  }
}

// ─── Helper: servidor de métricas standalone (workers sem HTTP) ──

function startMetricsServer(port) {
  const http = require('http')
  const server = http.createServer(async (req, res) => {
    if (req.url === '/metrics') {
      res.writeHead(200, { 'Content-Type': register.contentType })
      res.end(await register.metrics())
    } else if (req.url === '/health') {
      res.writeHead(200, { 'Content-Type': 'application/json' })
      res.end(JSON.stringify({ status: 'ok' }))
    } else {
      res.writeHead(404)
      res.end()
    }
  })
  server.listen(port, '0.0.0.0', () => {
    console.log(`Metrics server listening on :${port}`)
  })
  return server
}

module.exports = {
  register,
  metrics: {
    eventsIngested,
    eventsValidated,
    eventsDiscarded,
    leadsDelivered,
    leadsFailed,
    leadsPersisted,
    deliveryDuration,
    validationDuration,
    ingestionDuration,
    queueMessagesAvailable,
    queueMessagesInflight,
    activeWorkers,
    deliveryRetries,
  },
  metricsHandler,
  startMetricsServer,
}
'use strict'

const Fastify = require('fastify')
const multipart = require('@fastify/multipart')
const { parse } = require('csv-parse/sync')
const { v4: uuidv4 } = require('uuid')
const { createLogger, sendMessage, sendMessageBatch, putObject } = require('@gex/shared')

const logger = createLogger('ingestion')

const RAW_QUEUE_URL = process.env.SQS_RAW_QUEUE_URL
const S3_BUCKET     = process.env.S3_BUCKET || 'gex-sales-batch'

// ─── Normaliza registro CSV para evento canônico ──────────────────

function normalizeRecord(record, correlationId) {
  return {
    ...record,
    correlation_id: correlationId,
    received_at:    new Date().toISOString(),
    // Campos numéricos podem vir como string no CSV
    price_usd: record.price_usd,
    quantity:  record.quantity,
  }
}

async function start() {
  const app = Fastify({ logger: false, bodyLimit: 104857600 }) // 100MB

  await app.register(multipart, { limits: { fileSize: 104857600 } })

  // ─── Real-time: 1 evento ────────────────────────────────────────
  app.post('/events', {
    schema: {
      body: {
        type: 'object',
        required: ['order_id'],
        properties: {
          order_id: { type: 'string' },
        },
      },
    },
  }, async (req, reply) => {
    const event = req.body
    const correlationId = uuidv4()
    const payload = normalizeRecord(event, correlationId)

    await sendMessage(
      RAW_QUEUE_URL,
      payload,
      {
        messageGroupId:          event.order_id,
        messageDeduplicationId:  `${event.order_id}-${correlationId}`,
      }
    )

    logger.info({ order_id: event.order_id, correlation_id: correlationId }, 'Event ingested → SQS')
    return reply.status(202).send({ correlation_id: correlationId, status: 'queued' })
  })

  // ─── Batch: CSV via HTTP multipart ─────────────────────────────
  app.post('/batch', async (req, reply) => {
    let csvContent
    const contentType = req.headers['content-type'] || ''

    if (contentType.includes('multipart/form-data')) {
      const data = await req.file()
      csvContent = (await data.toBuffer()).toString('utf8')
    } else {
      csvContent = req.body?.toString()
    }

    if (!csvContent) return reply.status(400).send({ error: 'CSV obrigatório' })

    let records
    try {
      records = parse(csvContent, {
        columns:          true,
        skip_empty_lines: true,
        trim:             true,
        bom:              true,
      })
    } catch (err) {
      return reply.status(400).send({ error: 'CSV inválido', detail: err.message })
    }

    const batchId      = uuidv4()
    const s3Key        = `batches/${new Date().toISOString().slice(0,10)}/${batchId}.csv`

    // 1. Persiste CSV original no S3 (rastreabilidade)
    await putObject(S3_BUCKET, s3Key, csvContent, 'text/csv')
    logger.info({ batchId, records: records.length, s3Key }, 'Batch uploaded to S3')

    // 2. Enfileira cada evento no SQS (em chunks de 10)
    const messages = records.map((record) => {
      const correlationId = uuidv4()
      return {
        id:             record.order_id || uuidv4(),
        body:           normalizeRecord(record, correlationId),
        messageGroupId: record.order_id || batchId,
      }
    })

    await sendMessageBatch(RAW_QUEUE_URL, messages)

    logger.info({ batchId, queued: messages.length }, 'Batch enqueued → SQS')
    return reply.status(202).send({ batch_id: batchId, queued: messages.length, s3_key: s3Key })
  })

  // ─── Batch: upload direto para S3 (webhook S3 → SQS) ──────────
  app.post('/batch/s3', async (req, reply) => {
    const data = await req.file()
    if (!data) return reply.status(400).send({ error: 'Arquivo obrigatório' })

    const csvContent = (await data.toBuffer()).toString('utf8')
    const batchId    = uuidv4()
    const s3Key      = `batches/${new Date().toISOString().slice(0,10)}/${batchId}.csv`

    await putObject(S3_BUCKET, s3Key, csvContent, 'text/csv')

    logger.info({ batchId, s3Key, filename: data.filename }, 'File uploaded to S3 — SQS notification will trigger processing')
    return reply.status(202).send({ batch_id: batchId, s3_key: s3Key, status: 'uploaded' })
  })

  // ─── Health ────────────────────────────────────────────────────
  app.get('/health', async () => ({
    status:  'ok',
    service: 'ingestion',
    ts:      new Date().toISOString(),
    queue:   RAW_QUEUE_URL,
  }))

  const PORT = process.env.PORT || 3001
  await app.listen({ port: PORT, host: '0.0.0.0' })
  logger.info({ port: PORT }, '🚀 Ingestion service started')
}

start().catch((err) => {
  logger.error({ err }, 'Fatal error in ingestion service')
  process.exit(1)
})

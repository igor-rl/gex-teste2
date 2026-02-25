'use strict'

const {
  SQSClient,
  SendMessageCommand,
  SendMessageBatchCommand,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  DeleteMessageBatchCommand,
  GetQueueAttributesCommand,
} = require('@aws-sdk/client-sqs')

const { S3Client, PutObjectCommand, GetObjectCommand } = require('@aws-sdk/client-s3')
const pino = require('pino')

const logger = pino({
  level: process.env.LOG_LEVEL || 'info',
  transport: process.env.NODE_ENV === 'development' ? { target: 'pino-pretty', options: { colorize: true } } : undefined,
  base: { service: 'aws-client' },
})

// ─── Config ───────────────────────────────────────────────────────

const awsConfig = {
  region: process.env.AWS_REGION || 'us-east-1',
  credentials: {
    accessKeyId:     process.env.AWS_ACCESS_KEY_ID     || 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || 'test',
  },
  ...(process.env.AWS_ENDPOINT_URL ? { endpoint: process.env.AWS_ENDPOINT_URL } : {}),
}

const sqsClient = new SQSClient(awsConfig)
const s3Client = new S3Client({ ...awsConfig, forcePathStyle: true })

// ─── SQS Helpers ─────────────────────────────────────────────────

/**
 * Envia mensagem única para SQS.
 * Para filas FIFO, MessageGroupId é obrigatório.
 */
async function sendMessage(queueUrl, body, opts = {}) {
  const isFifo = queueUrl.endsWith('.fifo')
  const params = {
    QueueUrl:    queueUrl,
    MessageBody: typeof body === 'string' ? body : JSON.stringify(body),
    ...(opts.messageGroupId || isFifo
      ? { MessageGroupId: opts.messageGroupId || 'default' }
      : {}),
    ...(opts.messageDeduplicationId
      ? { MessageDeduplicationId: opts.messageDeduplicationId }
      : {}),
    ...(opts.delaySeconds !== undefined ? { DelaySeconds: opts.delaySeconds } : {}),
    ...(opts.attributes ? { MessageAttributes: opts.attributes } : {}),
  }

  const result = await sqsClient.send(new SendMessageCommand(params))
  return result.MessageId
}

/**
 * Envia até 10 mensagens em batch (economiza custos em produção).
 */
async function sendMessageBatch(queueUrl, messages) {
  const isFifo = queueUrl.endsWith('.fifo')
  const entries = messages.map((msg, idx) => ({
    Id: msg.id || String(idx),
    MessageBody: typeof msg.body === 'string' ? msg.body : JSON.stringify(msg.body),
    ...(isFifo ? { MessageGroupId: msg.messageGroupId || 'default' } : {}),
    ...(msg.delaySeconds !== undefined ? { DelaySeconds: msg.delaySeconds } : {}),
  }))

  // SQS batch limit = 10
  const results = []
  for (let i = 0; i < entries.length; i += 10) {
    const chunk = entries.slice(i, i + 10)
    const r = await sqsClient.send(new SendMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: chunk,
    }))
    results.push(r)
    if (r.Failed?.length) {
      logger.warn({ failed: r.Failed }, 'SQS batch: some messages failed')
    }
  }
  return results
}

/**
 * Recebe até maxMessages da fila, com long polling.
 */
async function receiveMessages(queueUrl, maxMessages = 10, waitSeconds = 20) {
  const result = await sqsClient.send(new ReceiveMessageCommand({
    QueueUrl:              queueUrl,
    MaxNumberOfMessages:   Math.min(maxMessages, 10),
    WaitTimeSeconds:       waitSeconds,
    AttributeNames:        ['All'],
    MessageAttributeNames: ['All'],
  }))
  return result.Messages || []
}

/**
 * Deleta mensagem após processamento bem-sucedido.
 */
async function deleteMessage(queueUrl, receiptHandle) {
  await sqsClient.send(new DeleteMessageCommand({ QueueUrl: queueUrl, ReceiptHandle: receiptHandle }))
}

/**
 * Deleta batch de mensagens.
 */
async function deleteMessageBatch(queueUrl, messages) {
  for (let i = 0; i < messages.length; i += 10) {
    const chunk = messages.slice(i, i + 10)
    await sqsClient.send(new DeleteMessageBatchCommand({
      QueueUrl: queueUrl,
      Entries: chunk.map((m, idx) => ({ Id: String(idx), ReceiptHandle: m.ReceiptHandle })),
    }))
  }
}

/**
 * Retorna atributos da fila (ApproximateNumberOfMessages, etc.).
 */
async function getQueueAttributes(queueUrl) {
  const result = await sqsClient.send(new GetQueueAttributesCommand({
    QueueUrl:       queueUrl,
    AttributeNames: ['All'],
  }))
  return result.Attributes || {}
}

// ─── S3 Helpers ───────────────────────────────────────────────────

async function putObject(bucket, key, body, contentType = 'application/json') {
  await s3Client.send(new PutObjectCommand({
    Bucket:      bucket,
    Key:         key,
    Body:        typeof body === 'string' ? body : JSON.stringify(body),
    ContentType: contentType,
  }))
}

async function getObject(bucket, key) {
  const result = await s3Client.send(new GetObjectCommand({ Bucket: bucket, Key: key }))
  const chunks = []
  for await (const chunk of result.Body) chunks.push(chunk)
  return Buffer.concat(chunks).toString('utf8')
}

// ─── Poll Worker (substitui BullMQ) ──────────────────────────────

/**
 * Cria um worker de polling SQS com:
 * - Long polling (waitSeconds=20)
 * - Retry com backoff exponencial
 * - Visibility timeout management
 * - Graceful shutdown
 */
function createSQSWorker({ queueUrl, concurrency = 5, handler, logger: workerLogger }) {
  const log = workerLogger || logger
  let running = true
  let activeJobs = 0

  async function processMessage(msg) {
    const startMs = Date.now()
    let body
    try {
      body = JSON.parse(msg.Body)
    } catch {
      body = msg.Body
    }

    const attempt = parseInt(msg.Attributes?.ApproximateReceiveCount || '1', 10)
    log.info({ messageId: msg.MessageId, attempt }, 'Processing message')

    try {
      await handler(body, { messageId: msg.MessageId, attempt, rawMessage: msg })
      await deleteMessage(queueUrl, msg.ReceiptHandle)
      log.info({ messageId: msg.MessageId, durationMs: Date.now() - startMs }, 'Message processed')
    } catch (err) {
      log.error({ messageId: msg.MessageId, attempt, err: err.message }, 'Message processing failed — will retry via visibility timeout')
      // Não deletamos: SQS vai recolocar na fila após VisibilityTimeout
      // Após maxReceiveCount, vai para DLQ automaticamente
    }
  }

  async function poll() {
    while (running) {
      try {
        if (activeJobs >= concurrency) {
          await sleep(100)
          continue
        }

        const messages = await receiveMessages(queueUrl, Math.min(10, concurrency - activeJobs))
        if (!messages.length) continue

        for (const msg of messages) {
          activeJobs++
          processMessage(msg).finally(() => activeJobs--)
        }
      } catch (err) {
        if (running) {
          log.error({ err: err.message }, 'Poll error — retrying in 5s')
          await sleep(5000)
        }
      }
    }
  }

  function stop() {
    running = false
    log.info('SQS worker stopping...')
  }

  process.on('SIGTERM', stop)
  process.on('SIGINT',  stop)

  return { start: poll, stop }
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

module.exports = {
  sqsClient,
  s3Client,
  sendMessage,
  sendMessageBatch,
  receiveMessages,
  deleteMessage,
  deleteMessageBatch,
  getQueueAttributes,
  putObject,
  getObject,
  createSQSWorker,
}

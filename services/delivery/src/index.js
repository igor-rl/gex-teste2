'use strict'

const { fetch } = require('undici')
const { createLogger, sendMessage, createSQSWorker, metrics, startMetricsServer } = require('@gex/shared')

const logger = createLogger('delivery')

const VALID_QUEUE_URL   = process.env.SQS_VALID_QUEUE_URL
const RESULTS_QUEUE_URL = process.env.SQS_RESULTS_QUEUE_URL
const WEBHOOK_URL       = process.env.WEBHOOK_URL

const MAX_RETRIES   = 3
const BASE_DELAY_MS = 200

// ─── Throttle Adaptativo (AIMD) ───────────────────────────────────────────────
//
// AIMD = Additive Increase, Multiplicative Decrease
// O mesmo algoritmo que o TCP usa para controle de congestionamento.
//
// Regra simples:
//   sucesso    → sobe RPS devagar (+0.1 a cada janela)
//   429        → desce rápido (divide por 2)
//   timeout    → desce mais agressivo (divide por 3)
//   500        → mantém, pode ser erro pontual da API
//
// Isso faz o delivery "aprender" o ritmo que a API aguenta
// sem precisar de configuração manual ou API Gateway.
//
const RPS_MIN     = parseFloat(process.env.RPS_MIN     || '0.5')   // mínimo absoluto
const RPS_MAX     = parseFloat(process.env.RPS_MAX     || '20.0')  // teto — nunca passa disso
const RPS_INITIAL = parseFloat(process.env.WEBHOOK_RPS || '1.5')   // ponto de partida

// Estado do throttle adaptativo
const throttle = {
  rps:         RPS_INITIAL,
  lastRequest: 0,

  // Janela de observação: conta eventos nos últimos N ms
  window: {
    success:  0,
    rateLimit: 0,
    timeout:  0,
    error:    0,
    resetAt:  Date.now() + 10_000, // avalia a cada 10s
  },

  // Registra resultado e ajusta RPS ao fim de cada janela
  record(outcome) {
    this.window[outcome]++
    const now = Date.now()

    if (now >= this.window.resetAt) {
      this._adjust()
      this.window = { success: 0, rateLimit: 0, timeout: 0, error: 0, resetAt: now + 10_000 }
    }
  },

  _adjust() {
    const { success, rateLimit, timeout } = this.window
    const total = success + rateLimit + timeout
    if (total === 0) return

    const prevRps = this.rps

    if (timeout > 0) {
      // Timeout = API sofrendo — recua agressivo
      this.rps = Math.max(RPS_MIN, this.rps / 3)
    } else if (rateLimit > 0) {
      // 429 = passou do limite — recua moderado
      this.rps = Math.max(RPS_MIN, this.rps / 2)
    } else if (success === total) {
      // Tudo certo — sobe devagar
      this.rps = Math.min(RPS_MAX, this.rps + 0.1)
    }

    if (this.rps !== prevRps) {
      logger.info({
        prev_rps:   prevRps.toFixed(2),
        new_rps:    this.rps.toFixed(2),
        window:     this.window,
      }, '⚖️  Adaptive throttle adjusted')
    }
  },

  // Espera o intervalo mínimo antes de deixar o próximo request passar
  async wait() {
    const intervalMs = 1000 / this.rps
    const now        = Date.now()
    const wait       = intervalMs - (now - this.lastRequest)
    if (wait > 0) await sleep(wait)
    this.lastRequest = Date.now()
  },
}

// ─── RateLimitError ───────────────────────────────────────────────────────────
//
// 429 recebido da API. Carrega o Retry-After em ms.
// O backoff usa esse valor em vez do exponencial padrão
// para respeitar o tempo que a API pediu pra esperar.
//
class RateLimitError extends Error {
  constructor(retryAfterMs) {
    super(`Rate limited — retry after ${retryAfterMs}ms`)
    this.name         = 'RateLimitError'
    this.retryAfterMs = retryAfterMs
  }
}

// ─── Backoff exponencial ──────────────────────────────────────────────────────
async function withExponentialBackoff(fn, maxRetries, baseDelayMs) {
  let lastErr
  for (let attempt = 0; attempt <= maxRetries; attempt++) {
    try {
      return await fn()
    } catch (err) {
      lastErr = err
      if (attempt < maxRetries) {
        const isRateLimit = err instanceof RateLimitError
        const delay = isRateLimit
          ? err.retryAfterMs
          : baseDelayMs * Math.pow(2, attempt) + Math.random() * 200

        metrics.deliveryRetries.inc()
        logger.warn({
          attempt:    attempt + 1,
          maxRetries,
          delay,
          webhook:    WEBHOOK_URL,
          rate_limit: isRateLimit,
          current_rps: throttle.rps.toFixed(2),
          err:        err.message,
        }, isRateLimit ? '🚦 Rate limited — waiting before retry' : '🔁 Retrying after backoff')

        await sleep(delay)
      }
    }
  }
  throw lastErr
}

// ─── Envio para o webhook ─────────────────────────────────────────────────────
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
    // Throttle adaptativo — respeita o RPS atual antes de cada tentativa
    await throttle.wait()

    const controller = new AbortController()
    const timer      = setTimeout(() => controller.abort(), 10_000)

    try {
      const res = await fetch(WEBHOOK_URL, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload),
        signal:  controller.signal,
      })

      if (res.status === 429) {
        throttle.record('rateLimit')
        const retryAfterSec = res.headers.get('retry-after')
        const retryAfterMs  = retryAfterSec ? parseInt(retryAfterSec) * 1000 : 60_000
        throw new RateLimitError(retryAfterMs)
      }

      if (!res.ok) {
        throttle.record('error')
        throw new Error(`HTTP ${res.status}: ${res.statusText}`)
      }

      throttle.record('success')
      return { success: true, status: res.status, payload }

    } catch (err) {
      // AbortError = timeout do nosso lado
      if (err.name === 'AbortError') {
        throttle.record('timeout')
        throw new Error('Request timed out after 10s')
      }
      throw err
    } finally {
      clearTimeout(timer)
    }
  }, MAX_RETRIES, BASE_DELAY_MS)
}

// ─── Handler de cada mensagem da fila ────────────────────────────────────────
async function handleLead(lead, { messageId, attempt }) {
  const end     = metrics.deliveryDuration.startTimer()
  const startMs = Date.now()

  metrics.activeWorkers.set({ service: 'delivery' }, 1)
  logger.info({
    order_id:       lead.order_id,
    correlation_id: lead.correlation_id,
    attempt,
    messageId,
    current_rps:    throttle.rps.toFixed(2),
  }, 'Delivering lead')

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
    logger.info({ order_id: lead.order_id, durationMs, current_rps: throttle.rps.toFixed(2) }, '✅ Lead delivered')

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
    logger.error({
      order_id:    lead.order_id,
      err:         err.message,
      webhook:     WEBHOOK_URL,
      current_rps: throttle.rps.toFixed(2),
      durationMs,
    }, '❌ Delivery failed after all retries')
  }

  await sendMessage(RESULTS_QUEUE_URL, result)
}

// ─── Inicialização ────────────────────────────────────────────────────────────
startMetricsServer(parseInt(process.env.METRICS_PORT || '9101', 10))

const worker = createSQSWorker({
  queueUrl:    VALID_QUEUE_URL,
  concurrency: 3,
  handler:     handleLead,
  logger,
})

logger.info({
  queue:       VALID_QUEUE_URL,
  webhook:     WEBHOOK_URL,
  rps_initial: RPS_INITIAL,
  rps_min:     RPS_MIN,
  rps_max:     RPS_MAX,
  concurrency: 3,
}, '📤 Delivery worker starting (adaptive throttle enabled)')

worker.start().catch((err) => {
  logger.error({ err }, 'Fatal error in delivery worker')
  process.exit(1)
})

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }
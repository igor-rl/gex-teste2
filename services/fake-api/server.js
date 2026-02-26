'use strict'

const express = require('express')
const app = express()
app.use(express.json())

// ─── Config mutável em runtime ────────────────────────────────────────────────
//
// Todos os parâmetros de comportamento ficam aqui.
// Os endpoints /config/* sobrescrevem esses valores sem precisar reiniciar.
//
let config = {
  RATE_LIMIT: parseInt(process.env.RATE_LIMIT || '100', 10),
  P_ERROR:    parseFloat(process.env.P_ERROR   || '0.05'),
  P_TIMEOUT:  parseFloat(process.env.P_TIMEOUT || '0.03'),
  P_SLOW:     parseFloat(process.env.P_SLOW    || '0.05'),
  profile:    'default',
}

// ─── Perfis prontos ───────────────────────────────────────────────────────────
const PROFILES = {
  reallyfast: {
    RATE_LIMIT: 2000,
    P_ERROR:    0.00,
    P_TIMEOUT:  0.00,
    P_SLOW:     0.00,
    profile:    'reallyfast',
  },
  fast: {
    RATE_LIMIT: 500,
    P_ERROR:    0.01,
    P_TIMEOUT:  0.00,
    P_SLOW:     0.01,
    profile:    'fast',
  },
  slow: {
    RATE_LIMIT: 30,
    P_ERROR:    0.05,
    P_TIMEOUT:  0.02,
    P_SLOW:     0.40,   // 40% das respostas são lentas (2s–5s)
    profile:    'slow',
  },
  unstable: {
    RATE_LIMIT: 50,
    P_ERROR:    0.20,   // 20% erro
    P_TIMEOUT:  0.10,   // 10% timeout
    P_SLOW:     0.30,   // 30% lento
    profile:    'unstable',
  },
}

// ─── Stats ────────────────────────────────────────────────────────────────────
const stats = {
  total:       0,
  success:     0,
  errors:      0,
  timeouts:    0,
  rateLimited: 0,
  totalLatencyMs: 0,
}

// ─── Rate limiting ────────────────────────────────────────────────────────────
let requestsThisMinute = 0
setInterval(() => {
  if (requestsThisMinute > 0) {
    console.log(JSON.stringify({
      ts:       new Date().toISOString(),
      event:    'rate_reset',
      requests: requestsThisMinute,
      profile:  config.profile,
    }))
  }
  requestsThisMinute = 0
}, 60_000)

// ─── Webhook ──────────────────────────────────────────────────────────────────
app.post('/webhook', async (req, res) => {
  stats.total++
  requestsThisMinute++

  // Rate limit
  if (requestsThisMinute > config.RATE_LIMIT) {
    stats.rateLimited++
    console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'rate_limited', order_id: req.body.order_id, profile: config.profile }))
    return res.status(429).json({ success: false, error: { code: 'RATE_LIMITED', message: 'Too many requests' } })
  }

  // Timeout
  if (Math.random() < config.P_TIMEOUT) {
    stats.timeouts++
    console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'timeout_simulated', order_id: req.body.order_id, profile: config.profile }))
    setTimeout(() => res.destroy(), 15_000)
    return
  }

  // Latência
  const slow = Math.random() < config.P_SLOW
  const latencyMs = slow
    ? 2000 + Math.random() * 3000
    : 50   + Math.random() * 300

  await sleep(latencyMs)
  stats.totalLatencyMs += latencyMs

  // Erro 500
  if (Math.random() < config.P_ERROR) {
    stats.errors++
    console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'error_simulated', order_id: req.body.order_id, latency_ms: Math.round(latencyMs), profile: config.profile }))
    return res.status(500).json({ success: false, error: { code: 'SIMULATED_ERROR', message: 'Erro simulado pela API' } })
  }

  // Sucesso
  stats.success++
  console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'lead_received', order_id: req.body.order_id, email: req.body.email, product: req.body.product_name, latency_ms: Math.round(latencyMs), slow, profile: config.profile }))

  res.status(200).json({ success: true, order_id: req.body.order_id, received_at: new Date().toISOString() })
})

// ─── Config endpoints ─────────────────────────────────────────────────────────

// GET /config — mostra config atual
app.get('/config', (req, res) => res.json(config))

// POST /config — altera valores manualmente
// Ex: curl -X POST localhost:8080/config -d '{"P_ERROR":0.1,"RATE_LIMIT":200}'
app.post('/config', (req, res) => {
  const allowed = ['RATE_LIMIT', 'P_ERROR', 'P_TIMEOUT', 'P_SLOW']
  for (const key of allowed) {
    if (req.body[key] !== undefined) config[key] = parseFloat(req.body[key])
  }
  config.profile = 'custom'
  console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'config_changed', config }))
  res.json({ ok: true, config })
})

// POST /config/reallyfast — API bruta
app.post('/config/reallyfast', (req, res) => {
  config = { ...PROFILES.reallyfast }
  console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'profile_changed', profile: 'reallyfast', config }))
  res.json({ ok: true, config })
})

// POST /config/fast — API saudável e rápida
app.post('/config/fast', (req, res) => {
  config = { ...PROFILES.fast }
  console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'profile_changed', profile: 'fast', config }))
  res.json({ ok: true, config })
})

// POST /config/slow — API lenta, simula sistema legado sobrecarregado
app.post('/config/slow', (req, res) => {
  config = { ...PROFILES.slow }
  console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'profile_changed', profile: 'slow', config }))
  res.json({ ok: true, config })
})

// POST /config/unstable — API instável, alta taxa de erro e timeout
app.post('/config/unstable', (req, res) => {
  config = { ...PROFILES.unstable }
  console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'profile_changed', profile: 'unstable', config }))
  res.json({ ok: true, config })
})

// ─── Stats & Health ───────────────────────────────────────────────────────────
app.get('/stats', (req, res) => {
  const avgLatency = stats.success > 0 ? Math.round(stats.totalLatencyMs / stats.success) : 0
  res.json({
    ...stats,
    avg_latency_ms:   avgLatency,
    error_rate_pct:   stats.total > 0 ? ((stats.errors   / stats.total) * 100).toFixed(1) : '0.0',
    timeout_rate_pct: stats.total > 0 ? ((stats.timeouts / stats.total) * 100).toFixed(1) : '0.0',
    config,
  })
})

app.get('/health', (req, res) => res.json({ status: 'ok', profile: config.profile }))

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 8080
app.listen(PORT, () => {
  console.log(JSON.stringify({ ts: new Date().toISOString(), event: 'started', port: PORT, config }))
})

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }
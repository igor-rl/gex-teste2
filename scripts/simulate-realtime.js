'use strict'

/**
 * GEX — Real-time Event Simulator
 * Simula chegada de eventos em tempo real via HTTP.
 * 
 * Uso:
 *   node simulate-realtime.js            → 100 eventos, 10/s
 *   node simulate-realtime.js 500 5      → 500 eventos, 5/s
 *   node simulate-realtime.js 1000 50    → 1000 eventos, 50/s
 */

const http = require('http')

const TOTAL_EVENTS = parseInt(process.argv[2], 10) || 100
const RATE_PER_SEC = parseInt(process.argv[3], 10) || 10
const HOST         = process.env.INGESTION_HOST || 'localhost'
const PORT         = process.env.INGESTION_PORT || 3001

const PRODUCTS = [
  { id: 'PROD-001', name: 'Fit Burn',      niche: 'weight_loss' },
  { id: 'PROD-002', name: 'Memory Lift',   niche: 'memory' },
  { id: 'PROD-003', name: 'Gluco Control', niche: 'blood_sugar' },
  { id: 'PROD-004', name: 'Mens Growth',   niche: 'male_enhancement' },
  { id: 'PROD-005', name: 'Joint Flex',    niche: 'joint_health' },
]

const COUNTRIES = ['US', 'US', 'US', 'GB', 'CA', 'AU']
const SOURCES   = ['facebook_ads', 'google_ads', 'tiktok_ads', 'email_campaign', 'organic']
const GATEWAYS  = ['cartpanda', 'digistore']
const QTYS      = [1, 3, 6]
const PRICES    = { 1: 49, 3: 97, 6: 149 }

// Introduz erros intencionais como no CSV original (10% dos eventos)
function maybeCorrupt(event, idx) {
  const r = idx % 10
  if (r === 0) event.customer_email = event.customer_email.replace('@', '') // email inválido
  if (r === 1) event.customer_phone = '+1555'  // phone curto
  if (r === 2) { event.payment_status = 'declined'; event.event = 'order.declined' }
  if (r === 3) event.customer_first_name = ''  // nome vazio
  return event
}

function generateEvent(idx) {
  const product  = PRODUCTS[idx % PRODUCTS.length]
  const country  = COUNTRIES[idx % COUNTRIES.length]
  const qty      = QTYS[idx % QTYS.length]
  const source   = SOURCES[idx % SOURCES.length]
  const gateway  = GATEWAYS[idx % GATEWAYS.length]
  const ts       = new Date().toISOString()
  const suffix   = String(idx + 1).padStart(8, '0')

  const event = {
    order_id:            `ORD-SIM-${suffix}`,
    created_at:          ts,
    event:               'order.approved',
    customer_email:      `user${suffix}@example.com`,
    customer_first_name: `User`,
    customer_last_name:  `${suffix}`,
    customer_phone:      `+1${String(2000000000 + idx).padStart(10, '0')}`,
    customer_country:    country,
    customer_state:      country === 'US' ? 'TX' : '',
    product_id:          product.id,
    product_name:        product.name,
    product_niche:       product.niche,
    quantity:            qty,
    price_usd:           PRICES[qty],
    payment_method:      'credit_card',
    payment_gateway:     gateway,
    payment_status:      'approved',
    funnel_source:       source,
    utm_campaign:        `sim_${product.niche}_test`,
  }

  return maybeCorrupt(event, idx)
}

function sendEvent(event) {
  return new Promise((resolve, reject) => {
    const body = JSON.stringify(event)
    const req  = http.request({
      hostname: HOST,
      port:     PORT,
      path:     '/events',
      method:   'POST',
      headers:  { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(body) },
    }, (res) => {
      res.resume()
      res.on('end', () => resolve(res.statusCode))
    })
    req.on('error', reject)
    req.write(body)
    req.end()
  })
}

async function simulate() {
  const intervalMs = 1000 / RATE_PER_SEC
  let sent = 0, errors = 0, discarded = 0
  const startTime = Date.now()

  console.log(`🚀 GEX Simulator`)
  console.log(`   Target:  ${TOTAL_EVENTS} eventos`)
  console.log(`   Rate:    ${RATE_PER_SEC}/s`)
  console.log(`   Host:    http://${HOST}:${PORT}`)
  console.log(`   Corrupt: ~10% (simulando dados sujos)`)
  console.log(``)

  for (let i = 0; i < TOTAL_EVENTS; i++) {
    const event = generateEvent(i)
    try {
      const status = await sendEvent(event)
      if (status === 202) {
        sent++
      } else {
        errors++
        console.error(`  ❌ ${event.order_id} → HTTP ${status}`)
      }
    } catch (err) {
      errors++
      console.error(`  ❌ ${event.order_id} → ${err.message}`)
    }

    if ((i + 1) % 100 === 0) {
      const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
      const rate    = ((i + 1) / elapsed).toFixed(1)
      process.stdout.write(`\r  📤 ${i + 1}/${TOTAL_EVENTS} enviados | ${rate}/s | ${errors} erros      `)
    }

    if (i < TOTAL_EVENTS - 1) await sleep(intervalMs)
  }

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(1)
  console.log(`\n`)
  console.log(`═══════════════════════════════`)
  console.log(`✅ Simulação concluída em ${elapsed}s`)
  console.log(`   Enviados: ${sent}`)
  console.log(`   Erros:    ${errors}`)
  console.log(`═══════════════════════════════`)
}

function sleep(ms) { return new Promise(r => setTimeout(r, ms)) }

simulate().catch(console.error)

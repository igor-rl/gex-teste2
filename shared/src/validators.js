'use strict'

const { v4: uuidv4 } = require('uuid')

// ─── Email ────────────────────────────────────────────────────────

function validateEmail(email) {
  if (!email || typeof email !== 'string') return { valid: false, clean: null }
  const clean = email.trim().toLowerCase()
  if (!clean) return { valid: false, clean: null }
  // RFC-compliant basic check: localpart@domain.tld
  const valid = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/.test(clean)
  return { valid, clean }
}

// ─── Phone ────────────────────────────────────────────────────────

const COUNTRY_PREFIXES = {
  US: '+1', CA: '+1', GB: '+44', AU: '+61',
  NZ: '+64', IE: '+353', DE: '+49', FR: '+33',
}

function validatePhone(phone, country = 'US') {
  if (!phone || typeof phone !== 'string' || !phone.trim()) {
    return { valid: false, clean: null }
  }

  // Remove tudo exceto dígitos e +
  let clean = phone.trim().replace(/[\s\-().]/g, '')

  // Se já tem + na frente, usa como está
  if (!clean.startsWith('+')) {
    const prefix = COUNTRY_PREFIXES[country] || '+1'
    clean = `${prefix}${clean}`
  }

  // Remove caracteres não-numéricos exceto + inicial
  clean = '+' + clean.slice(1).replace(/\D/g, '')

  // E.164: + seguido de 7-15 dígitos
  const valid = /^\+\d{7,15}$/.test(clean)

  return { valid, clean: valid ? clean : clean }
}

// ─── Lead enrichment ─────────────────────────────────────────────

function enrichLead(event) {
  const emailResult = validateEmail(event.customer_email)
  const phoneResult = validatePhone(event.customer_phone, event.customer_country)

  const firstName = (event.customer_first_name || '').trim() || 'Customer'
  const lastName  = (event.customer_last_name  || '').trim()

  const tags = buildTags(event)

  return {
    order_id:       event.order_id,
    correlation_id: event.correlation_id || uuidv4(),
    email:          emailResult.clean,
    email_valid:    emailResult.valid,
    full_name:      `${firstName} ${lastName}`.trim(),
    phone:          phoneResult.clean,
    phone_valid:    phoneResult.valid,
    country:        event.customer_country,
    product_name:   event.product_name,
    product_niche:  event.product_niche,
    order_value:    parseFloat(event.price_usd) || 0,
    bottles_qty:    parseInt(event.quantity, 10) || 1,
    purchase_date:  event.created_at,
    funnel_source:  event.funnel_source,
    utm_campaign:   event.utm_campaign,
    tags,
    processed_at:   new Date().toISOString(),
  }
}

function buildTags(event) {
  const tags = []
  if (event.product_name) {
    tags.push(`buyer_${event.product_name.toLowerCase().replace(/\s+/g, '_')}`)
  }
  if (event.product_niche) tags.push(event.product_niche)
  const qty = parseInt(event.quantity, 10)
  if (qty)                  tags.push(`${qty}_bottles`)
  if (event.funnel_source)  tags.push(event.funnel_source)
  return tags
}

// ─── Filtro de aprovação ──────────────────────────────────────────

function isApprovedOrder(event) {
  return (
    event.payment_status === 'approved' &&
    event.event          === 'order.approved'
  )
}

module.exports = { validateEmail, validatePhone, enrichLead, isApprovedOrder }

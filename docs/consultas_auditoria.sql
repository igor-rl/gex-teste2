-- ═══════════════════════════════════════════════════════════════════
-- GEX — Consultas de Auditoria e Monitoramento
-- ═══════════════════════════════════════════════════════════════════

-- ─────────────────────────────────────────────────────────────────
-- 1. RESUMO GERAL: quantos enviados, erro, descartados?
-- ─────────────────────────────────────────────────────────────────
SELECT
  status,
  COUNT(*)                                       AS total,
  COUNT(*) FILTER (WHERE email_valid = FALSE)    AS invalid_email,
  COUNT(*) FILTER (WHERE phone_valid = FALSE)    AS invalid_phone,
  ROUND(SUM(order_value), 2)                     AS total_revenue_usd
FROM lead_control
GROUP BY status
ORDER BY status;

-- ─────────────────────────────────────────────────────────────────
-- 2. TEMPO DE LATÊNCIA: tempo médio entre venda e chegada na plataforma
--    Limite aceitável: < 5 minutos (SLA interno GEX)
--    Crítico: > 30 minutos (indica pipeline parado ou lento)
-- ─────────────────────────────────────────────────────────────────
SELECT
  ROUND(AVG(EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60), 2)  AS avg_latency_min,
  ROUND(MIN(EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60), 2)  AS min_latency_min,
  ROUND(MAX(EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60), 2)  AS max_latency_min,
  PERCENTILE_CONT(0.50) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60
  )                                                                         AS p50_latency_min,
  PERCENTILE_CONT(0.95) WITHIN GROUP (
    ORDER BY EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60
  )                                                                         AS p95_latency_min,
  COUNT(*)                                                                  AS sample_size
FROM lead_control
WHERE status = 'enviado'
  AND purchase_date IS NOT NULL
  AND processed_at  > NOW() - INTERVAL '24 hours';

-- ─────────────────────────────────────────────────────────────────
-- 3. TAXA DE ERRO POR HORA
--    Alerta crítico: taxa > 5% em qualquer hora
--    Alerta atenção: taxa > 2%
-- ─────────────────────────────────────────────────────────────────
SELECT
  DATE_TRUNC('hour', processed_at)                                         AS hour_bucket,
  COUNT(*)                                                                  AS total,
  COUNT(*) FILTER (WHERE status = 'enviado')                               AS sent,
  COUNT(*) FILTER (WHERE status = 'erro')                                  AS errors,
  COUNT(*) FILTER (WHERE status = 'descartado')                            AS discarded,
  ROUND(
    COUNT(*) FILTER (WHERE status = 'erro') * 100.0 / NULLIF(COUNT(*), 0), 2
  )                                                                         AS error_rate_pct,
  CASE
    WHEN COUNT(*) FILTER (WHERE status = 'erro') * 100.0 / NULLIF(COUNT(*),0) > 5
      THEN '🔴 CRÍTICO'
    WHEN COUNT(*) FILTER (WHERE status = 'erro') * 100.0 / NULLIF(COUNT(*),0) > 2
      THEN '🟡 ATENÇÃO'
    ELSE '🟢 OK'
  END                                                                       AS alert_level
FROM lead_control
WHERE processed_at > NOW() - INTERVAL '24 hours'
GROUP BY 1
ORDER BY 1 DESC;

-- ─────────────────────────────────────────────────────────────────
-- 4. ALERTA DE PARADA: detecta se o fluxo parou há 30+ minutos
--    Retorna pipeline_stalled = TRUE se não houve processamento
--    em horário ativo (08h-23h)
-- ─────────────────────────────────────────────────────────────────
SELECT
  MAX(processed_at)                                                        AS last_processed_at,
  ROUND(EXTRACT(EPOCH FROM (NOW() - MAX(processed_at))) / 60, 1)          AS minutes_since_last,
  COUNT(*) FILTER (WHERE processed_at > NOW() - INTERVAL '1 hour')        AS processed_last_hour,
  CASE
    WHEN MAX(processed_at) < NOW() - INTERVAL '30 minutes'
     AND EXTRACT(HOUR FROM NOW()) BETWEEN 8 AND 23
      THEN TRUE
    ELSE FALSE
  END                                                                       AS pipeline_stalled,
  CASE
    WHEN MAX(processed_at) < NOW() - INTERVAL '30 minutes'
     AND EXTRACT(HOUR FROM NOW()) BETWEEN 8 AND 23
      THEN '🔴 PIPELINE PARADO — Verificar serviços!'
    ELSE '🟢 Pipeline ativo'
  END                                                                       AS alert_message
FROM lead_control;

-- ─────────────────────────────────────────────────────────────────
-- 5. RECONCILIAÇÃO: comparar total de vendas aprovadas vs leads enviados
--    Diferença > 0 indica leads pendentes ou perdidos
-- ─────────────────────────────────────────────────────────────────
SELECT
  COUNT(*)                                            AS total_records,
  COUNT(*) FILTER (WHERE status = 'enviado')         AS successfully_sent,
  COUNT(*) FILTER (WHERE status = 'erro')            AS failed,
  COUNT(*) FILTER (WHERE status = 'descartado')      AS discarded,
  COUNT(*) FILTER (WHERE status = 'retrying')        AS retrying,
  -- Gap: registros que não chegaram ao destino
  COUNT(*) FILTER (WHERE status IN ('erro','retrying')) AS pending_reprocess,
  ROUND(
    COUNT(*) FILTER (WHERE status = 'enviado') * 100.0 / NULLIF(COUNT(*),0), 2
  )                                                   AS delivery_success_rate_pct
FROM lead_control;

-- ─────────────────────────────────────────────────────────────────
-- 6. LEADS NÃO ENTREGUES (para reprocessamento manual se necessário)
-- ─────────────────────────────────────────────────────────────────
SELECT
  order_id,
  email,
  product_name,
  country,
  purchase_date,
  status,
  error_message,
  attempt_count,
  processed_at
FROM lead_control
WHERE status IN ('erro', 'retrying')
ORDER BY processed_at DESC
LIMIT 100;

-- ─────────────────────────────────────────────────────────────────
-- 7. ANÁLISE POR PAÍS E PRODUTO (útil para reconciliação por segmento)
-- ─────────────────────────────────────────────────────────────────
SELECT
  country,
  product_name,
  COUNT(*)                                            AS total,
  COUNT(*) FILTER (WHERE status = 'enviado')         AS sent,
  COUNT(*) FILTER (WHERE status = 'descartado')      AS discarded,
  COUNT(*) FILTER (WHERE status = 'erro')            AS errors,
  ROUND(SUM(order_value) FILTER (WHERE status = 'enviado'), 2) AS revenue_sent_usd
FROM lead_control
GROUP BY country, product_name
ORDER BY total DESC;

-- ─────────────────────────────────────────────────────────────────
-- 8. AUDIT TRAIL: rastrear um pedido específico ponta a ponta
-- ─────────────────────────────────────────────────────────────────
-- Substitua 'ORD-2026-10001' pelo order_id que deseja investigar
SELECT
  t.event_type,
  t.service,
  t.payload,
  t.error_detail,
  t.duration_ms,
  t.ts,
  -- Tempo entre eventos consecutivos
  t.ts - LAG(t.ts) OVER (PARTITION BY t.order_id ORDER BY t.ts) AS time_since_prev
FROM lead_audit_trail t
WHERE t.order_id = 'ORD-2026-10001'
ORDER BY t.ts ASC;

-- ─────────────────────────────────────────────────────────────────
-- 9. LEADS COM E-MAIL INVÁLIDO QUE FORAM DESCARTADOS
--    (para recuperação manual ou campanha alternativa)
-- ─────────────────────────────────────────────────────────────────
SELECT
  order_id,
  email,
  full_name,
  product_name,
  country,
  purchase_date,
  discard_reason
FROM lead_control
WHERE status = 'descartado'
  AND discard_reason LIKE '%invalid_email%'
ORDER BY purchase_date DESC;

-- ─────────────────────────────────────────────────────────────────
-- 10. LEADS DO FIM DE SEMANA NÃO ENTREGUES
--     (exatamente para o cenário do problema real descrito)
-- ─────────────────────────────────────────────────────────────────
SELECT
  COUNT(*)                                            AS total_weekend,
  COUNT(*) FILTER (WHERE status = 'enviado')         AS sent,
  COUNT(*) FILTER (WHERE status IN ('erro','retrying')) AS not_delivered,
  COUNT(*) FILTER (WHERE status = 'descartado')      AS discarded,
  MIN(purchase_date)                                  AS weekend_start,
  MAX(purchase_date)                                  AS weekend_end
FROM lead_control
WHERE EXTRACT(DOW FROM purchase_date) IN (0, 6)  -- 0=domingo, 6=sábado
  AND purchase_date >= DATE_TRUNC('week', NOW()) - INTERVAL '2 days';

-- ─────────────────────────────────────────────────────────────────
-- 11. ÍNDICES RECOMENDADOS (já criados no init.sql)
-- ─────────────────────────────────────────────────────────────────
-- CREATE INDEX idx_lead_status        ON lead_control(status);
-- CREATE INDEX idx_lead_processed_at  ON lead_control(processed_at);
-- CREATE INDEX idx_lead_email         ON lead_control(email);
-- CREATE INDEX idx_lead_country       ON lead_control(country);
-- CREATE INDEX idx_lead_product       ON lead_control(product_name);
-- CREATE INDEX idx_audit_order_id     ON lead_audit_trail(order_id);
-- CREATE INDEX idx_audit_ts           ON lead_audit_trail(ts);

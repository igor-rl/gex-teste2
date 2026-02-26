-- ═══════════════════════════════════════════════════════════════
-- GEX — Schema PostgreSQL
-- v2: suporte a deduplicação por record_hash
-- ═══════════════════════════════════════════════════════════════

-- ─── Tabela principal de controle de leads ───────────────────────
CREATE TABLE IF NOT EXISTS lead_control (
  id              SERIAL PRIMARY KEY,
  order_id        VARCHAR(100) NOT NULL,         -- sem UNIQUE: múltiplas versões permitidas
  record_hash     VARCHAR(64) NOT NULL,           -- SHA-256 dos campos relevantes
  correlation_id  UUID,
  email           VARCHAR(255),
  phone           VARCHAR(50),
  full_name       VARCHAR(255),
  product_name    VARCHAR(255),
  product_niche   VARCHAR(100),
  country         VARCHAR(10),
  order_value     NUMERIC(10,2),
  bottles_qty     INT,
  funnel_source   VARCHAR(100),
  purchase_date   TIMESTAMPTZ,
  -- Status: enviado | erro | descartado | retrying
  status          VARCHAR(20) NOT NULL CHECK (status IN ('enviado','erro','descartado','retrying')),
  discard_reason  TEXT,
  error_message   TEXT,
  email_valid     BOOLEAN NOT NULL DEFAULT FALSE,
  phone_valid     BOOLEAN NOT NULL DEFAULT FALSE,
  attempt_count   INT NOT NULL DEFAULT 1,
  processed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── Tabela de trilha de auditoria (imutável — append-only) ──────
CREATE TABLE IF NOT EXISTS lead_audit_trail (
  id              SERIAL PRIMARY KEY,
  order_id        VARCHAR(100) NOT NULL,
  correlation_id  UUID,
  event_type      VARCHAR(50) NOT NULL,   -- received | validated | discarded | sent | failed | retried
  service         VARCHAR(50),
  payload         JSONB,
  error_detail    TEXT,
  duration_ms     INT,
  ts              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── Índices de performance ──────────────────────────────────────

-- Deduplicação: bloqueia inserção de mesma versão do mesmo pedido
CREATE UNIQUE INDEX IF NOT EXISTS idx_lead_order_hash
  ON lead_control(order_id, record_hash);

-- Busca da versão mais recente por order_id
CREATE INDEX IF NOT EXISTS idx_lead_order_created
  ON lead_control(order_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_lead_status        ON lead_control(status);
CREATE INDEX IF NOT EXISTS idx_lead_processed_at  ON lead_control(processed_at);
CREATE INDEX IF NOT EXISTS idx_lead_email         ON lead_control(email);
CREATE INDEX IF NOT EXISTS idx_lead_country       ON lead_control(country);
CREATE INDEX IF NOT EXISTS idx_lead_product       ON lead_control(product_name);
CREATE INDEX IF NOT EXISTS idx_audit_order_id     ON lead_audit_trail(order_id);
CREATE INDEX IF NOT EXISTS idx_audit_ts           ON lead_audit_trail(ts);
CREATE INDEX IF NOT EXISTS idx_audit_event        ON lead_audit_trail(event_type);

-- ─── Trigger para updated_at automático ──────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_lead_updated_at
  BEFORE UPDATE ON lead_control
  FOR EACH ROW EXECUTE FUNCTION update_updated_at();

-- ─── Função: upsert com deduplicação ─────────────────────────────
-- Retorna: 'inserted' | 'duplicate'
CREATE OR REPLACE FUNCTION upsert_lead(
  p_order_id       VARCHAR,
  p_record_hash    VARCHAR,
  p_correlation_id UUID,
  p_email          VARCHAR,
  p_phone          VARCHAR,
  p_full_name      VARCHAR,
  p_product_name   VARCHAR,
  p_product_niche  VARCHAR,
  p_country        VARCHAR,
  p_order_value    NUMERIC,
  p_bottles_qty    INT,
  p_funnel_source  VARCHAR,
  p_purchase_date  TIMESTAMPTZ,
  p_status         VARCHAR
) RETURNS TEXT AS $$
BEGIN
  -- Tenta inserir; se (order_id, record_hash) já existe, ignora
  INSERT INTO lead_control (
    order_id, record_hash, correlation_id,
    email, phone, full_name, product_name, product_niche,
    country, order_value, bottles_qty, funnel_source,
    purchase_date, status
  ) VALUES (
    p_order_id, p_record_hash, p_correlation_id,
    p_email, p_phone, p_full_name, p_product_name, p_product_niche,
    p_country, p_order_value, p_bottles_qty, p_funnel_source,
    p_purchase_date, p_status
  )
  ON CONFLICT (order_id, record_hash) DO NOTHING;

  IF FOUND THEN
    RETURN 'inserted';
  ELSE
    RETURN 'duplicate';
  END IF;
END;
$$ LANGUAGE plpgsql;

-- ─── View: resumo por status ─────────────────────────────────────
CREATE OR REPLACE VIEW lead_summary AS
SELECT
  status,
  COUNT(*)                                       AS total,
  COUNT(*) FILTER (WHERE email_valid = FALSE)    AS invalid_email_count,
  COUNT(*) FILTER (WHERE phone_valid = FALSE)    AS invalid_phone_count,
  ROUND(AVG(order_value),2)                      AS avg_order_value,
  MIN(processed_at)                              AS first_processed,
  MAX(processed_at)                              AS last_processed
FROM lead_control
GROUP BY status;

-- ─── View: versão mais recente de cada order_id ──────────────────
CREATE OR REPLACE VIEW lead_latest AS
SELECT DISTINCT ON (order_id)
  *
FROM lead_control
ORDER BY order_id, created_at DESC;

-- ─── View: histórico de versões por pedido ────────────────────────
CREATE OR REPLACE VIEW lead_versions AS
SELECT
  order_id,
  COUNT(*)          AS version_count,
  MIN(created_at)   AS first_seen,
  MAX(created_at)   AS last_updated,
  ARRAY_AGG(record_hash ORDER BY created_at) AS hash_history
FROM lead_control
GROUP BY order_id
HAVING COUNT(*) > 1;

-- ─── View: métricas de latência ──────────────────────────────────
CREATE OR REPLACE VIEW latency_metrics AS
SELECT
  DATE_TRUNC('hour', purchase_date)                        AS hour_bucket,
  COUNT(*)                                                 AS total_leads,
  ROUND(AVG(
    EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60
  ), 2)                                                    AS avg_latency_minutes,
  MAX(
    EXTRACT(EPOCH FROM (processed_at - purchase_date)) / 60
  )                                                        AS max_latency_minutes
FROM lead_control
WHERE status = 'enviado' AND purchase_date IS NOT NULL
GROUP BY 1
ORDER BY 1 DESC;

-- ─── View: taxa de erro por hora ─────────────────────────────────
CREATE OR REPLACE VIEW error_rate_by_hour AS
SELECT
  DATE_TRUNC('hour', processed_at)                         AS hour_bucket,
  COUNT(*)                                                 AS total,
  COUNT(*) FILTER (WHERE status = 'erro')                  AS errors,
  COUNT(*) FILTER (WHERE status = 'enviado')               AS sent,
  COUNT(*) FILTER (WHERE status = 'descartado')            AS discarded,
  ROUND(
    COUNT(*) FILTER (WHERE status = 'erro') * 100.0 / NULLIF(COUNT(*), 0), 2
  )                                                        AS error_rate_pct
FROM lead_control
GROUP BY 1
ORDER BY 1 DESC;
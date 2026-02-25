# GEX — Runbook: Investigação de Incidentes no Pipeline

## Cenário: "Leads aprovados não estão chegando na plataforma"

---

## 1. PRIMEIRA AÇÃO (primeiros 2 minutos)

**Não entre em pânico. Siga o checklist.**

```bash
# 1a. Verificar se os serviços estão vivos
docker compose ps

# 1b. Verificar health dos serviços críticos
curl http://localhost:3001/health     # ingestion
curl http://localhost:3005/health     # observability

# 1c. Verificar se o pipeline processou algo recentemente
curl http://localhost:3005/metrics/pipeline-health
# Se pipeline_stalled = true → serviço parado
# Se minutes_since_last > 30 em horário ativo → ALERTA
```

---

## 2. DIAGNÓSTICO: onde está o problema?

### Passo 2.1 — Verificar filas SQS

```bash
curl http://localhost:3005/metrics/queues
```

**Interpretar:**
| Situação | Causa provável |
|---|---|
| `raw` cheio, `valid` vazio | Validation service parado |
| `valid` cheio, `results` vazio | Delivery service parado |
| Todas as filas vazias, DB sem registros | Ingestion não está recebendo |
| `dlq` com mensagens | Erros fatais repetidos — ver DLQ |
| `results` cheio, DB sem atualizar | Persistence service parado |

```bash
# Ver mensagens na DLQ (eventos que falharam todas as tentativas)
curl http://localhost:3005/metrics/dlq
```

### Passo 2.2 — Verificar logs dos serviços

```bash
# Logs de todos os serviços (últimas 100 linhas)
docker compose logs --tail=100 validation
docker compose logs --tail=100 delivery
docker compose logs --tail=100 persistence

# Buscar erros específicos
docker compose logs --tail=200 delivery | grep '"level":50'   # errors
docker compose logs --tail=200 delivery | grep 'token\|auth\|401\|403'
```

### Passo 2.3 — Verificar banco de dados

```sql
-- Qual o último registro processado?
SELECT MAX(processed_at), COUNT(*) FROM lead_control;

-- Leads do período problemático (ex: fim de semana)
SELECT status, COUNT(*) 
FROM lead_control 
WHERE purchase_date BETWEEN '2026-02-08' AND '2026-02-09'
GROUP BY status;

-- Erros recentes com mensagem
SELECT order_id, error_message, processed_at 
FROM lead_control 
WHERE status = 'erro' 
ORDER BY processed_at DESC 
LIMIT 20;
```

---

## 3. IDENTIFICAR SE O PROBLEMA É NA ENTRADA, PROCESSAMENTO OU SAÍDA

```
[ENTRADA]          [PROCESSAMENTO]        [SAÍDA]
HTTP /events   →   Validation Worker  →   Delivery Worker  →  Webhook
S3 Upload      →   SQS raw            →   SQS valid        →  SQS results  →  DB
```

### Testar ENTRADA (ingestion):
```bash
curl -X POST http://localhost:3001/events \
  -H "Content-Type: application/json" \
  -d '{"order_id":"TEST-001","event":"order.approved","payment_status":"approved","customer_email":"test@test.com"}'
# Esperado: {"correlation_id":"...","status":"queued"}
```

### Testar PROCESSAMENTO (validation):
```bash
# Verificar se a fila raw está acumulando
curl http://localhost:3005/metrics/queues | grep -A4 '"name":"raw"'
# Se messages_available cresce e valid não cresce → validation parou
```

### Testar SAÍDA (delivery):
```bash
# Simular 1 evento e acompanhar
curl -X POST http://localhost:3001/events \
  -d '{"order_id":"DIAG-001","event":"order.approved","payment_status":"approved","customer_email":"diag@test.com","product_name":"Fit Burn","price_usd":97}'

sleep 5

# Verificar se chegou no banco
psql postgresql://pguser:pgpass@localhost:5432/gex \
  -c "SELECT * FROM lead_control WHERE order_id='DIAG-001'"
```

---

## 4. SE FOR ERRO DE API (token expirado, 401/403)

### Identificar:
```bash
docker compose logs delivery | grep '401\|403\|Unauthorized\|token'
```

### Corrigir o token:
```bash
# Atualizar variável de ambiente
docker compose stop delivery
# Editar .env ou docker-compose.yaml com novo WEBHOOK_URL/token
docker compose up -d delivery
```

### Reprocessar os leads com erro:
```bash
# 1. Buscar order_ids com erro no período
psql postgresql://pguser:pgpass@localhost:5432/gex -c "
  SELECT order_id FROM lead_control 
  WHERE status = 'erro' 
    AND processed_at >= '2026-02-08'
  ORDER BY processed_at
" -t > /tmp/failed_orders.txt

# 2. Para cada order_id, buscar dados e reenviar
# (use o script de reprocess abaixo)
node scripts/reprocess-failed.js /tmp/failed_orders.txt
```

### Script de reprocessamento:
```js
// scripts/reprocess-failed.js
// Busca leads com status='erro' e reenvia para a fila SQS
const { Pool } = require('pg')
const { sendMessage } = require('./shared/src/aws-client')

async function reprocess() {
  const db = new Pool({ connectionString: process.env.DATABASE_URL })
  const { rows } = await db.query(
    "SELECT * FROM lead_control WHERE status='erro' ORDER BY processed_at"
  )
  console.log(`Reprocessando ${rows.length} leads...`)
  for (const lead of rows) {
    await sendMessage(process.env.SQS_VALID_QUEUE_URL, lead, {
      messageGroupId: lead.order_id,
      messageDeduplicationId: `reprocess-${lead.order_id}-${Date.now()}`
    })
    // Marcar como retrying
    await db.query(
      "UPDATE lead_control SET status='retrying' WHERE order_id=$1",
      [lead.order_id]
    )
  }
  console.log('Reprocessamento enfileirado.')
  await db.end()
}
reprocess().catch(console.error)
```

---

## 5. MEDIDAS PREVENTIVAS (para não acontecer de novo)

| Medida | Implementação |
|---|---|
| **Alerta de parada automático** | Query #4 no cron a cada 5min → SNS alert |
| **Health check de filas** | `GET /metrics/queues` monitorado por CloudWatch |
| **DLQ monitoring** | Alerta se DLQ > 0 mensagens |
| **Retry com backoff** | Já implementado no delivery service (3 tentativas, backoff exponencial) |
| **Idempotência** | ON CONFLICT no PostgreSQL garante sem duplicatas no reprocessamento |
| **Correlation ID** | Cada evento tem UUID rastreável ponta a ponta |
| **Audit trail imutável** | Tabela `lead_audit_trail` para forensics |
| **Monitoramento de latência** | `GET /metrics/latency` com p95 < 5 min como SLA |

---

## 6. ENDPOINTS DE OBSERVABILIDADE — REFERÊNCIA RÁPIDA

| Endpoint | Uso |
|---|---|
| `GET :3005/health` | Health geral + DB |
| `GET :3005/metrics/queues` | Tamanho das filas SQS |
| `GET :3005/metrics/summary` | Totais por status |
| `GET :3005/metrics/latency` | Latência venda→entrega |
| `GET :3005/metrics/error-rate` | Taxa de erro por hora |
| `GET :3005/metrics/pipeline-health` | Detecta pipeline parado |
| `GET :3005/metrics/reconciliation` | DB vs filas |
| `GET :3005/metrics/recent-errors` | Últimos 50 erros |
| `GET :3005/metrics/dlq` | Dead Letter Queue |
| `GET :3005/audit/:orderId` | Trilha completa de 1 pedido |

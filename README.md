# GEX — Pipeline de Integração de Vendas (AWS + Microserviços)

Pipeline de processamento de eventos de vendas com **5 microserviços Node.js** desacoplados via **AWS SQS** (simulado com LocalStack), S3 para batch histórico, PostgreSQL para persistência e trilha de auditoria completa.

---

## Arquitetura

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ENTRADA DE DADOS                            │
│                                                                     │
│  POST /events (real-time) ──────┐                                   │
│  POST /batch  (CSV bulk)  ──────┼──► [Ingestion :3001]             │
│  POST /batch/s3 (upload S3) ───┘         │          │              │
│                                          ▼          ▼              │
│                                    SQS FIFO    S3 Bucket           │
│                                 gex-events-  gex-sales-            │
│                                    raw.fifo     batch              │
└──────────────────────────────────────────────────────────────────--─┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       PROCESSAMENTO                                 │
│                                                                     │
│              [Validation Worker]                                    │
│              ┌────────────────┐                                     │
│              │ • Filtra approved                                    │
│              │ • Valida email/phone                                 │
│              │ • Enriquece lead                                     │
│              │ • Audit trail                                        │
│              └────────┬───────┘                                     │
│                       │                                             │
│          ┌────────────┴────────────┐                               │
│          ▼                         ▼                               │
│   SQS leads.valid.fifo    SQS leads.discarded                      │
└──────────────────────────────────────────────────────────────────--─┘
                │                    │
                ▼                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       ENTREGA + PERSISTÊNCIA                        │
│                                                                     │
│    [Delivery Worker]           [Persistence Worker]                 │
│    ┌─────────────────┐        ┌─────────────────────┐             │
│    │ • POST webhook  │        │ • Upsert lead_control│             │
│    │ • Retry 3x      │   ──►  │ • Audit trail        │             │
│    │ • Backoff exp.  │        │ • Idempotente        │             │
│    │ • Timeout 10s   │        └─────────────────────┘             │
│    └─────────────────┘                 │                           │
│           │                            ▼                           │
│    SQS delivery.results          PostgreSQL                        │
│                                  lead_control                      │
│                              lead_audit_trail                      │
└──────────────────────────────────────────────────────────────────--─┘
                │
                ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     OBSERVABILIDADE :3005                           │
│                                                                     │
│  /health              /metrics/queues      /metrics/summary        │
│  /metrics/latency     /metrics/error-rate  /metrics/pipeline-health│
│  /metrics/dlq         /metrics/reconciliation  /audit/:orderId     │
└──────────────────────────────────────────────────────────────────--─┘
```

---

## Pré-requisitos

- Docker e Docker Compose
- Node.js 20+ (para scripts locais)
- Conta em https://webhook.site (copiar a URL)

---

## Setup e execução

### 1. Configurar webhook

```bash
cp .env.example .env
# Edite .env e coloque sua URL do webhook.site:
# WEBHOOK_URL=https://webhook.site/seu-uuid-aqui
```

### 2. Colocar o CSV base

```bash
# Copie o arquivo base_vendas_teste.csv para:
cp base_vendas_teste.csv scripts/generate-batch/Base\ de\ Dados.csv
```

### 3. Subir toda a stack

```bash
docker compose up --build
```

**O que acontece automaticamente:**
- LocalStack inicia e cria todas as filas SQS e buckets S3
- PostgreSQL inicializa com o schema completo
- 5 microserviços sobem e ficam aguardando eventos

### 4. Verificar que tudo está saudável

```bash
curl http://localhost:3001/health    # ingestion
curl http://localhost:3005/health    # observability
curl http://localhost:3005/metrics/queues  # filas SQS
```

---

## Enviando eventos

### Real-time (1 evento)

```bash
curl -X POST http://localhost:3001/events \
  -H "Content-Type: application/json" \
  -d '{
    "order_id": "ORD-TEST-001",
    "event": "order.approved",
    "customer_email": "john.smith@gmail.com",
    "customer_first_name": "John",
    "customer_last_name": "Smith",
    "customer_phone": "+18005551234",
    "customer_country": "US",
    "product_id": "PROD-001",
    "product_name": "Fit Burn",
    "product_niche": "weight_loss",
    "quantity": "3",
    "price_usd": "147",
    "payment_method": "credit_card",
    "payment_gateway": "cartpanda",
    "payment_status": "approved",
    "funnel_source": "facebook_ads",
    "utm_campaign": "fb_fitburn_cold",
    "created_at": "2026-02-10T14:32:00Z"
  }'
```

### Batch — CSV direto (100 registros do teste)

```bash
curl -X POST http://localhost:3001/batch \
  -H "Content-Type: text/plain" \
  --data-binary @scripts/generate-batch/Base\ de\ Dados.csv
```

### Batch — 10k registros (Etapa II)

```bash
# Gerar batch
cd scripts/generate-batch
node generate-batch.js          # → 10.000 registros
node generate-batch.js 50000    # → 50.000 registros
node generate-batch.js 1000000  # → 1MM registros

# Enviar
curl -X POST http://localhost:3001/batch \
  -H "Content-Type: text/plain" \
  --data-binary @scripts/generate-batch/batch_10000.csv
```

### Simulador real-time

```bash
node scripts/simulate-realtime.js          # 100 eventos, 10/s
node scripts/simulate-realtime.js 500 5    # 500 eventos, 5/s
node scripts/simulate-realtime.js 2000 20  # 2000 eventos, 20/s
```

---

## Observabilidade

| Endpoint | Descrição |
|---|---|
| `GET :3005/health` | Health check geral + DB |
| `GET :3005/metrics/queues` | Mensagens em cada fila SQS |
| `GET :3005/metrics/summary` | Totais: enviados/erro/descartado |
| `GET :3005/metrics/latency` | Latência p50/p95 venda→entrega |
| `GET :3005/metrics/error-rate` | Taxa de erro por hora (alerta se >5%) |
| `GET :3005/metrics/pipeline-health` | Detecta pipeline parado >30min |
| `GET :3005/metrics/reconciliation` | DB vs filas (gap de leads) |
| `GET :3005/metrics/recent-errors` | Últimos 50 erros |
| `GET :3005/metrics/dlq` | Dead Letter Queue (falhas fatais) |
| `GET :3005/audit/:orderId` | Trilha completa de um pedido |

---

## Recursos AWS criados (LocalStack)

| Recurso | Nome | Tipo |
|---|---|---|
| SQS | `gex-events-raw.fifo` | FIFO — entrada principal |
| SQS | `gex-leads-valid.fifo` | FIFO — leads aprovados |
| SQS | `gex-leads-discarded` | Standard — descartados |
| SQS | `gex-delivery-results` | Standard — resultados |
| SQS | `gex-events-raw-dlq` | Standard — dead letter |
| S3 | `gex-sales-batch` | Batch histórico CSV |
| S3 | `gex-audit-logs` | Logs e auditoria |
| CloudWatch | `/gex/*` | Log groups por serviço |
| SNS | `gex-alerts` | Alertas críticos |

---

## Decisões de arquitetura

**Por que SQS FIFO para entrada?**
Garante que o mesmo `order_id` não seja processado duas vezes concorrentemente. O `MessageDeduplicationId` baseado no `order_id` previne duplicatas mesmo em reenvios.

**Por que Long Polling (20s)?**
Reduz custos (menos requests vazios) e diminui latência vs polling curto.

**Por que PostgreSQL com ON CONFLICT?**
Idempotência nativa: reprocessar o mesmo lead N vezes sempre resulta no mesmo estado final. Sem duplicatas mesmo em falhas de rede.

**Por que audit trail separado?**
Tabela `lead_audit_trail` é append-only (imutável). Permite reconstruir exatamente o que aconteceu com qualquer pedido, mesmo após atualização do `lead_control`.

**Por que backoff exponencial no delivery?**
Protege o webhook externo de ser sobrecarregado em caso de instabilidade. 3 tentativas com delay 1s → 2s → 4s + jitter.

---

## Estrutura de arquivos

```
├── docker-compose.yaml              ← Stack completa
├── .env.example                     ← Configuração
├── infra/
│   ├── init.sql                     ← Schema PostgreSQL
│   └── localstack/
│       └── init-aws.sh              ← Cria recursos AWS automaticamente
├── services/
│   ├── ingestion/     :3001         ← HTTP + S3 upload
│   ├── validation/                  ← Filtra + enriquece leads
│   ├── delivery/                    ← Envia para webhook
│   ├── persistence/                 ← Grava no PostgreSQL
│   └── observability/ :3005         ← Métricas e auditoria
├── shared/
│   └── src/
│       ├── aws-client.js            ← SQS/S3 helpers + SQS Worker
│       ├── validators.js            ← Validação email/phone/lead
│       └── logger.js                ← Logger estruturado (pino)
├── scripts/
│   ├── generate-batch/
│   │   └── generate-batch.js        ← Gera 10k-1MM eventos
│   └── simulate-realtime.js         ← Simula eventos em tempo real
└── docs/
    ├── consultas_auditoria.sql      ← Queries de monitoramento
    └── RUNBOOK.md                   ← Guia de investigação de incidentes
```

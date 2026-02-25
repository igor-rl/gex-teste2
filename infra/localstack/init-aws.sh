#!/bin/bash
# ─────────────────────────────────────────────────────────────────
# GEX — LocalStack AWS Resource Bootstrap
# Executa automaticamente quando o LocalStack está pronto
# ─────────────────────────────────────────────────────────────────
set -e

echo "GEX — Criando recursos AWS no LocalStack..."

# ─── SQS Queues ──────────────────────────────────────────────────

# DLQ para eventos que falharam após todas as retentativas
awslocal sqs create-queue \
  --queue-name gex-events-raw-dlq \
  --attributes '{
    "MessageRetentionPeriod": "1209600",
    "VisibilityTimeout": "60"
  }'
echo "DLQ criada: gex-events-raw-dlq"

# Fila principal de entrada — FIFO para garantir ordem e deduplicação
awslocal sqs create-queue \
  --queue-name gex-events-raw.fifo \
  --attributes '{
    "FifoQueue": "true",
    "ContentBasedDeduplication": "true",
    "VisibilityTimeout": "60",
    "MessageRetentionPeriod": "86400",
    "RedrivePolicy": "{\"deadLetterTargetArn\":\"arn:aws:sqs:us-east-1:000000000000:gex-events-raw-dlq\",\"maxReceiveCount\":\"3\"}"
  }'
echo "Fila criada: gex-events-raw.fifo"

# Fila de leads válidos — FIFO
awslocal sqs create-queue \
  --queue-name gex-leads-valid.fifo \
  --attributes '{
    "FifoQueue": "true",
    "ContentBasedDeduplication": "true",
    "VisibilityTimeout": "30",
    "MessageRetentionPeriod": "86400"
  }'
echo "Fila criada: gex-leads-valid.fifo"

# Fila de leads descartados — standard
awslocal sqs create-queue \
  --queue-name gex-leads-discarded \
  --attributes '{
    "VisibilityTimeout": "30",
    "MessageRetentionPeriod": "604800"
  }'
echo "Fila criada: gex-leads-discarded"

# Fila de resultados de entrega — standard
awslocal sqs create-queue \
  --queue-name gex-delivery-results \
  --attributes '{
    "VisibilityTimeout": "30",
    "MessageRetentionPeriod": "86400"
  }'
echo "Fila criada: gex-delivery-results"

# ─── S3 Buckets ──────────────────────────────────────────────────

# Bucket para uploads de batch histórico
awslocal s3 mb s3://gex-sales-batch
awslocal s3api put-bucket-versioning \
  --bucket gex-sales-batch \
  --versioning-configuration Status=Enabled

# Notificação S3 → SQS quando arquivo CSV é enviado
awslocal s3api put-bucket-notification-configuration \
  --bucket gex-sales-batch \
  --notification-configuration '{
    "QueueConfigurations": [{
      "QueueArn": "arn:aws:sqs:us-east-1:000000000000:gex-events-raw.fifo",
      "Events": ["s3:ObjectCreated:*"],
      "Filter": {
        "Key": {
          "FilterRules": [{"Name": "suffix", "Value": ".csv"}]
        }
      }
    }]
  }' 2>/dev/null || echo "⚠️  S3 notification config (non-critical)"

echo "Bucket criado: gex-sales-batch"

# Bucket para logs e auditoria
awslocal s3 mb s3://gex-audit-logs
echo "Bucket criado: gex-audit-logs"

# ─── CloudWatch Log Groups ───────────────────────────────────────

for service in ingestion validation delivery persistence observability; do
  awslocal logs create-log-group --log-group-name "/gex/$service" 2>/dev/null || true
  awslocal logs put-retention-policy \
    --log-group-name "/gex/$service" \
    --retention-in-days 7 2>/dev/null || true
done
echo "CloudWatch Log Groups criados"

# ─── SNS Topics ──────────────────────────────────────────────────

# Tópico para alertas críticos
awslocal sns create-topic --name gex-alerts
echo "SNS Topic criado: gex-alerts"

# ─── Verificação final ───────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════"
echo "Todos os recursos AWS criados!"
echo "═══════════════════════════════════════════"
echo ""
echo "Filas SQS:"
awslocal sqs list-queues --output text 2>/dev/null || true
echo ""
echo "Buckets S3:"
awslocal s3 ls 2>/dev/null || true

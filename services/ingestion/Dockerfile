# ─── Dev ─────────────────────────────────────────────────────────
FROM node:20-alpine AS dev
WORKDIR /app
COPY shared ./shared
COPY services/ingestion/package*.json ./
COPY services/ingestion .
RUN npm install
CMD ["npx", "nodemon", "--legacy-watch", "src/index.js"]

# ─── Builder ─────────────────────────────────────────────────────
FROM node:20-alpine AS builder
WORKDIR /app
COPY package*.json ./
RUN npm ci --omit=dev

# ─── Runtime ─────────────────────────────────────────────────────
FROM node:20-alpine AS runtime
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
WORKDIR /app
COPY --from=builder /app/node_modules ./node_modules
COPY . .
RUN chown -R appuser:appgroup /app
USER appuser
HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD node -e "process.exit(0)"
CMD ["node", "src/index.js"]

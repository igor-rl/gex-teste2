'use strict'

const pino = require('pino')

/**
 * Cria um logger estruturado com campos padrão de rastreamento.
 * Todos os logs incluem: service, correlation_id, order_id quando disponíveis.
 */
function createLogger(service) {
  return pino({
    level: process.env.LOG_LEVEL || 'info',
    transport: process.env.NODE_ENV === 'development'
      ? { target: 'pino-pretty', options: { colorize: true, translateTime: 'SYS:standard' } }
      : undefined,
    base: { service },
    timestamp: pino.stdTimeFunctions.isoTime,
    formatters: {
      level: (label) => ({ level: label }),
    },
    serializers: {
      err: pino.stdSerializers.err,
    },
  })
}

module.exports = { createLogger }

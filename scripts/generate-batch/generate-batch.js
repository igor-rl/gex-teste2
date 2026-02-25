'use strict'

/**
 * GEX — Batch Generator
 * Gera N eventos a partir do CSV base, com order_ids únicos.
 * 
 * Uso:
 *   node generate-batch.js           → 10.000 registros
 *   node generate-batch.js 50000     → 50.000 registros
 *   node generate-batch.js 1000000   → 1MM registros
 */

const fs   = require('fs')
const path = require('path')

const INPUT_FILE  = path.resolve(__dirname, 'Base de Dados.csv')
const TARGET_SIZE = parseInt(process.argv[2], 10) || 10_000

function parseCSV(content) {
  const lines = content
    .toString('utf8')
    .replace(/^\uFEFF/, '') // BOM
    .split('\n')
    .filter(l => l.trim())

  const headers = lines[0].split(',').map(h => h.trim())
  return {
    headers,
    records: lines.slice(1).map(line => {
      const values = line.split(',')
      return Object.fromEntries(headers.map((h, i) => [h, (values[i] || '').trim()]))
    }),
  }
}

function stringifyCSV(rows, headers) {
  const escape = v => (v.includes(',') || v.includes('"') ? `"${v.replace(/"/g, '""')}"` : v)
  return [
    headers.join(','),
    ...rows.map(r => headers.map(h => escape(r[h] ?? '')).join(',')),
  ].join('\n')
}

function pad(n) { return String(n).padStart(8, '0') }

function generateBatch() {
  if (!fs.existsSync(INPUT_FILE)) {
    console.error(`❌ Arquivo não encontrado: ${INPUT_FILE}`)
    console.error('   Coloque o CSV base em scripts/generate-batch/Base de Dados.csv')
    process.exit(1)
  }

  const { headers, records } = parseCSV(fs.readFileSync(INPUT_FILE))
  if (!records.length) throw new Error('CSV vazio ou inválido')

  console.log(`📦 Base: ${records.length} registros → gerando ${TARGET_SIZE.toLocaleString()}...`)

  const rows    = []
  let   counter = 1

  while (rows.length < TARGET_SIZE) {
    for (const row of records) {
      if (rows.length >= TARGET_SIZE) break
      // Garante order_id único
      rows.push({ ...row, order_id: `ORD-GEN-${pad(counter++)}` })
    }
  }

  const outputFile = path.resolve(__dirname, `batch_${TARGET_SIZE}.csv`)
  fs.writeFileSync(outputFile, stringifyCSV(rows, headers))

  const fileSizeMb = (fs.statSync(outputFile).size / 1024 / 1024).toFixed(2)
  console.log(`✅ ${rows.length.toLocaleString()} registros gerados`)
  console.log(`📄 Arquivo: ${outputFile} (${fileSizeMb} MB)`)
  console.log(``)
  console.log(`Para enviar via HTTP batch:`)
  console.log(`  curl -X POST http://localhost:3001/batch \\`)
  console.log(`    -H "Content-Type: text/plain" \\`)
  console.log(`    --data-binary @${outputFile}`)
}

generateBatch()

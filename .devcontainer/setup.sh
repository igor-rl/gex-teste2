#!/bin/bash
set -e

for service in ingestion validation delivery persistence observability; do
  [ -f "services/$service/package.json" ] && npm install --prefix services/$service --silent
done

[ -f "scripts/package.json" ] && npm install --prefix scripts --silent

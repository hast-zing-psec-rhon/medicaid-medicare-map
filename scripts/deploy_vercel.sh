#!/usr/bin/env bash
set -euo pipefail

BACKEND_URL="${1:-}"
DEPLOY_MODE="${2:---prod}"

if [[ -z "$BACKEND_URL" ]]; then
  echo "Usage: $0 <backend-url> [--prod|--prebuilt|additional vercel args]" >&2
  exit 1
fi

if ! command -v npx >/dev/null 2>&1; then
  echo "npx is required to run the Vercel CLI." >&2
  exit 1
fi

if ! npx vercel whoami >/dev/null 2>&1; then
  echo "No active Vercel session found; launching login..."
  npx vercel login
fi

echo "Deploying frontend to Vercel with PUBLIC_API_BASE_URL=${BACKEND_URL}"
npx vercel ${DEPLOY_MODE} --build-env "PUBLIC_API_BASE_URL=${BACKEND_URL}"

#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${1:-}"
SERVICE_NAME="${2:-medicaid-medicare-map-api}"
REGION="${3:-us-central1}"

if [[ -z "$PROJECT_ID" ]]; then
  echo "Usage: $0 <gcp-project-id> [service-name] [region]" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "$REPO_DIR"

"${SCRIPT_DIR}/deploy_cloud_run.sh" "$PROJECT_ID" "$SERVICE_NAME" "$REGION"

BACKEND_URL="$(gcloud run services describe "$SERVICE_NAME" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"

echo
echo "Starting Vercel deployment..."
"${SCRIPT_DIR}/deploy_vercel.sh" "$BACKEND_URL" --prod

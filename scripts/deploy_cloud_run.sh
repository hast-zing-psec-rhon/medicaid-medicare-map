#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${1:-}"
SERVICE_NAME="${2:-medicaid-medicare-map-api}"
REGION="${3:-us-central1}"

if [[ -z "$PROJECT_ID" ]]; then
  echo "Usage: $0 <gcp-project-id> [service-name] [region]" >&2
  exit 1
fi

if ! command -v gcloud >/dev/null 2>&1; then
  echo "gcloud CLI is required." >&2
  exit 1
fi

if ! gcloud auth list --filter=status:ACTIVE --format='value(account)' | grep -q .; then
  echo "No active gcloud account found; launching login..."
  gcloud auth login
fi

gcloud config set project "$PROJECT_ID" >/dev/null

echo "Enabling required Google Cloud APIs..."
gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com >/dev/null

ALLOWED_ORIGINS="${APP_ALLOWED_ORIGINS:-http://127.0.0.1:8000,http://localhost:8000,http://127.0.0.1:8080,http://localhost:8080}"
ALLOWED_ORIGIN_REGEX="${APP_ALLOWED_ORIGIN_REGEX:-^https://.*\\.vercel\\.app$}"

echo "Deploying ${SERVICE_NAME} to Cloud Run (${REGION})..."
gcloud run deploy "$SERVICE_NAME" \
  --project "$PROJECT_ID" \
  --region "$REGION" \
  --source . \
  --allow-unauthenticated \
  --set-env-vars "APP_SERVE_FRONTEND=false,APP_ALLOWED_ORIGINS=${ALLOWED_ORIGINS},APP_ALLOWED_ORIGIN_REGEX=${ALLOWED_ORIGIN_REGEX}" \
  --quiet

SERVICE_URL="$(gcloud run services describe "$SERVICE_NAME" --project "$PROJECT_ID" --region "$REGION" --format='value(status.url)')"

echo
echo "Cloud Run deploy complete."
echo "Backend URL: ${SERVICE_URL}"
echo "Use this for Vercel build env: PUBLIC_API_BASE_URL=${SERVICE_URL}"

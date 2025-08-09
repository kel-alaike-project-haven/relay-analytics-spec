#!/usr/bin/env bash
set -euo pipefail

# ======= CONFIG =======
PROJECT_ID="relay-analytics-demo"
REGION="europe-west2"

# Buckets you plan to use (created elsewhere by Terraform)
RAW_BUCKET="${PROJECT_ID}-raw"
DLQ_BUCKET="${PROJECT_ID}-dlq"
DOCS_BUCKET="${PROJECT_ID}-docs"

gcloud config set project "${PROJECT_ID}" >/dev/null

create_sa () {
  local SA_ID="$1"
  local DESC="$2"
  if gcloud iam service-accounts list --format="value(email)" | grep -q "^${SA_ID}@${PROJECT_ID}.iam.gserviceaccount.com$"; then
    echo "✓ SA exists: ${SA_ID}"
  else
    gcloud iam service-accounts create "${SA_ID}" --display-name "${DESC}"
    echo "✓ SA created: ${SA_ID}"
  fi
}

bind_role () {
  local SA_ID="$1"
  local ROLE="$2"
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_ID}@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="roles/${ROLE}" >/dev/null
  echo "  ↳ roles/${ROLE}"
}

# ======= CREATE SERVICE ACCOUNTS =======
create_sa "sa-terraform"   "Terraform Automation"
create_sa "sa-generator"   "Event Generator (Pub/Sub publisher)"
create_sa "sa-ingest"      "Ingest (Pub/Sub push -> BigQuery Bronze)"
create_sa "sa-dataflow"    "Dataflow cold-path archiver"
create_sa "sa-dbt-runner"  "dbt Runner (BSG transforms + docs)"
create_sa "sa-api"         "Export/API (BQ read)"
create_sa "sa-ci-deployer" "CI Deployer (GitHub Actions via WIF)"

# ======= BIND ROLES =======
echo "Binding roles to sa-terraform"
bind_role "sa-terraform" "run.admin"
bind_role "sa-terraform" "artifactregistry.admin"
bind_role "sa-terraform" "pubsub.admin"
bind_role "sa-terraform" "storage.admin"
bind_role "sa-terraform" "bigquery.admin"
bind_role "sa-terraform" "iam.serviceAccountAdmin"
bind_role "sa-terraform" "iam.serviceAccountUser"
bind_role "sa-terraform" "serviceusage.serviceUsageAdmin"
bind_role "sa-terraform" "cloudbuild.builds.editor"

echo "Binding roles to sa-generator"
bind_role "sa-generator" "pubsub.publisher"

echo "Binding roles to sa-ingest"
bind_role "sa-ingest" "bigquery.dataEditor"
bind_role "sa-ingest" "pubsub.publisher"

echo "Binding roles to sa-dataflow"
bind_role "sa-dataflow" "dataflow.worker"
bind_role "sa-dataflow" "pubsub.subscriber"
bind_role "sa-dataflow" "storage.objectAdmin"

echo "Binding roles to sa-dbt-runner"
bind_role "sa-dbt-runner" "bigquery.jobUser"
bind_role "sa-dbt-runner" "bigquery.dataEditor"
bind_role "sa-dbt-runner" "storage.objectAdmin"

echo "Binding roles to sa-api"
bind_role "sa-api" "bigquery.jobUser"
bind_role "sa-api" "bigquery.dataViewer"

echo "Binding roles to sa-ci-deployer"
bind_role "sa-ci-deployer" "cloudbuild.builds.editor"
bind_role "sa-ci-deployer" "artifactregistry.admin"

# Allow CI SA to impersonate Terraform SA (for WIF)
gcloud iam service-accounts add-iam-policy-binding "sa-terraform@${PROJECT_ID}.iam.gserviceaccount.com" \
  --member="serviceAccount:sa-ci-deployer@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/iam.serviceAccountTokenCreator" >/dev/null
echo "  ↳ sa-ci-deployer can impersonate sa-terraform (roles/iam.serviceAccountTokenCreator)"

echo "All service accounts created and roles bound."

# ======= OPTIONAL: dataset- or bucket-scoped bindings (uncomment to tighten scope) =======
# gcloud storage buckets add-iam-policy-binding "gs://${RAW_BUCKET}" \
#   --member="serviceAccount:sa-dataflow@${PROJECT_ID}.iam.gserviceaccount.com" \
#   --role="roles/storage.objectAdmin"
# gcloud storage buckets add-iam-policy-binding "gs://${DOCS_BUCKET}" \
#   --member="serviceAccount:sa-dbt-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
#   --role="roles/storage.objectAdmin"

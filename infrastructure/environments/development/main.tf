#############################################
# main.tf — module wiring (dev providers)
#############################################

# Providers (dev)
provider "google" {
  alias                       = "dev"
  project                     = var.dev_project
  region                      = var.dev_region
  impersonate_service_account = var.dev_service_account
}

provider "google-beta" {
  alias                       = "beta_dev"
  project                     = var.dev_project
  region                      = var.dev_region
  impersonate_service_account = var.dev_service_account
}

#############################################
# Modules (loaded from ../../modules/*)
#############################################

# Cloud Run (services + optional push subscription wiring)
module "cloud_run" {
  source = "../../modules/cloud_run"

  providers = {
    google      = google.dev
  }

  # Pass the env-filtered services map from locals.tf
  all_cloud_run_variables = local.cloud_run_services
}

# Cloud Storage (buckets, lifecycle, cors, retention)
module "cloud_storage" {
  source = "../../modules/cloud_storage"

  providers = {
    google = google.dev
  }

  all_cloud_storage_variables = local.buckets
}

# BigQuery datasets (IAM, labels, defaults)
module "datasets" {
  source = "../../modules/datasets"

  providers = {
    google = google.dev
  }

  all_bigquery_datasets_variables = local.datasets
}

# Pub/Sub (topics + subscriptions)
module "pubsub" {
  source = "../../modules/pubsub"

  providers = {
    google = google.dev
  }

  all_pubsub_topics_variables         = local.topics
  all_pubsub_subscriptions_variables  = local.subscriptions
}

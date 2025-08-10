############################################################
# locals.tf — load JSON configs and expose maps/lists
############################################################

# Which environment slice to use (dev|stg|prod, etc.)
variable "environment" {
  type        = string
  description = "Environment selector for config filtering"
  default     = "dev"
}

locals {

  # ---------- Load raw JSON ----------
  cloud_build_data       = jsondecode(file("../../modules/cloud_build/config/cloud_build.json"))
  cloud_run_data         = jsondecode(file("../../modules/cloud_run/config/cloud_run.json"))
  cloud_storage_data     = jsondecode(file("../../modules/cloud_storage/config/cloud_storage.json"))
  datasets_data          = jsondecode(file("../../modules/datasets/config/datasets.json"))
  pubsub_subscriptions   = jsondecode(file("../../modules/pubsub/config/pubsub_subscriptions.json"))
  pubsub_topics          = jsondecode(file("../../modules/pubsub/config/pubsub_topics.json"))

  # ---------- Normalize to keyed maps (env/id) ----------

  all_cloud_build_triggers = {
    for s in local.cloud_build_data.cloud_build_triggers :
    "${s.environment}/${s.service_name}" => s
  }

  all_cloud_run_services = {
    for s in local.cloud_run_data.cloud_run_services :
    "${s.environment}/${s.service_name}" => s
  }

  all_buckets = {
    for b in local.cloud_storage_data.buckets :
    "${b.environment}/${b.name}" => b
  }

  all_datasets = {
    for d in local.datasets_data.bigquery_datasets :
    "${d.environment}/${d.dataset_id}" => d
  }

  all_topics = {
    for t in local.pubsub_topics.topics :
    "${t.environment}/${t.topic_id}" => t
  }

  all_subscriptions = {
    for s in local.pubsub_subscriptions.subscriptions :
    "${s.environment}/${s.subscription_id}" => s
  }

  # ---------- Environment-filtered subsets ----------
  cloud_build_services = {
    for k, v in local.all_cloud_build_triggers : k => v
    if v.environment == var.environment
  }

  cloud_run_services = {
    for k, v in local.all_cloud_run_services : k => v
    if v.environment == var.environment
  }

  buckets = {
    for k, v in local.all_buckets : k => v
    if v.environment == var.environment
  }

  datasets = {
    for k, v in local.all_datasets : k => v
    if v.environment == var.environment
  }

  topics = {
    for k, v in local.all_topics : k => v
    if v.environment == var.environment
  }

  subscriptions = {
    for k, v in local.all_subscriptions : k => v
    if v.environment == var.environment
  }

  # ---------- Convenience lists (sometimes easier than maps) ----------
  cloud_build_services_list = values(local.cloud_build_services)
  cloud_run_services_list   = values(local.cloud_run_services)
  buckets_list              = values(local.buckets)
  datasets_list             = values(local.datasets)
  topics_list               = values(local.topics)
  subscriptions_list        = values(local.subscriptions)
}

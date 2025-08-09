resource "google_storage_bucket" "bucket_dev" {
  for_each = {
    for k, v in var.all_cloud_storage_variables : k => v
    if v.environment == "dev"
  }

  project  = each.value.project_id
  name     = each.value.name
  location = each.value.location

  storage_class                 = each.value.storage_class
  uniform_bucket_level_access   = each.value.uniform_bucket_level_access
  labels                        = try(each.value.labels, {})

  # Versioning block expects "enabled", include always using your boolean
  versioning {
    enabled = each.value.versioning
  }

  # Retention policy only if provided
  dynamic "retention_policy" {
    for_each = try(each.value.retention_policy, null) != null ? [each.value.retention_policy] : []
    content {
      retention_period = retention_policy.value.retention_period
    }
  }

  # Lifecycle rules (zero, one, or many)
  dynamic "lifecycle_rule" {
    for_each = try(each.value.lifecycle_rules, [])
    content {
      action {
        type          = lifecycle_rule.value.action.type
        storage_class = try(lifecycle_rule.value.action.storageClass, null)
      }
      condition {
        age                 = try(lifecycle_rule.value.condition.age, null)
        created_before      = try(lifecycle_rule.value.condition.createdBefore, null)
        with_state          = try(lifecycle_rule.value.condition.withState, null)
        matches_storage_class = try(lifecycle_rule.value.condition.matchesStorageClass, null)
        num_newer_versions  = try(lifecycle_rule.value.condition.numNewerVersions, null)
      }
    }
  }

  # CORS (zero, one, or many)
  dynamic "cors" {
    for_each = try(each.value.cors, [])
    content {
      origin          = cors.value.origin
      method          = cors.value.method
      response_header = try(cors.value.responseHeader, null)
      max_age_seconds = try(cors.value.maxAgeSeconds, null)
    }
  }
}

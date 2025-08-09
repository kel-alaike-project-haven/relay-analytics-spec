# main.tf — BigQuery datasets from variables (env == "dev" only)

resource "google_bigquery_dataset" "dataset_dev" {
  for_each = {
    for k, v in var.all_bigquery_datasets_variables : k => v
    if v.environment == "dev"
  }

  project     = each.value.project_id
  dataset_id  = each.value.dataset_id
  location    = each.value.location
  description = try(each.value.description, null)
  labels      = try(each.value.labels, null)

  # Dataset-level defaults (safe to 0 if not provided)
  default_table_expiration_ms     = try(each.value.default_table_expiration_ms, 0)
  default_partition_expiration_ms = try(each.value.default_partition_expiration_ms, 0)

  # Helpful in non-prod so 'destroy' cleans up tables too; override in vars if you want
  delete_contents_on_destroy = try(each.value.delete_contents_on_destroy, true)

  # Access blocks (supports group/user/domain/specialGroup/iam_member or view grants)
  dynamic "access" {
    for_each = try(each.value.access, [])
    content {
      role            = try(access.value.role, null)
      user_by_email   = try(access.value.user_by_email, null)
      group_by_email  = try(access.value.group_by_email, null)
      domain          = try(access.value.domain, null)
      special_group   = try(access.value.special_group, null)
      iam_member      = try(access.value.iam_member, null)

      # Optional view-level access (when granting to a view)
      dynamic "view" {
        for_each = try([access.value.view], [])
        content {
          project_id = view.value.project_id
          dataset_id = view.value.dataset_id
          table_id   = view.value.table_id
        }
      }
    }
  }
}

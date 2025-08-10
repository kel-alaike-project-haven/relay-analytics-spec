resource "google_cloudbuild_trigger" "trigger" {
  for_each = {
    for k, v in var.all_cloud_build_triggers_variables : k => v
    if v.environment == "dev"
  }

  name = "${each.value.service_name}-build-trigger"

  github {
    owner = each.value.github_owner
    name  = each.value.github_repo
    push {
      branch = each.value.branch
    }
  }

  filename = each.value.cloudbuild_file
}

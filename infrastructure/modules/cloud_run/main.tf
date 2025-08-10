resource "google_cloud_run_service" "svc_dev" {
  for_each = {
    for k, v in var.all_cloud_run_variables : k => v
    if v.environment == "dev"
  }

  project  = each.value.project_id
  name     = each.value.service_name
  location = each.value.region

  metadata {
    annotations = {
      "run.googleapis.com/ingress" = try(each.value.ingress, "all")
    }
  }

  template {
    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale" = tostring(try(each.value.min_instances, 0))
        "autoscaling.knative.dev/maxScale" = tostring(try(each.value.max_instances, 3))
      }
    }
    spec {
      service_account_name  = try(each.value.service_account_email, null)
      container_concurrency = try(each.value.concurrency, 80)
      timeout_seconds       = try(each.value.request_timeout_seconds, 60)

      containers {
        image = each.value.image

        dynamic "env" {
          for_each = try(each.value.env, {})
          content {
            name  = env.key
            value = env.value
          }
        }

        resources {
          limits = {
            memory = try(each.value.memory, "512Mi")
            cpu    = tostring(try(each.value.cpu, 1))
          }
        }

        ports {
          name           = "http1"
          container_port = 8080
        }
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  autogenerate_revision_name = true
}

# If you want unauthenticated access, add this IAM binding:
resource "google_cloud_run_service_iam_member" "svc_invoker_public" {
  for_each = {
    for k, v in var.all_cloud_run_variables : k => v
    if v.environment == "dev" && try(v.allow_unauthenticated, false)
  }
  project  = each.value.project_id
  location = each.value.region
  service  = google_cloud_run_service.svc_dev[each.key].name
  role     = "roles/run.invoker"
  member   = "allUsers"
}

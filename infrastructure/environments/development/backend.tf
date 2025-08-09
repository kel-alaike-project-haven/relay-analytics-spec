terraform {
  backend "gcs" {
    bucket = "project-haven-infrastructure"
    prefix = "terraform/state"
  }
}

#############################################
# Topics
#############################################
resource "google_pubsub_topic" "topic_dev" {
  # Key by topic_id so we can reference from subs directly
  for_each = {
    for _, v in var.all_pubsub_topics_variables :
    v.topic_id => v
    if v.environment == "dev"
  }


  project = each.value.project_id
  name    = each.value.topic_id
  labels  = try(each.value.labels, null)

  dynamic "message_storage_policy" {
    for_each = try(each.value.message_storage_policy, null) != null ? [each.value.message_storage_policy] : []
    content {
      allowed_persistence_regions = try(message_storage_policy.value.allowed_persistence_regions, null)
    }
  }
}

#############################################
# Subscriptions
#############################################
resource "google_pubsub_subscription" "sub_dev" {
  # Key by subscription_id
  for_each = {
    for _, v in var.all_pubsub_subscriptions_variables :
    v.subscription_id => v
    if v.environment == "dev"
  }

  project = each.value.project_id
  name    = each.value.subscription_id

  # Reference topic by its topic_id key from topic_dev
  topic = google_pubsub_topic.topic_dev[each.value.topic_id].name

  ack_deadline_seconds     = try(each.value.ack_deadline_seconds, 10)
  retain_acked_messages    = try(each.value.retain_acked_messages, false)
  message_retention_duration = try(each.value.message_retention_duration, null)
  enable_message_ordering  = try(each.value.enable_message_ordering, false)
  labels                   = try(each.value.labels, null)
  filter                   = try(each.value.filter, null)

  # Optional push config
  dynamic "push_config" {
    for_each = try(each.value.push_config, null) != null ? [each.value.push_config] : []
    content {
      push_endpoint = push_config.value.endpoint
      oidc_token {
        service_account_email = push_config.value.oidc_service_account_email
        audience              = try(push_config.value.audience, null)
      }
    }
  }

  # Optional dead-letter policy
  dynamic "dead_letter_policy" {
    for_each = try(each.value.dead_letter_policy, null) != null ? [each.value.dead_letter_policy] : []
    content {
      dead_letter_topic     = "projects/${each.value.project_id}/topics/${dead_letter_policy.value.dead_letter_topic_id}"
      max_delivery_attempts = try(dead_letter_policy.value.max_delivery_attempts, 5)
    }
  }

  # Optional retry policy
  dynamic "retry_policy" {
    for_each = try(each.value.retry_policy, null) != null ? [each.value.retry_policy] : []
    content {
      minimum_backoff = try(retry_policy.value.minimum_backoff, null)
      maximum_backoff = try(retry_policy.value.maximum_backoff, null)
    }
  }
}
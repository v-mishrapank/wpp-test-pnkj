# Long-lived Container App for an automation worker. Differs from analytics's
# `container_app_job` (which has manual_trigger_config and runs to completion):
# this is a continuously-running app that listens on a Service Bus subscription
# via a KEDA scale rule, processes messages, and scales to zero when idle.
#
# Per-tenant: the env_module instantiates this once per cloud tenant from
# tenants.json (for_each over local.cloud_tenants). Each instance has its
# own SB subscription, MI, and scale rule scoped to its tenant_key.

resource "azurerm_container_app" "this" {
  name                         = var.name
  resource_group_name          = var.resource_group_name
  container_app_environment_id = var.container_app_environment_id
  revision_mode                = "Single"
  workload_profile_name        = "Consumption"
  tags                         = var.tags

  # SystemAssigned for SB / KV / target-tenant graph auth (cert from KV);
  # UserAssigned for ACR image pull (the shared aca_pull UAMI granted
  # AcrPull on the spoke ACR).
  identity {
    type         = "SystemAssigned, UserAssigned"
    identity_ids = [var.registry_identity_id]
  }

  registry {
    server   = var.registry_server
    identity = var.registry_identity_id
  }

  template {
    min_replicas = var.min_replicas
    max_replicas = var.max_replicas

    container {
      name   = "worker"
      image  = var.image
      cpu    = var.cpu
      memory = var.memory

      dynamic "env" {
        for_each = var.env_vars
        content {
          name  = env.key
          value = env.value
        }
      }
    }

    # KEDA azure-servicebus scaler. Watches activeMessageCount on the
    # subscription; activates from 0→1 on any message (activationMessageCount=0),
    # then targets messageCount messages per replica. With max=1 the
    # messageCount value is essentially a no-op for actual scaling — set to 1
    # to make the intent obvious. cooldownPeriod and pollingInterval use KEDA
    # defaults (300s / 30s) and live on the runtime config rather than here.
    #
    # identity_id = "System" tells KEDA to authenticate to SB using this Container
    # App's SystemAssigned MI. The MI must hold "Azure Service Bus Data
    # Receiver" on the subscription scope (granted out-of-band in env_module).
    # (azurerm added identity_id support in 4.69.0; capital-S "System" required.)
    custom_scale_rule {
      name             = var.scale_rule_name
      custom_rule_type = "azure-servicebus"
      metadata = {
        namespace              = var.servicebus_namespace
        topicName              = var.scale_topic_name
        subscriptionName       = var.scale_subscription_name
        messageCount           = "1"
        activationMessageCount = "0"
      }
      identity_id = "System"
    }
  }

  # CI updates the container image to a git-SHA tag via
  # `az containerapp update --image <ACR>/...:<sha>` after each build.
  # TF would otherwise persistently revert it to whatever tag is in config,
  # defeating the SHA-pinned deploy. Same lifecycle pattern as analytics
  # container_app_job.
  lifecycle {
    ignore_changes = [template[0].container[0].image]
  }
}

variable "name" {
  description = "Container App name (e.g. ca-ma-toolkit-<env>-worker-<tenant_key>-001)."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
}

variable "container_app_environment_id" {
  description = "ID of the shared ACA Environment to host the app in."
  type        = string
}

variable "registry_server" {
  description = "ACR login server (e.g. crmatoolkitbranch001.azurecr.io)."
  type        = string
}

variable "registry_identity_id" {
  description = "Resource ID of the UAMI used to pull images from the ACR (must hold AcrPull on the registry). The shared aca_pull UAMI is the standard choice."
  type        = string
}

variable "image" {
  description = "Container image reference including tag (e.g. <acr>/automation/worker:<sha>). CI rotates this via `az containerapp update --image`; lifecycle ignores changes here so TF doesn't fight CI."
  type        = string
}

variable "cpu" {
  description = "CPU cores allocated to the container."
  type        = number
  default     = 1.0
}

variable "memory" {
  description = "Memory allocation (e.g. 2Gi)."
  type        = string
  default     = "2Gi"
}

variable "min_replicas" {
  description = "Minimum replicas. 0 = scale to zero when idle."
  type        = number
  default     = 0
}

variable "max_replicas" {
  description = "Maximum replicas. Capped at 1 for per-tenant workers — single instance per tenant by design (PowerShell runspace pool handles concurrency within an instance)."
  type        = number
  default     = 1
}

variable "env_vars" {
  description = "Map of environment variables passed to the container."
  type        = map(string)
  default     = {}
}

variable "scale_rule_name" {
  description = "Name of the KEDA scale rule (shows up in Azure portal). Convention: sb-<worker_id>."
  type        = string
}

variable "servicebus_namespace" {
  description = "FQDN of the Service Bus namespace the scaler watches (e.g. sb-ma-toolkit-branch-001.servicebus.windows.net)."
  type        = string
}

variable "scale_topic_name" {
  description = "Name of the topic the scaler watches (e.g. worker-jobs)."
  type        = string
}

variable "scale_subscription_name" {
  description = "Name of the subscription on the topic the scaler watches (e.g. worker-madev1). The Container App's SAMI must have Azure Service Bus Data Receiver on this subscription."
  type        = string
}

variable "tags" {
  description = "Tags to apply."
  type        = map(string)
  default     = {}
}

variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "environment_name" {
  type = string
}

variable "log_analytics_workspace_id" {
  type = string
}

variable "log_retention_in_days" {
  type    = number
  default = 30
}

variable "app_name" {
  type = string
}

variable "container_name" {
  type    = string
  default = "app"
}

variable "image" {
  type = string
}

variable "cpu" {
  type    = number
  default = 0.5
}

variable "memory" {
  type    = string
  default = "1.0Gi"
}

variable "min_replicas" {
  type    = number
  default = 1
}

variable "max_replicas" {
  type    = number
  default = 2
}

variable "target_port" {
  type    = number
  default = 80
}

variable "external_enabled" {
  type    = bool
  default = true
}

variable "env_vars" {
  type    = map(string)
  default = {}
}

variable "tags" {
  type = map(string)
}

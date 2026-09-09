variable "name" {
  type = string
}

variable "resource_group_name" {
  type = string
}

variable "container_app_environment_id" {
  type = string
}

variable "revision_mode" {
  type = string
}

variable "user_assigned_identity_id" {
  type = string
}

variable "acr_login_server" {
  type = string
}

variable "github_token" {
  type      = string
  sensitive = true
}

variable "container_name" {
  type = string
}

variable "image" {
  type = string
}

variable "cpu" {
  type = number
}

variable "memory" {
  type = string
}

variable "min_replicas" {
  type = number
}

variable "max_replicas" {
  type = number
}

variable "env_vars" {
  type = map(string)
}

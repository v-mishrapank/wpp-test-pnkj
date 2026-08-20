variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "host_pool_name" {
  type = string
}

variable "workspace_name" {
  type = string
}

variable "application_group_name" {
  type = string
}

variable "friendly_name" {
  type = string
}

variable "maximum_sessions_allowed" {
  type    = number
  default = 4
}

variable "user_principal_names" {
  type    = set(string)
  default = []
}

variable "tags" {
  type    = map(string)
  default = {}
}

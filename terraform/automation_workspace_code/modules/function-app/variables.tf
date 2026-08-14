variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_prefix" {
  type = string
}

variable "service_plan_id" {
  type = string
}

variable "storage_account_name" {
  type = string
}

variable "storage_account_access_key" {
  type = string
}

variable "function_app_name" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "application_insights_connection_string" {
  type = string
}

variable "key_vault_uri" {
  type = string
}

variable "runtime" {
  type    = string
  default = "python"
}

variable "runtime_version" {
  type    = string
  default = "3.11"
}

variable "app_settings" {
  type    = map(string)
  default = {}
}

variable "tags" {
  type = map(string)
}

variable "resource_prefix" {
  type = string
}

variable "app_registration_sign_in_audience" {
  type    = string
  default = "AzureADMyOrg"
}
variable "dispatcher_function_app_name" {
  description = "Name of the dispatcher Function App."
  type        = string
}

variable "dispatcher_app_reg_name" {
  description = "Display name for the dispatcher AAD app registration. Distinct from the Function App name — AAD app regs use identity-focused naming, not Azure-resource conventions."
  type        = string
}

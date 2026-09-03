variable "name" {
  description = "Name of the Application Insights instance."
  type        = string
}

variable "location" {
  description = "Azure region."
  type        = string
}

variable "resource_group_name" {
  description = "Resource group name."
  type        = string
}

variable "log_analytics_workspace_id" {
  description = "ID of the Log Analytics Workspace to link to."
  type        = string
}

variable "tags" {
  description = "Tags to apply."
  type        = map(string)
  default     = {}
}

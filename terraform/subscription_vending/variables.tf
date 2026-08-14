variable "enrollment_account_name" {
  description = "Enrollment Account activated by WPP ET team"
  validation {
    condition     = var.enrollment_account_name != null && var.enrollment_account_name != ""
    error_message = "The Enrollment Account has not been set. Please contact WPP ET administration."
  }
}

variable "billing_account_name" {
  description = "wpp.cloud Billing account"
  default     = "69793823"
}

variable "management_group_id" {
  description = "Management Group ID to associate with customer created subscriptions"
  validation {
    condition     = var.management_group_id != null && var.management_group_id != ""
    error_message = "The Management Group ID has not been set. Please contact WPP ET administration."
  }
}

variable "sub_detail" {
  type = list(object({
    subscription_name           = string
    service_principal_object_id = optional(string, null)
    role                        = optional(string, "Reader")
  }))
  default = []
}

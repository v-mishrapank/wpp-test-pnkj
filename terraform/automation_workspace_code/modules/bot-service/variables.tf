variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "bot_name" {
  type = string
}

variable "microsoft_app_id" {
  type = string
}

variable "endpoint_url" {
  type = string
}

variable "description" {
  type    = string
  default = "Bot Service for communication workloads"
}

variable "tags" {
  type = map(string)
}

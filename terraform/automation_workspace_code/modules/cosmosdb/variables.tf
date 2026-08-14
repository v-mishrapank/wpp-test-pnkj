variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_prefix" {
  type = string
}

variable "account_name" {
  type = string
}

variable "offer_type" {
  type    = string
  default = "Standard"
}

variable "kind" {
  type    = string
  default = "GlobalDocumentDB"
}

variable "consistency_level" {
  type    = string
  default = "Session"
}

variable "capacity_mode" {
  type    = string
  default = "Serverless"
}

variable "throughput" {
  type    = number
  default = 400
}

variable "public_network_access_enabled" {
  type    = bool
  default = false
}

variable "private_endpoints_subnet_id" {
  type = string
}

variable "private_dns_zone_cosmos_id" {
  type = string
}

variable "tags" {
  type = map(string)
}

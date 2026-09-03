variable "name" {
  description = "Name of the private endpoint."
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

variable "subnet_id" {
  description = "ID of the subnet for the private endpoint."
  type        = string
}

variable "service_connection_name" {
  description = "Name of the private service connection."
  type        = string
}

variable "private_connection_resource_id" {
  description = "Resource ID of the target resource."
  type        = string
}

variable "is_manual_connection" {
  description = "Is this a manual connection."
  type        = bool
  default     = false
}

variable "subresource_names" {
  description = "Subresource names for the connection."
  type        = list(string)
}

variable "private_dns_zone_ids" {
  description = "List of private DNS zone IDs for DNS registration."
  type        = list(string)
  default     = []
}

variable "tags" {
  description = "Tags to apply."
  type        = map(string)
  default     = {}
}

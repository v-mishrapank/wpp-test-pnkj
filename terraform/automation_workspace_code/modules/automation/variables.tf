variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "resource_prefix" {
  type = string
}

variable "automation_name" {
  type = string
}

variable "identity_id" {
  type = string
}

variable "enable_hybrid_worker" {
  type    = bool
  default = false
}

variable "tags" {
  type = map(string)
}

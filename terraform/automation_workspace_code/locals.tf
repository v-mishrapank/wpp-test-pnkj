locals {
  company      = lower(var.company)
  environment  = lower(var.environment)
  workload     = lower(var.workload)
  region_short = lower(var.location_short)

  resource_prefix             = "${local.company}-${local.environment}-${local.region_short}"
  application_resource_prefix = lower(var.application_resource_prefix)

  windows_vms = {
    vm01 = {
      name          = "vm-${local.application_resource_prefix}-001"
      computer_name = "matoolkitvm001"
      zone          = "1"
    }
    vm02 = {
      name          = "vm-${local.application_resource_prefix}-002"
      computer_name = "matoolkitvm002"
      zone          = "2"
    }
  }

  common_tags = merge(
    {
      environment        = var.environment
      owner              = "wpp-platform"
      costCenter         = "platform"
      application        = "wpp-cloud-automation"
      managedBy          = "terraform"
      tenant             = "wpp-cloud"
      workload           = var.workload
      dataClassification = "confidential"
    },
    var.tags
  )

  resource_names = {
    rg = var.resource_group_name != null ? var.resource_group_name : "${local.resource_prefix}-rg-01"
  }
}

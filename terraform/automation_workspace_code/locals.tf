locals {
  company      = lower(var.company)
  environment  = lower(var.env)
  workload     = lower(var.workload)
  region_short = lower(var.location_short)

  resource_prefix             = "${local.company}-${local.environment}-${local.region_short}"
  application_resource_prefix = lower(var.application_resource_prefix)

  common_tags = merge(
    {
      environment        = var.env
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
}
# WPP Cloud Automation Platform Terraform Deployment

This deployment provisions the Azure resources required for the WPP Cloud automation architecture, including:
- Resource group
- Virtual network and subnets
- Azure Key Vault with private endpoint
- Azure Automation Account and automation worker support
- Azure Entra app registrations and service principals
- Azure Bot Service with Teams channel
- DADM, DAPI, dispatcher, and orchestrator Function Apps
- Container Apps environment with EXO, Graph, Log, PBI, Power Platform, and SharePoint Online jobs
- Cosmos DB SQL API with database and containers
- Managed identities and RBAC
- Log Analytics and Application Insights
- Private endpoints and network hardening

## Architecture summary

The deployment is designed for the WPP Cloud tenant and follows the platform view described in the automation architecture. It uses:
- Azure managed identities for resource-to-resource access
- RBAC instead of shared secrets where supported
- Key Vault for secret storage and references
- VNet integration for Function Apps
- Private networking for sensitive services
- Log Analytics and Application Insights for monitoring

## Folder structure

- main.tf
- network.tf
- identity.tf
- aad.tf
- keyvault.tf
- automation.tf
- functions.tf
- botservice.tf
- cosmosdb.tf
- monitoring.tf
- roleassignments.tf
- outputs.tf
- README.md

## Prerequisites

Before running Terraform:
- Azure subscription access to the WPP Cloud tenant
- Azure AD / Entra admin access for app registrations and service principals
- Owner or Contributor rights for resource creation
- Terraform v1.6 or newer
- Azure CLI login for local deployment
- Access to the target subscription and tenant

## Required variables

At minimum, provide:
- tenant_id
- subscription_id

The application resources use `application_resource_prefix = "ma-toolkit-branch"` by default, producing the names shown in the reference screenshots. Override `container_app_job_images` with the workload images before deployment; the defaults are bootstrap images intended to validate resource provisioning.

Example:
```hcl
tenant_id       = "00000000-0000-0000-0000-000000000000"
subscription_id = "11111111-1111-1111-1111-111111111111"
environment     = "nonprod"
location        = "uksouth"
```

## Deployment steps

```bash
terraform init
terraform fmt
terraform validate
terraform plan
terraform apply
```

## Managed identity model

This deployment uses Azure managed identities for secure resource-to-resource communication:
- Function Apps access Key Vault via managed identity and RBAC
- Function Apps access Cosmos DB via managed identity and RBAC
- Automation Account reads secrets from Key Vault using managed identity
- App registrations are only used where Azure resources require them, such as bot identity and integrations

## Key Vault and secrets

- Key Vault uses RBAC authorization
- Secrets are stored only when required
- Secrets are not embedded in Terraform files when the workload can use managed identity
- Private endpoint is enabled for the Key Vault

## Networking design

- Single VNet with multiple dedicated subnets
- Function Apps integrated into the application subnet
- Private endpoints on the private endpoint subnet
- Private DNS zones for Key Vault and Cosmos DB
- NSG and route table controls

## Monitoring

- Log Analytics workspace
- Application Insights attached to Function Apps
- Diagnostic logs are recommended for platform resources

## Azure Well-Architected Framework alignment

This design aligns with WAF:
- Security: private networking, RBAC, managed identities, Key Vault references
- Reliability: explicit dependencies, diagnostics, health-friendly app settings
- Performance: tuned SKUs and VNet integration
- Cost optimization: environment-based settings and conservative defaults
- Operational excellence: standard naming, tagging, modular structure, and clear outputs

## Assumptions and extension points

- Bot registration may be adjusted to match your Entra and Teams configuration
- If your tenant requires stricter RBAC or custom role definitions, extend roleassignments.tf
- If your Function Apps require additional app settings, add them to functions.tf
- If the Azure provider version in your environment differs, validate the resource schemas before deployment

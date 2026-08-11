# wpp-test-pnkj

## HCP Terraform Azure OIDC

Configure these HCP Terraform workspace variables with the `Environment variable`
category:

| Variable | Value |
| --- | --- |
| `TFC_AZURE_PROVIDER_AUTH` | `true` |
| `TFC_AZURE_RUN_CLIENT_ID` | Microsoft Entra application (client) ID |
| `ARM_SUBSCRIPTION_ID` | Azure subscription ID |
| `ARM_TENANT_ID` | Microsoft Entra tenant ID |

Do not define `ARM_CLIENT_ID`, `ARM_USE_OIDC`, `ARM_OIDC_TOKEN`, or an Azure
client secret. HCP Terraform generates the OIDC token and injects the AzureRM
provider authentication settings for each run.

The Entra application needs two federated identity credentials, one for each
HCP Terraform run phase:

```text
organization:<organization>:project:<project>:workspace:<workspace>:run_phase:plan
organization:<organization>:project:<project>:workspace:<workspace>:run_phase:apply
```

Use the HCP Terraform URL (for example, `https://app.terraform.io`) as the issuer
without a trailing slash. The default audience is `api://AzureADTokenExchange`.
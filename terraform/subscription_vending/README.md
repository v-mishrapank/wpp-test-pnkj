# Template Repository for use with TFCProject / Azure MG / Subscription creation

Clone this repository and amend values to vend subscriptions

Should be used with a TFC VCS driven workspace in accordance ith docs here:
https://confluence.uhub.biz/display/WPIGLOCLOUDHUB/Non-CAP+Subscription+creation+workflow

--------------cut here---------------

# Use to vend subscrions for <project/environment>

## Terraform Cloud Workspace:

https://...<update me>

## Instructions

### Varibles required in Workspace

Environment variables:

- TFC_AZURE_RUN_CLIENT_ID

*The App Registrarion Application (client) ID for the AppReg that has been delegated subscription vending role for this project and for this porjects Azure Management Group*

*The app registration should have federated identity credentials configured in the form:*

#### plan
- Federated credential scenario: other issuer
- Issuer: https://app.terraform.io
- subject identifier: organization:wpp-cloudhub:project:PROJECTNAME:workspace:WORKSPACENAME:run_phase:plan
- Name:tfc-IaC-ENVIRONMENT-plan

#### apply
-  Federated credential scenario: other issuer
- Issuer: https://app.terraform.io
- subject identifier: organization:wpp-cloudhub:project:PROJECTNAME:workspace:WORKSPACENAME:run_phase:apply
- Name:tfc-IaC-ENVIRONMENT-apply


*Provided and configured by Cloud Hub*

- TFC_AZURE_PROVIDER_AUTH

*Should be set to "true"

- ARM_SUBSCRIPTION_ID

*clz-wpp-it-cms-automation subscription (d8ff1f8e-1ba9-4c92-bdf2-012e121f56a0) is used to give the Azure RM provider a security context when connecting to azure. Required where the credentials used by the provider do not have access to any other subscriptions*
*Provided by Cloud Hub*

- ARM_TENANT_ID

*The Azure Tenant ID*
*Provided by Cloud Hub* (wpp.cloud is 3d8820e2-f4eb-46a2-8253-82539d7cc066)

### How to vend a subscrition

- Create a Branch for the change.
- Update terraform.auto.tfvars.
- Create a PR to merge you change to main, this will trigger a speculative plan in TFC.
- Assuming that the speculative plan is good, merge the PR and delete the branch.
- Apply the changes you merged to main.


The variable "sub_mg" is a key/value variable map of subscriptions names and parent management group IDs.
The Management group ID is unlikely to change.

### How to make role assignments

The "locals.tf" file is an example mapping User or Groups Entra object IDs to terraform variables that can use use for role assignment.

The "rbac.tf" file has examples for making both built in and custom role assignments

You will need to uncomment all lines and then adjust the code to be appropriate for the user or group principals and roles you wish to assign.

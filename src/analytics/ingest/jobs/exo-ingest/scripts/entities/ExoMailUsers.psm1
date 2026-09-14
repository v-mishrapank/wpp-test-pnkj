function Get-ModuleStages {
    @{
        'mail_users_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-ExoMailUsersRoot'
            ApiFamily  = 'exo'
        }
    }
}

function Get-ModuleEntities {
    @{
        'exo_mail_users' = @{
            # Get-EXORecipient returns whatever PropertySets/Properties ask
            # for; the module doesn't reshape the result. No field filter,
            # so SelectFields is omitted — a dev value here wouldn't be
            # honored by the fetch.
            Stage    = 'mail_users_root'
            WritesTo = 'root'
        }
    }
}

function Get-ExoMailUsersRoot {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory)][hashtable]$Context,
        [Parameter(Mandatory)]$Writer
    )

    $extraProps = @(
        'DisplayName','Alias','PrimarySmtpAddress','EmailAddresses',
        'ExternalEmailAddress','HiddenFromAddressListsEnabled',
        'WhenCreated','WhenChanged','FirstName','LastName',
        'City','Company','Department','Manager','Office','Title',
        'Notes','Identity','DistinguishedName'
    )

    Get-EXORecipient -RecipientTypeDetails MailUser,GuestMailUser -ResultSize Unlimited `
        -PropertySets Archive,Custom,MailboxMove,Policy `
        -Properties $extraProps -ErrorAction Stop | ForEach-Object {
        $Writer.WriteRecord($_)
    }
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-ExoMailUsersRoot

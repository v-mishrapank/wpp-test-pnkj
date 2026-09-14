function Get-ModuleStages {
    @{
        'contacts_root' = @{
            InputFrom  = $null
            RunsOnPool = $false
            Function   = 'Get-ExoContactsRoot'
            ApiFamily  = 'exo'
        }
    }
}

function Get-ModuleEntities {
    @{
        'exo_contacts' = @{
            Stage    = 'contacts_root'
            WritesTo = 'root'
        }
    }
}

function Get-ExoContactsRoot {
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

    Get-EXORecipient -RecipientTypeDetails MailContact -ResultSize Unlimited `
        -PropertySets Archive,Custom,MailboxMove,Policy `
        -Properties $extraProps -ErrorAction Stop | ForEach-Object {
        $Writer.WriteRecord($_)
    }
}

Export-ModuleMember -Function Get-ModuleStages, Get-ModuleEntities, Get-ExoContactsRoot

using System.ComponentModel.DataAnnotations;

namespace IngestDispatcher.Functions.Settings;

public class IngestSettings
{
    public const string SectionName = "Ingest";

    [Required]
    public required string KeyVaultName { get; set; }

    [Required]
    public required string SubscriptionId { get; set; }

    [Required]
    public required string ResourceGroupName { get; set; }

    public string ConfigPath { get; set; } = "Config";

    [Required]
    public required string IngestClientId { get; set; }

    [Required]
    public required string IngestCertName { get; set; }
}

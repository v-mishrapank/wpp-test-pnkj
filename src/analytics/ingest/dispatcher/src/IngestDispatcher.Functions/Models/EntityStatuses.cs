namespace IngestDispatcher.Functions.Models;

// Entity status vocabulary. The container writes the full union (pending
// until touched, running while in flight, terminal at end). The Cancelled /
// Timeout / CancellationFailed values are dispatcher-side only — synthesized
// at render time when reinterpreting an abandoned run-state blob.
public static class EntityStatuses
{
    public const string Pending = "pending";
    public const string Running = "running";
    public const string Success = "success";
    public const string Partial = "partial";
    public const string Skipped = "skipped";
    public const string Failed = "failed";

    public const string Cancelled = "cancelled";
    public const string TimedOut = "timed_out";
    public const string CancellationFailed = "cancellation_failed";
}

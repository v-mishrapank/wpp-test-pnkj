using IngestDispatcher.Functions.Services;

namespace IngestDispatcher.Functions.Tests;

// Pure-function tests for the status-code classifier used by
// AcaJobClient.CancelExecutionAsync. The interesting case is that ARM
// throttling (429) and request timeouts (408) must be transient — the
// rest of the dispatcher's resilience handler already treats them that
// way for GETs, and the cancel state machine is "caller retries on false"
// so they belong in the same bucket as 5xx.
public class AcaCancelClassificationTests
{
    [Theory]
    [InlineData(200)]
    [InlineData(202)]
    [InlineData(204)]
    public void TwoHundreds_Accepted(int status) =>
        Assert.Equal(AcaJobClient.StopResponseClass.Accepted,
            AcaJobClient.ClassifyStopResponse(status));

    [Fact]
    public void NotFound_AlreadyGone() =>
        Assert.Equal(AcaJobClient.StopResponseClass.AlreadyGone,
            AcaJobClient.ClassifyStopResponse(404));

    [Theory]
    [InlineData(408)] // request timeout
    [InlineData(429)] // throttle
    [InlineData(500)]
    [InlineData(502)]
    [InlineData(503)]
    [InlineData(504)]
    public void TransientCodes_Transient(int status) =>
        Assert.Equal(AcaJobClient.StopResponseClass.Transient,
            AcaJobClient.ClassifyStopResponse(status));

    [Theory]
    [InlineData(400)]
    [InlineData(401)]
    [InlineData(403)]
    [InlineData(409)]
    [InlineData(422)]
    public void HardFailures_Throw(int status) =>
        Assert.Equal(AcaJobClient.StopResponseClass.HardFailure,
            AcaJobClient.ClassifyStopResponse(status));
}

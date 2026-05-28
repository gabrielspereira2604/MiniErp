namespace Shared.Kernel;

public abstract record DomainEvent
{
    public Guid Id { get; } = Guid.NewGuid();
    public DateTime OccurredAt { get; } = DateTime.UtcNow;
    public string CorrelationId { get; init; } = string.Empty;
}

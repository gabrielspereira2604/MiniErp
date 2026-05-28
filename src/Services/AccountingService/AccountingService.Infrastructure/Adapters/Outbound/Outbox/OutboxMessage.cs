namespace AccountingService.Infrastructure.Adapters.Outbound.Outbox;

public class OutboxMessage
{
    public Guid Id { get; init; } = Guid.NewGuid();
    public string Topic { get; init; } = null!;
    public string Payload { get; init; } = null!;
    public string CorrelationId { get; init; } = string.Empty;
    public DateTime CreatedAt { get; init; } = DateTime.UtcNow;
    public DateTime? PublishedAt { get; set; }
}

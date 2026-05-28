namespace AccountingService.Domain.Ports;

public interface IEventPublisher
{
    Task PublishAsync(string topic, string payload, string correlationId, CancellationToken cancellationToken = default);
}

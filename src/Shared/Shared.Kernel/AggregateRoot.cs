namespace Shared.Kernel;

public abstract class AggregateRoot
{
    private readonly List<DomainEvent> _domainEvents = [];

    public Guid Id { get; protected set; }
    public IReadOnlyCollection<DomainEvent> DomainEvents => _domainEvents.AsReadOnly();

    protected void RaiseDomainEvent(DomainEvent domainEvent)
    {
        _domainEvents.Add(domainEvent);
    }

    public void ClearDomainEvents()
    {
        _domainEvents.Clear();
    }
}

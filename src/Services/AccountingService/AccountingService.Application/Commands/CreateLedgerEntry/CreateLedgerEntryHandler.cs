using AccountingService.Domain.Aggregates;
using AccountingService.Domain.Ports;
using AccountingService.Domain.ValueObjects;
using MediatR;

namespace AccountingService.Application.Commands.CreateLedgerEntry;

public class CreateLedgerEntryHandler : IRequestHandler<CreateLedgerEntryCommand, CreateLedgerEntryResult>
{
    private readonly ILedgerEntryRepository _repository;

    public CreateLedgerEntryHandler(ILedgerEntryRepository repository)
    {
        _repository = repository;
    }

    public async Task<CreateLedgerEntryResult> Handle(CreateLedgerEntryCommand command, CancellationToken cancellationToken)
    {
        // idempotência — se já existe um lançamento para esse receivable, retorna
        var existing = await _repository.GetByReceivableIdAsync(command.ReceivableId, cancellationToken);
        if (existing is not null)
            return new CreateLedgerEntryResult(existing.Id);

        var money = new Money(command.Amount, command.Currency);
        var entry = LedgerEntry.Create(Guid.NewGuid(), command.ReceivableId, money);

        await _repository.SaveAsync(entry, cancellationToken);

        return new CreateLedgerEntryResult(entry.Id);
    }
}

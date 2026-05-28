using AccountingService.Domain.Ports;
using MediatR;
using Shared.Kernel;

namespace AccountingService.Application.Commands.ReverseLedgerEntry;

public class ReverseLedgerEntryHandler : IRequestHandler<ReverseLedgerEntryCommand>
{
    private readonly ILedgerEntryRepository _repository;

    public ReverseLedgerEntryHandler(ILedgerEntryRepository repository)
    {
        _repository = repository;
    }

    public async Task Handle(ReverseLedgerEntryCommand command, CancellationToken cancellationToken)
    {
        var entry = await _repository.GetByReceivableIdAsync(command.ReceivableId, cancellationToken)
            ?? throw new DomainException($"LedgerEntry não encontrado para receivable {command.ReceivableId}");

        entry.Reverse(command.Reason);

        await _repository.SaveAsync(entry, cancellationToken);
    }
}

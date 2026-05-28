using MediatR;

namespace AccountingService.Application.Commands.ReverseLedgerEntry;

public record ReverseLedgerEntryCommand(
    Guid ReceivableId,
    string Reason,
    string CorrelationId
) : IRequest;

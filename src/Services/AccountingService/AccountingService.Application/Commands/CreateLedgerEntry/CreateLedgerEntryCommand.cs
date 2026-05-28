using MediatR;

namespace AccountingService.Application.Commands.CreateLedgerEntry;

public record CreateLedgerEntryCommand(
    Guid ReceivableId,
    decimal Amount,
    string Currency,
    string CorrelationId
) : IRequest<CreateLedgerEntryResult>;

public record CreateLedgerEntryResult(Guid LedgerEntryId);

using MediatR;

namespace FinancialService.Application.Commands.CreateReceivable;

public record CreateReceivableCommand(
    Guid InvoiceId,
    decimal Amount,
    string Currency,
    string CorrelationId
) : IRequest<CreateReceivableResult>;

public record CreateReceivableResult(Guid ReceivableId);

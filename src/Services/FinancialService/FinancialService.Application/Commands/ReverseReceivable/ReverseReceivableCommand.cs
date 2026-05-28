using MediatR;

namespace FinancialService.Application.Commands.ReverseReceivable;

public record ReverseReceivableCommand(
    Guid InvoiceId,
    string Reason,
    string CorrelationId
) : IRequest;

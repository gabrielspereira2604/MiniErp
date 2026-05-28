using MediatR;

namespace InvoiceService.Application.Commands.CreateInvoice;

public record CreateInvoiceCommand(
    Guid IdempotencyKey,
    decimal Amount,
    string Currency,
    string CustomerDocument
) : IRequest<CreateInvoiceResult>;

public record CreateInvoiceResult(Guid InvoiceId);

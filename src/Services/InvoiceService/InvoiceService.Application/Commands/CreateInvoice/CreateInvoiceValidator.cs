using FluentValidation;

namespace InvoiceService.Application.Commands.CreateInvoice;

public class CreateInvoiceValidator : AbstractValidator<CreateInvoiceCommand>
{
    public CreateInvoiceValidator()
    {
        RuleFor(x => x.Amount)
            .GreaterThan(0).WithMessage("Valor deve ser maior que zero");

        RuleFor(x => x.Currency)
            .NotEmpty().WithMessage("Moeda é obrigatória")
            .Length(3).WithMessage("Moeda deve ter 3 caracteres");

        RuleFor(x => x.CustomerDocument)
            .NotEmpty().WithMessage("Documento do cliente é obrigatório");

        RuleFor(x => x.IdempotencyKey)
            .NotEmpty().WithMessage("Chave de idempotência é obrigatória");
    }
}

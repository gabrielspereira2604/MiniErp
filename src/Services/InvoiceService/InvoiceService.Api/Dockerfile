FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS base
WORKDIR /app
EXPOSE 8080

FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY src/Shared/Shared.Kernel/Shared.Kernel.csproj src/Shared/Shared.Kernel/
COPY src/Services/InvoiceService/InvoiceService.Domain/InvoiceService.Domain.csproj src/Services/InvoiceService/InvoiceService.Domain/
COPY src/Services/InvoiceService/InvoiceService.Application/InvoiceService.Application.csproj src/Services/InvoiceService/InvoiceService.Application/
COPY src/Services/InvoiceService/InvoiceService.Infrastructure/InvoiceService.Infrastructure.csproj src/Services/InvoiceService/InvoiceService.Infrastructure/
COPY src/Services/InvoiceService/InvoiceService.Api/InvoiceService.Api.csproj src/Services/InvoiceService/InvoiceService.Api/

RUN dotnet restore src/Services/InvoiceService/InvoiceService.Api/InvoiceService.Api.csproj

COPY . .

RUN dotnet publish src/Services/InvoiceService/InvoiceService.Api/InvoiceService.Api.csproj \
    -c Release -o /app/publish --no-restore

FROM base AS final
WORKDIR /app
COPY --from=build /app/publish .
ENTRYPOINT ["dotnet", "InvoiceService.Api.dll"]

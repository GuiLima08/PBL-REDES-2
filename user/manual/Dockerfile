# Estágio de Compilação
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY user.go .
RUN go mod init user && go build -o user user.go

# Estágio de Produção
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/user .

ENTRYPOINT ["./user"]
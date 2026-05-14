# Estágio de Compilação
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY broker.go .
# Inicia um módulo go genérico e compila o binário
RUN go mod init broker && go build -o broker broker.go

# Estágio de Produção
FROM alpine:latest
WORKDIR /app
# Copia apenas o binário compilado do estágio anterior
COPY --from=builder /app/broker .

# Usa ENTRYPOINT para permitir a passagem de argumentos (portas e IPs) via 'docker run'
ENTRYPOINT ["./broker"]
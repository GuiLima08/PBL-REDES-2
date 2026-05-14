# Estágio de Compilação
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY sensor.go .
RUN go mod init sensor && go build -o sensor sensor.go

# Estágio de Produção
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/sensor .

ENTRYPOINT ["./sensor"]
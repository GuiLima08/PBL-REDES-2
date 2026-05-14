# Estágio de Compilação
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY raceTest.go .
RUN go mod init racetest && go build -o racetest raceTest.go

# Estágio de Produção
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/racetest .

ENTRYPOINT ["./racetest"]
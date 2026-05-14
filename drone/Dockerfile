# Estágio de Compilação
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY drone.go .
RUN go mod init drone && go build -o drone drone.go

# Estágio de Produção
FROM alpine:latest
WORKDIR /app
COPY --from=builder /app/drone .

ENTRYPOINT ["./drone"]
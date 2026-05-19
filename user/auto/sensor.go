package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

// Função de reconexão cíclica
func connectWithRetry(serverIP, serverPort string) net.Conn {
	for {
		log.Printf("Tentando conectar ao servidor em %s:%s...\n", serverIP, serverPort)
		conn, err := net.Dial("tcp", serverIP+":"+serverPort)
		if err == nil {
			log.Println("Conexão estabelecida com sucesso!")
			return conn
		}
		log.Printf("-!- Falha ao conectar: %v. Tentando novamente em 5 segundos...\n", err)
		time.Sleep(5 * time.Second)
	}
}

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Uso: go run sensor.go <server_ip> <port>")
	}
	serverIP := os.Args[1]
	serverPort := os.Args[2]

	// Inicia a conexão
	tcpConn := connectWithRetry(serverIP, serverPort)
	defer func() {
		if tcpConn != nil {
			tcpConn.Close()
		}
	}()

	fmt.Println("Enviando processos automaticamente...")

	for {
		// Gera o tempo antes para poder enviar de 5 em 5 segundos
		time.Sleep(5 * time.Second)

		priority := rand.Intn(10) + 1
		duration := rand.Intn(14) + 2

		texto := fmt.Sprintf("%d,%d", priority, duration)
		mensagem := fmt.Sprintf("%s\n", "P/"+texto)

		// Loop de envio seguro
		for {
			_, err := tcpConn.Write([]byte(mensagem))
			if err != nil {
				log.Printf("-!- Conexão perdida ao enviar: %v", err)
				tcpConn.Close()
				
				// Reconecta
				tcpConn = connectWithRetry(serverIP, serverPort)
				
				// Após conectar, tenta dar o Write da mensagem pendente novamente
				continue
			}
			break
		}

		fmt.Printf("Processo automático [Prioridade: %d | Tempo: %ds] enviado ao servidor.\n", priority, duration)
	}
}
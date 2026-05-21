package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

// Tenta estabelecer uma conexão TCP com o servidor localizado
// em serverIP. Em caso de falha, a função bloqueia a execução e realiza
// tentativas infinitas de reconexão a cada 5 segundos até obter sucesso,
// retornando a conexão ativa e pronta para envio de dados.
func connectWithRetry(serverIP string) net.Conn {
	for {
		log.Printf("Tentando conectar ao servidor em %s...\n", serverIP)
		conn, err := net.Dial("tcp", serverIP)
		if err == nil {
			log.Println("Conexão estabelecida com sucesso!")
			return conn
		}
		log.Printf("-!- Falha ao conectar: %v. Tentando novamente em 5 segundos...\n", err)
		time.Sleep(5 * time.Second)
	}
}

// Inicializa o sensor, estabelece a conexão primária com o servidor
// e entra em um loop infinito de geração de processos. O envio é protegido
// por um mecanismo de retenção que garante que, se a rede cair, o processo
// gerado não seja perdido, sendo enviado assim que a conexão for reestabelecida.
func main() {
	if len(os.Args) != 2 {
		log.Fatal("Uso: go run sensor.go <server_ip:port>")
	}
	serverIP := os.Args[1]

	tcpConn := connectWithRetry(serverIP)
	defer func() {
		if tcpConn != nil {
			tcpConn.Close()
		}
	}()

	fmt.Println("Enviando processos automaticamente...")

	for {
		time.Sleep(5 * time.Second)

		priority := rand.Intn(10) + 1
		duration := rand.Intn(14) + 2

		texto := fmt.Sprintf("%d,%d", priority, duration)
		mensagem := fmt.Sprintf("%s\n", "P/"+texto)

		for {
			_, err := tcpConn.Write([]byte(mensagem))
			if err != nil {
				log.Printf("-!- Conexão perdida ao enviar: %v", err)
				tcpConn.Close()
				
				tcpConn = connectWithRetry(serverIP)
				
				continue
			}
			break
		}

		fmt.Printf("Processo automático [Prioridade: %d | Tempo: %ds] enviado ao servidor.\n", priority, duration)
	}
}
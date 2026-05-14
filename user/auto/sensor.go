package main

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Uso: go run sensor.go <server_ip> <port>")
	}
	serverIP := os.Args[1]
	serverPort := os.Args[2]

	log.Printf("Conectando ao servidor em %s:%s...\n", serverIP, serverPort)
	tcpConn, err := net.Dial("tcp", serverIP+":"+serverPort)
	if err != nil {
		log.Fatalf("-!- Erro ao conectar ao servidor: %v", err)
	}
	defer tcpConn.Close()
	log.Println("Conexão estabelecida com o servidor.")

	fmt.Println("Enviando processos...")

	// Loop infinito lendo as entradas do usuário
	for {
		time.Sleep(5 * time.Second)
		
		priority := rand.Intn(10) + 1
		time := rand.Intn(14) + 2

		texto := fmt.Sprintf("%d,%d",priority, time)

		// --- 3. Envio ao Servidor ---
		
		// Se passou por todas as validações, enviamos a string original pela rede.
		// Adicionamos o "\n" no final para que o bufio.Scanner lá no Broker consiga ler a linha!
		mensagem := fmt.Sprintf("%s\n", "P/"+texto)
		
		_, err = tcpConn.Write([]byte(mensagem))
		if err != nil {
			log.Printf("-!- Erro fatal de rede ao enviar dados: %v\n", err)
			break
		}

		fmt.Printf("Processo [Prioridade: %d | Tempo: %ds] enviado ao servidor.\n", priority, time)
	}
}
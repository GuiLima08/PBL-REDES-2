package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Função que fica tentando conectar infinitamente a cada 5 segundos
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

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Uso: go run user.go <server_ip:port>")
	}
	serverIP := os.Args[1]

	// Usa a nova função resiliente
	tcpConn := connectWithRetry(serverIP)
	defer func() {
		if tcpConn != nil {
			tcpConn.Close()
		}
	}()

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("\n--------------------------------------------------")
	fmt.Println("MÓDULO DE ENVIO DE PROCESSOS")
	fmt.Println("Formato exigido: <prioridade>,<tempo>")
	fmt.Println("Regras: Prioridade (1 a 10) | Tempo (>= 1 segundo)")
	fmt.Println("Exemplo: 5,10")
	fmt.Println("Digite 'sair' para encerrar a aplicação.")
	fmt.Println("--------------------------------------------------")

	for {
		fmt.Print("\n> ")

		if !scanner.Scan() {
			break
		}

		texto := strings.TrimSpace(scanner.Text())

		if strings.ToLower(texto) == "sair" || strings.ToLower(texto) == "exit" {
			fmt.Println("Desconectando...")
			break
		}

		partes := strings.Split(texto, ",")
		if len(partes) != 2 {
			fmt.Println("Formato inválido. Use a vírgula para separar a prioridade do tempo. Ex: 5,10")
			continue
		}

		prioridade, errPri := strconv.Atoi(partes[0])
		tempo, errTem := strconv.Atoi(partes[1])

		if errPri != nil || errTem != nil {
			fmt.Println("A prioridade e o tempo devem ser números inteiros válidos.")
			continue
		}
		if prioridade < 1 || prioridade > 10 {
			fmt.Println("A prioridade deve estar entre 1 e 10.")
			continue
		}
		if tempo < 1 {
			fmt.Println("O tempo de execução deve ser maior ou igual a 1 segundo.")
			continue
		}

		mensagem := fmt.Sprintf("%s\n", "P/"+texto)

		// Loop de envio seguro: se a conexão cair, ele reconecta e envia o mesmo processo
		for {
			_, err := tcpConn.Write([]byte(mensagem))
			if err != nil {
				log.Printf("\n-!- Conexão perdida ao enviar: %v", err)
				tcpConn.Close() // Fecha o socket quebrado
				
				// Trava a execução e tenta reconectar
				tcpConn = connectWithRetry(serverIP)
				
				// Quando reconectar, o loop 'for' vai rodar novamente tentando dar o Write
				continue
			}
			// Se o Write funcionou, sai do loop de reenvio
			break
		}

		fmt.Printf("Processo [Prioridade: %d | Tempo: %ds] enviado ao servidor.\n", prioridade, tempo)
	}
}
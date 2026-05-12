package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
)

func main() {
	if len(os.Args) != 3 {
		log.Fatal("Uso: go run user.go <server_ip> <port>")
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

	// Scanner para ler o que o usuário digita no terminal
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("\n--------------------------------------------------")
	fmt.Println("MÓDULO DE ENVIO DE PROCESSOS")
	fmt.Println("Formato exigido: <prioridade>,<tempo>")
	fmt.Println("Regras: Prioridade (1 a 10) | Tempo (>= 1 segundo)")
	fmt.Println("Exemplo: 5,10")
	fmt.Println("Digite 'sair' para encerrar a conexão.")
	fmt.Println("--------------------------------------------------")

	// Loop infinito lendo as entradas do usuário
	for {
		fmt.Print("\n> ")
		
		// Espera o usuário digitar e apertar Enter
		if !scanner.Scan() {
			break 
		}
		
		// Pega o texto e remove espaços vazios nas pontas (caso o usuário digite com espaço sem querer)
		texto := strings.TrimSpace(scanner.Text())

		// Opção para fechar o programa amigavelmente
		if strings.ToLower(texto) == "sair" || strings.ToLower(texto) == "exit" {
			fmt.Println("Desconectando...")
			break
		}
		
		// Divide a string na vírgula. partes[0] será a prioridade, partes[1] será o tempo.
		partes := strings.Split(texto, ",")

		if len(partes) != 2 {
			fmt.Println("Formato inválido. Use a vírgula para separar a prioridade do tempo. Ex: P/5,10")
			continue
		}

		// --- 2. Validação de Regras de Negócio ---
		
		// Converte os textos para números inteiros
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

		// --- 3. Envio ao Servidor ---
		
		// Se passou por todas as validações, enviamos a string original pela rede.
		// Adicionamos o "\n" no final para que o bufio.Scanner lá no Broker consiga ler a linha!
		mensagem := fmt.Sprintf("%s\n", "P/"+texto)
		
		_, err = tcpConn.Write([]byte(mensagem))
		if err != nil {
			log.Printf("-!- Erro fatal de rede ao enviar dados: %v\n", err)
			break
		}

		fmt.Printf("✔️ Processo [Prioridade: %d | Tempo: %ds] enviado ao servidor!\n", prioridade, tempo)
	}
}
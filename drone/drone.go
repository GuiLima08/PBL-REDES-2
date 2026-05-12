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

// Process representa a tarefa. client modificado para string (ex: IP do cliente)
type Process struct {
	client   string
	id       string
	priority int
	timeLeft int
}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Uso: drone <broker1_ip:port> <broker2_ip:port> ... <brokerN_ip:port>")
	}
	
	alvos := os.Args[1:]

	// Loop infinito que mantem o drone vivo e permite redirecionamentos limpos
	for {
		novoAlvo := executarSessaoDrone(alvos)
		if novoAlvo != "" {
			// Atualiza a lista de alvos para apenas o novo broker
			alvos = []string{novoAlvo}
		} else {
			// Se retornar vazio, houve um erro crítico e ele tenta reconectar na lista atual
			time.Sleep(2 * time.Second)
		}
	}
}

func executarSessaoDrone(brokers []string) string {
	conn := connectToBroker(brokers, 0)
	defer conn.Close()

	processChan := make(chan Process)
	redirectChan := make(chan string) // Canal para avisar o loop principal sobre o redirecionamento

	go func() {
		reader := bufio.NewReader(conn)
		for {
			msg, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Conexao com broker perdida.")
				close(processChan) // Libera o select principal
				return
			}

			msg = strings.TrimSpace(msg)
			if msg == "" {
				continue
			}

			if strings.HasPrefix(msg, "REDIRECT/") {
				partes := strings.Split(msg, "/")
				if len(partes) == 2 {
					log.Printf("--- COMANDO RECEBIDO: Redirecionando para %s ---", partes[1])
					redirectChan <- partes[1] // Envia o novo endereço pro loop principal
					return
				}
			}

			p, err := parseProcess(msg)
			if err != nil {
				continue
			}
			processChan <- p
		}
	}()

	var currentProcess *Process
	ticker := time.NewTicker(time.Hour) // Ticker inerte inicial
	ticker.Stop()

	for {
		if currentProcess == nil {
			select {
			case novoProcesso, ok := <-processChan:
				if !ok { return "" } // Conexão caiu
				currentProcess = &novoProcesso
				log.Printf("Iniciando processo [%s] do cliente %s (%ds restantes)\n", currentProcess.id, currentProcess.client, currentProcess.timeLeft)
				ticker.Reset(1 * time.Second)
			case novoBroker := <-redirectChan:
				ticker.Stop()
				return novoBroker // Sai da função limpando tudo com o defer, e retorna o novo endereço
			}
		} else {
			select {
			case novoProcesso, ok := <-processChan:
				if !ok { return "" }
				log.Printf("--- INTERRUPCAO --- Processo [%s] parado. Restam %ds\n", currentProcess.id, currentProcess.timeLeft)
				ticker.Stop()
				sendProcess(conn, currentProcess)
				currentProcess = &novoProcesso
				log.Printf("Iniciando NOVO processo [%s] (%ds restantes)\n", currentProcess.id, currentProcess.timeLeft)
				ticker.Reset(1 * time.Second)

			case novoBroker := <-redirectChan:
				// Interrompido por um REDIRECT (raro, mas possível de acontecer num race condition)
				ticker.Stop()
				sendProcess(conn, currentProcess) // Devolve antes de ir embora
				return novoBroker

			case <-ticker.C:
				currentProcess.timeLeft--
				log.Printf("Executando [%s]... tempo restante: %ds\n", currentProcess.id, currentProcess.timeLeft)
				if currentProcess.timeLeft <= 0 {
					log.Printf("Processo [%s] concluido!\n", currentProcess.id)
					ticker.Stop()
					sendProcess(conn, currentProcess)
					currentProcess = nil
				}
			}
		}
	}
}

// Converte a string recebida de volta para a estrutura Process (INTACTO)
func parseProcess(msg string) (Process, error) {
	parts := strings.Split(msg, ",")
	if len(parts) != 4 {
		return Process{}, fmt.Errorf("formato incorreto (esperado 4 blocos)")
	}

	priority, err := strconv.Atoi(parts[2])
	if err != nil {
		return Process{}, err
	}

	timeLeft, err := strconv.Atoi(parts[3])
	if err != nil {
		return Process{}, err
	}

	return Process{
		client:   parts[0],
		id:       parts[1],
		priority: priority,
		timeLeft: timeLeft,
	}, nil
}

// Converte a estrutura Process para string e envia na rede (INTACTO)
func sendProcess(conn net.Conn, p *Process) {
	// Formata usando a quebra de linha obrigatoria no final
	msg := fmt.Sprintf("%s,%s,%d,%d\n", p.client, p.id, p.priority, p.timeLeft)

	_, err := conn.Write([]byte(msg))
	if err != nil {
		log.Printf("Erro critico ao devolver processo pro broker: %v\n", err)
	}
}

// Lógica de reconexão cíclica com tolerância a falhas (INTACTO)
func connectToBroker(brokers []string, startIndex int) net.Conn {
	brokerIndex := startIndex
	for {
		brokerAtual := brokers[brokerIndex]
		log.Printf("Tentando conectar ao broker em %s...\n", brokerAtual)

		for tentativa := 1; tentativa <= 5; tentativa++ {
			conn, err := net.Dial("tcp", brokerAtual)
			if err == nil {
				log.Printf("Conectado com sucesso ao broker em %s\n", brokerAtual)
				return conn
			}

			log.Printf("Falha ao conectar em %s: %v", brokerAtual, err)
			if tentativa < 5 {
				log.Printf("Tentando novamente em 5 segundos... (tentativa %d/5)\n", tentativa)
				time.Sleep(5 * time.Second)
			}
		}

		log.Printf("Esgotadas as tentativas para %s. Trocando de broker...\n", brokerAtual)
		brokerIndex = (brokerIndex + 1) % len(brokers)
	}
}
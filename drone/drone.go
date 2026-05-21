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

// Process representa uma tarefa atribuída a este drone pelo broker.
type Process struct {
	client   string // ID do cliente
	id       string // ID do processo
	priority int    // Prioridade do processo (1 a 10)
	timeLeft int    // Tempo restante para o processo ser concluído (segundos)
}

// Inicializa a lista de alvos com o broker primário fornecido via linha
// de comando e gerencia o ciclo de vida global, reiniciando a sessão
// de execução de forma resiliente em caso de falhas ou redirecionamentos.
func main() {
	if len(os.Args) != 2 {
		log.Fatal("Uso: drone <broker_ip:port>")
	}

	alvos := []string{os.Args[1]}

	for {
		novoAlvo := executarSessaoDrone(&alvos)
		if novoAlvo != "" {
			alvos = []string{novoAlvo}
		} else {
			time.Sleep(2 * time.Second)
		}
	}
}

// Estabelece a conexão com um broker e gerencia o loop principal de
// processamento. Ele interpreta comandos de handshake para aprendizado de vizinhos,
// redirecionamentos e simula a execução de processos utilizando um temporizador.
//
// Recebe um ponteiro para a lista de brokers, permitindo a atualização dinâmica
// dos vizinhos conhecidos na memória do escopo superior. Retorna o endereço do novo
// broker em caso de redirecionamento, ou uma string vazia em caso de falha de conexão.
func executarSessaoDrone(brokers *[]string) string {
	conn := connectToBroker(*brokers, 0)
	defer conn.Close()

	processChan := make(chan Process)
	redirectChan := make(chan string)

	go func() {
		reader := bufio.NewReader(conn)
		for {
			msg, err := reader.ReadString('\n')
			if err != nil {
				log.Printf("Conexao com broker perdida.")
				close(processChan)
				return
			}

			msg = strings.TrimSpace(msg)
			if msg == "" {
				continue
			}

			if strings.HasPrefix(msg, "NEIGHBORS/") {
				partes := strings.Split(msg, "/")
				if len(partes) >= 2 && partes[1] != "" {
					vizinhosRecebidos := strings.Split(strings.TrimSpace(partes[1]), ",")

					brokerAtual := conn.RemoteAddr().String()

					mapaUnicos := make(map[string]bool)
					novaLista := []string{brokerAtual}
					mapaUnicos[brokerAtual] = true

					for _, vizinho := range vizinhosRecebidos {
						vizinho = strings.TrimSpace(vizinho)
						if vizinho != "" && !mapaUnicos[vizinho] {
							novaLista = append(novaLista, vizinho)
							mapaUnicos[vizinho] = true
						}
					}

					*brokers = novaLista
					log.Printf("--- HANDSHAKE --- Dicionário atualizado. Vizinhos conhecidos: %v", *brokers)
				}
				continue
			}

			if strings.HasPrefix(msg, "REDIRECT/") {
				partes := strings.Split(msg, "/")
				if len(partes) == 2 {
					log.Printf("--- COMANDO RECEBIDO: Redirecionando para %s ---", partes[1])
					redirectChan <- partes[1]
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
	ticker := time.NewTicker(time.Hour)
	ticker.Stop()

	for {
		if currentProcess == nil {
			select {
			case novoProcesso, ok := <-processChan:
				if !ok {
					return ""
				}
				currentProcess = &novoProcesso
				log.Printf("Iniciando processo [%s] do cliente %s (%ds restantes)\n", currentProcess.id, currentProcess.client, currentProcess.timeLeft)
				ticker.Reset(1 * time.Second)
			case novoBroker := <-redirectChan:
				ticker.Stop()
				return novoBroker
			}
		} else {
			select {
			case novoProcesso, ok := <-processChan:
				if !ok {
					return ""
				}
				log.Printf("--- INTERRUPCAO --- Processo [%s] parado. Restam %ds\n", currentProcess.id, currentProcess.timeLeft)
				ticker.Stop()
				sendProcess(conn, currentProcess)
				currentProcess = &novoProcesso
				log.Printf("Iniciando NOVO processo [%s] (%ds restantes)\n", currentProcess.id, currentProcess.timeLeft)
				ticker.Reset(1 * time.Second)

			case novoBroker := <-redirectChan:
				ticker.Stop()
				sendProcess(conn, currentProcess)
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


// Converte a string msg formatada recebida da rede em uma estrutura Process.
// Retorna um erro caso a string não contenha os quatro blocos esperados separados
// por vírgula ou caso ocorra falha na conversão dos valores numéricos de prioridade e tempo.
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
	return Process{client: parts[0], id: parts[1], priority: priority, timeLeft: timeLeft}, nil
}


// Formata o estado atual do ponteiro de processo "p" e o transmite de
// volta ao broker através da conexão TCP conn. Utilizado tanto para reportar a
// conclusão de uma tarefa quanto para devolver um processo interrompido.
func sendProcess(conn net.Conn, p *Process) {
	msg := fmt.Sprintf("%s,%s,%d,%d\n", p.client, p.id, p.priority, p.timeLeft)
	_, err := conn.Write([]byte(msg))
	if err != nil {
		log.Printf("Erro critico ao devolver processo pro broker: %v\n", err)
	}
}


// Tenta estabelecer uma conexão TCP de forma resiliente iterando
// ciclicamente sobre a lista de brokers conhecidos a partir do índice startIndex.
// Retorna a conexão net.Conn ativa e pronta para uso após o sucesso, implementando
// tentativas múltiplas com espaçamento temporal em caso de recusa.
func connectToBroker(brokers []string, startIndex int) net.Conn {
	brokerIndex := startIndex
	for {
		if len(brokers) == 0 {
			log.Println("Nenhum broker na lista. Aguardando...")
			time.Sleep(5 * time.Second)
			continue
		}

		brokerAtual := brokers[brokerIndex%len(brokers)]
		log.Printf("Tentando conectar ao broker em %s...\n", brokerAtual)

		for tentativa := 1; tentativa <= 3; tentativa++ {
			conn, err := net.Dial("tcp", brokerAtual)
			if err == nil {
				log.Printf("Conectado com sucesso ao broker em %s\n", brokerAtual)
				return conn
			}

			log.Printf("Falha ao conectar em %s: %v", brokerAtual, err)
			if tentativa < 3 {
				log.Printf("Tentando novamente em 5 segundos... (tentativa %d/3)\n", tentativa)
				time.Sleep(5 * time.Second)
			}
		}

		log.Printf("Esgotadas as tentativas para %s. Trocando de broker...\n", brokerAtual)
		brokerIndex = (brokerIndex + 1) % len(brokers)
	}
}
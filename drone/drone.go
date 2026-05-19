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

type Process struct {
	client   string
	id       string
	priority int
	timeLeft int
}

func main() {
	if len(os.Args) != 2 {
		log.Fatal("Uso: drone <broker_ip:port>")
	}

	alvos := []string{os.Args[1]}

	for {
		// Passa o endereço da lista para que a sessão possa atualizá-la
		novoAlvo := executarSessaoDrone(&alvos) 
		if novoAlvo != "" {
			alvos = []string{novoAlvo} // Redirecionamento direto substitui a lista
		} else {
			time.Sleep(2 * time.Second)
		}
	}
}

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

			// --- INTERPRETAÇÃO DO HANDSHAKE DE VIZINHOS ---
			if strings.HasPrefix(msg, "NEIGHBORS/") {
				partes := strings.Split(msg, "/")
				if len(partes) >= 2 && partes[1] != "" {
					vizinhosRecebidos := strings.Split(strings.TrimSpace(partes[1]), ",")
					
					brokerAtual := conn.RemoteAddr().String()
					
					// Usamos um mapa para evitar brokers duplicados na lista
					mapaUnicos := make(map[string]bool)
					novaLista := []string{brokerAtual} // O broker atual (inicial) é sempre o primeiro
					mapaUnicos[brokerAtual] = true

					for _, vizinho := range vizinhosRecebidos {
						vizinho = strings.TrimSpace(vizinho)
						if vizinho != "" && !mapaUnicos[vizinho] {
							novaLista = append(novaLista, vizinho)
							mapaUnicos[vizinho] = true
						}
					}

					*brokers = novaLista // Atualiza a lista na memória da função main!
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
				if !ok { return "" }
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
				if !ok { return "" }
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

func sendProcess(conn net.Conn, p *Process) {
	msg := fmt.Sprintf("%s,%s,%d,%d\n", p.client, p.id, p.priority, p.timeLeft)
	_, err := conn.Write([]byte(msg))
	if err != nil {
		log.Printf("Erro critico ao devolver processo pro broker: %v\n", err)
	}
}

func connectToBroker(brokers []string, startIndex int) net.Conn {
	brokerIndex := startIndex
	for {
		// Proteção caso a lista esteja vazia
		if len(brokers) == 0 {
			log.Println("Nenhum broker na lista. Aguardando...")
			time.Sleep(5 * time.Second)
			continue
		}

		brokerAtual := brokers[brokerIndex % len(brokers)] // Evita panics de index
		log.Printf("Tentando conectar ao broker em %s...\n", brokerAtual)

		for tentativa := 1; tentativa <= 3; tentativa++ {
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
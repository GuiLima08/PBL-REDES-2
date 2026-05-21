// Package main implementa o servidor Broker central para o sistema distribuído.
// Ele gerencia uma fila de processos com prioridade, orquestra a conexão com
// clientes, drones e outros brokers vizinhos, realizando o balanceamento de carga e
// o empréstimo dinâmico de trabalhadores (drones).
package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	clientes = make(map[net.Conn]int)
	clientMU = sync.RWMutex{}
	queueMU  = sync.RWMutex{}

	drones  = make(map[net.Conn]*Process)
	droneMU = sync.RWMutex{}

	Queue = make(PriorityQueue, 0)

	adjacentBrokers = make(map[string]net.Conn)
	knownDroneAddrs = make(map[string]string)
	adjMU           = sync.RWMutex{}

	portaDrones  string
	portaBrokers string
)

// Process representa uma tarefa individual submetida por um cliente para ser executada.
type Process struct {
	Client   string
	ID       string
	Priority int
	TimeLeft int
}

// PriorityQueue implementa o pacote container/heap para gerenciar a fila de processos.
// A ordenação garante que o processo com maior prioridade e, em caso de empate,
// menor tempo de execução, seja sempre o primeiro a ser atendido.
type PriorityQueue []Process

// Less compara dois elementos da fila e estabelece a regra de ordenação:
// Compara o atributo prioridade primeiro, e desempate é decidido pelo
// processo mais rápido.
func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	return pq[i].TimeLeft < pq[j].TimeLeft
}

func (pq PriorityQueue) Len() int { return len(pq) }
func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }
func (pq *PriorityQueue) Push(x any) { *pq = append(*pq, x.(Process)) }
func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

// main inicializa as configurações globais do Broker, sobe os servidores TCP em
// goroutines separadas e executa um loop de monitoramento de status do sistema.
func main() {
	if len(os.Args) < 4 {
		log.Fatalf("ERR: Uso incorreto do comando\nUso correto: go run server.go <p2p_port> <client_port> <drone_port> <broker1_ip:port> ...")
	}

	brokerIPs := os.Args[4:]
	portaBrokers = ":" + os.Args[1]
	portaClientes := ":" + os.Args[2]
	portaDrones = ":" + os.Args[3]

	heap.Init(&Queue)

	for _, brokerIP := range brokerIPs {
		go dialBroker(brokerIP)
	}

	go iniciarServidorTCP("Broker-to-Broker", portaBrokers, handleBroker)
	go iniciarServidorTCP("Cliente", portaClientes, handleClient)
	go iniciarServidorTCP("Drone", portaDrones, handleDrone)

	go monitorAndAskForDrones(brokerIPs)

	log.Println("Broker iniciado com sucesso.")
	log.Printf("Escutando Brokers na porta %s...\n", portaBrokers)
	log.Printf("Escutando Clientes na porta %s...\n", portaClientes)
	log.Printf("Escutando Drones na porta %s...\n", portaDrones)

	for {
		time.Sleep(5 * time.Second)
		queueMU.RLock()
		droneMU.RLock()

		log.Printf("\n--- Status Atual ---")
		log.Printf("Fila de Processos (%d):", len(Queue))

		filaExibicao := make([]Process, len(Queue))
		copy(filaExibicao, Queue)

		sort.Slice(filaExibicao, func(i, j int) bool {
			if filaExibicao[i].Priority != filaExibicao[j].Priority {
				return filaExibicao[i].Priority > filaExibicao[j].Priority
			}
			return filaExibicao[i].TimeLeft < filaExibicao[j].TimeLeft
		})

		for _, process := range filaExibicao {
			log.Printf("  - %+v", process)
		}

		log.Printf("--------------------")
		log.Printf("Drones Conectados (%d):", len(drones))
		for conn, process := range drones {
			if process == nil {
				log.Printf("  - Drone %s: LIVRE", conn.RemoteAddr().String())
			} else {
				log.Printf("  - Drone %s: Executando %s", conn.RemoteAddr().String(), process.ID)
			}
		}

		log.Printf("--------------------")
		log.Printf("Sensores Conectados (%d):", len(clientes))
		for conn := range clientes {
			log.Printf("  - Sensor %s", conn.RemoteAddr().String())
		}

		log.Printf("--------------------")
		log.Printf("Brokers vizinhos (%d):", len(adjacentBrokers))
		for addr := range adjacentBrokers {
			log.Printf("  - Broker %s", addr)
		}

		droneMU.RUnlock()
		queueMU.RUnlock()
	}
}

// iniciarServidorTCP cria um listener em uma determinada porta e lida com múltiplas
// conexões de entrada despachando-as para a função handler.
func iniciarServidorTCP(nome, porta string, handler func(net.Conn)) {
	listener, err := net.Listen("tcp", porta)
	if err != nil {
		log.Fatalf("Erro ao iniciar servidor %s na porta %s: %v", nome, porta, err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("Erro ao aceitar conexão %s: %v", nome, err)
			continue
		}
		go handler(conn)
	}
}

// isHigherPriority verifica se o processo "a" tem precedência de execução sobre o processo "b".
func isHigherPriority(a, b Process) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.TimeLeft < b.TimeLeft
}

// isWorsePriority verifica se o processo "a" é menos importante que o processo "b".
func isWorsePriority(a, b Process) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.TimeLeft > b.TimeLeft
}

// dispatch avalia a fila de pendências e distribui processos para drones disponíveis.
// Realiza a preempção interrompendo um drone em execução caso surja um processo
// com prioridade consideravelmente maior na fila.
func dispatch() {
	queueMU.Lock()
	droneMU.Lock()
	defer queueMU.Unlock()
	defer droneMU.Unlock()

	for len(Queue) > 0 {
		topProcess := Queue[0]

		var selectedDrone net.Conn
		var isFree bool
		var worstDrone net.Conn
		var worstProc *Process

		for conn, proc := range drones {
			if proc == nil {
				selectedDrone = conn
				isFree = true
				break
			}
			if worstProc == nil || isWorsePriority(*proc, *worstProc) {
				worstProc = proc
				worstDrone = conn
			}
		}

		if !isFree {
			if worstProc != nil && isHigherPriority(topProcess, *worstProc) {
				selectedDrone = worstDrone
			}
		}

		if selectedDrone != nil {
			p := heap.Pop(&Queue).(Process)
			drones[selectedDrone] = &p

			msg := fmt.Sprintf("%s,%s,%d,%d\n", p.Client, p.ID, p.Priority, p.TimeLeft)
			selectedDrone.Write([]byte(msg))
		} else {
			break
		}
	}
}

// handleBroker processa uma conexão conn de entrada de outro broker vizinho, lidando com
// os handshakes iniciais e respondendo a eventuais requisições de empréstimo de drones.
func handleBroker(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	ipPedinte, _, _ := net.SplitHostPort(remoteAddr)

	minhaPortaBroker := strings.TrimPrefix(portaBrokers, ":")
	minhaPortaDrone := strings.TrimPrefix(portaDrones, ":")
	conn.Write([]byte("HELLO_BROKER/" + minhaPortaBroker + "/" + minhaPortaDrone + "\n"))

	var trueBrokerAddr string

	defer func() {
		if trueBrokerAddr != "" {
			log.Printf("[BROKER] Conexão encerrada: %s\n", trueBrokerAddr)
			adjMU.Lock()
			delete(adjacentBrokers, trueBrokerAddr)
			delete(knownDroneAddrs, trueBrokerAddr)
			adjMU.Unlock()
		}
		conn.Close()
	}()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		linha := strings.TrimSpace(scanner.Text())
		parts := strings.Split(linha, "/")

		if len(parts) >= 3 && parts[0] == "HELLO_BROKER" {
			portaBrokerVizinho := parts[1]
			portaDroneVizinho := parts[2]

			trueBrokerAddr = ipPedinte + ":" + portaBrokerVizinho
			addrDroneVizinho := ipPedinte + ":" + portaDroneVizinho

			adjMU.Lock()
			adjacentBrokers[trueBrokerAddr] = conn
			knownDroneAddrs[trueBrokerAddr] = addrDroneVizinho
			adjMU.Unlock()

			log.Printf("[HANDSHAKE] Broker vizinho registrado com endereço real: %s\n", trueBrokerAddr)
			continue
		}

		if len(parts) >= 2 && parts[0] == "REQ_DRONE" {
			targetBrokerPort := parts[1]
			targetBrokerAddr := ipPedinte + ":" + targetBrokerPort

			log.Printf("[SISTEMA] Recebido pedido de drone do vizinho %s\n", targetBrokerAddr)

			queueMU.RLock()
			hasProcesses := len(Queue) > 0
			queueMU.RUnlock()

			if hasProcesses {
				log.Printf("[SISTEMA] Pedido do vizinho %s recusado: Temos processos na fila.\n", targetBrokerAddr)
				conn.Write([]byte("RESP_NO_DRONE\n"))
				continue
			}

			var idleDrone net.Conn
			droneMU.Lock()
			for dConn, proc := range drones {
				if proc == nil {
					idleDrone = dConn
					break
				}
			}

			if idleDrone != nil {
				log.Printf("[BROKER] Emprestando drone ocioso. Redirecionando para %s\n", targetBrokerAddr)
				idleDrone.Write([]byte("REDIRECT/" + targetBrokerAddr + "\n"))
				delete(drones, idleDrone)
				droneMU.Unlock()
			} else {
				droneMU.Unlock()
				log.Printf("[SISTEMA] Pedido do vizinho %s recusado: Nenhum drone livre.\n", targetBrokerAddr)
				conn.Write([]byte("RESP_NO_DRONE\n"))
			}
		}
	}
}

// handleClient atende cliente na conexão conn e sensores que enviam novas cargas de trabalho.
// Ele efetua o parsing da string recebida, gera o ID correspondente, anexa
// na fila e força a chamada do despachante.
func handleClient(conn net.Conn) {
	defer func() {
		log.Printf("[CLIENTE] Conexão encerrada: %s\n", conn.RemoteAddr().String())
		clientMU.Lock()
		delete(clientes, conn)
		clientMU.Unlock()
		conn.Close()
	}()

	clientIP := conn.RemoteAddr().String()
	log.Printf("[CLIENTE] Novo cliente conectado: %s\n", clientIP)

	clientMU.Lock()
	clientes[conn] = 0
	clientMU.Unlock()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		linha := scanner.Text()

		parts := strings.Split(linha, "/")
		if len(parts) != 2 {
			log.Printf("[CLIENTE %s] Mensagem inválida: %s\n", clientIP, linha)
			continue
		}

		kind := parts[0]
		data := parts[1]

		switch kind {
		case "P":
			log.Printf("[CLIENTE %s] Processo recebido: %s\n", clientIP, data)
			processParts := strings.Split(data, ",")
			if len(processParts) != 2 {
				log.Printf("[CLIENTE %s] Formato inválido: %s\n", clientIP, data)
				continue
			}

			priority, err1 := strconv.Atoi(processParts[0])
			timeLeft, err2 := strconv.Atoi(processParts[1])
			if err1 != nil || err2 != nil {
				log.Printf("[CLIENTE %s] Erro ao converter valores: %s\n", clientIP, data)
				continue
			}

			id := genProcessId(conn)
			process := Process{
				Client:   clientIP,
				ID:       id,
				Priority: priority,
				TimeLeft: timeLeft,
			}

			queueMU.Lock()
			heap.Push(&Queue, process)
			queueMU.Unlock()

			log.Printf("[CLIENTE %s] Processo criado na fila: %+v\n", clientIP, process)

			dispatch()
		}
	}
}

// handleDrone gerencia a comunicação constante com um drone ativo (conexão conn). Responsável por
// registrar a presença do drone, fornecer vizinhos para contingência e manipular
// tarefas encerradas ou interrompidas que o drone devolve ao broker.
func handleDrone(conn net.Conn) {
	droneMU.Lock()
	drones[conn] = nil
	droneMU.Unlock()

	adjMU.RLock()
	var neighbors []string
	for _, addr := range knownDroneAddrs {
		neighbors = append(neighbors, addr)
	}
	adjMU.RUnlock()

	if len(neighbors) > 0 {
		neighborStr := strings.Join(neighbors, ",")
		conn.Write([]byte("NEIGHBORS/" + neighborStr + "\n"))
	}

	dispatch()

	defer func() {
		log.Printf("[DRONE] Conexão encerrada: %s\n", conn.RemoteAddr().String())

		droneMU.Lock()
		proc := drones[conn]
		delete(drones, conn)
		droneMU.Unlock()

		if proc != nil {
			log.Printf("[DRONE %s] Recuperando processo %s devido a queda.\n", conn.RemoteAddr().String(), proc.ID)
			queueMU.Lock()
			heap.Push(&Queue, *proc)
			queueMU.Unlock()
			dispatch()
		}
		conn.Close()
	}()

	log.Printf("[DRONE] Novo drone conectado: %s\n", conn.RemoteAddr().String())

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		linha := scanner.Text()
		parts := strings.Split(strings.TrimSpace(linha), ",")

		if len(parts) == 4 {
			priority, _ := strconv.Atoi(parts[2])
			timeLeft, _ := strconv.Atoi(parts[3])

			p := Process{
				Client:   parts[0],
				ID:       parts[1],
				Priority: priority,
				TimeLeft: timeLeft,
			}

			if p.TimeLeft == 0 {
				log.Printf("[DRONE %s] Processo concluído: %s\n", conn.RemoteAddr().String(), p.ID)
				droneMU.Lock()
				if drones[conn] != nil && drones[conn].ID == p.ID {
					drones[conn] = nil
				}
				droneMU.Unlock()
			} else {
				log.Printf("[DRONE %s] Processo interrompido retornado: %s\n", conn.RemoteAddr().String(), p.ID)
				queueMU.Lock()
				heap.Push(&Queue, p)
				queueMU.Unlock()
			}

			dispatch()
		}
	}
}

// dialBroker estabelece e mantém ativamente uma conexão P2P de saída com o endereço de um broker
// conhecido (brokerIP), possibilitando que ambos mapeiem as portas de Drones entre si.
func dialBroker(brokerIP string) {
	for {
		conn, err := net.Dial("tcp", brokerIP)
		if err != nil {
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Conexão estabelecida com o broker %s\n", brokerIP)

		adjMU.Lock()
		adjacentBrokers[brokerIP] = conn
		adjMU.Unlock()

		minhaPortaBroker := strings.TrimPrefix(portaBrokers, ":")
		minhaPortaDrone := strings.TrimPrefix(portaDrones, ":")
		conn.Write([]byte("HELLO_BROKER/" + minhaPortaBroker + "/" + minhaPortaDrone + "\n"))

		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			linha := strings.TrimSpace(scanner.Text())
			parts := strings.Split(linha, "/")

			if len(parts) >= 3 && parts[0] == "HELLO_BROKER" {
				portaDroneVizinho := parts[2]
				ip, _, _ := net.SplitHostPort(brokerIP)
				addrDroneVizinho := ip + ":" + portaDroneVizinho

				adjMU.Lock()
				knownDroneAddrs[brokerIP] = addrDroneVizinho
				adjMU.Unlock()
				log.Printf("[HANDSHAKE] Vizinho %s registrou sua porta de drones: %s\n", ip, addrDroneVizinho)
				continue
			}

			if len(parts) >= 2 && parts[0] == "REQ_DRONE" {
				ip, _, _ := net.SplitHostPort(brokerIP)
				targetBrokerPort := parts[1]
				targetBrokerAddr := ip + ":" + targetBrokerPort

				log.Printf("[SISTEMA] Recebido pedido de drone do vizinho %s\n", targetBrokerAddr)

				queueMU.RLock()
				hasProcesses := len(Queue) > 0
				queueMU.RUnlock()

				if hasProcesses {
					log.Printf("[SISTEMA] Pedido do vizinho %s recusado: Temos processos na fila.\n", targetBrokerAddr)
					conn.Write([]byte("RESP_NO_DRONE\n"))
					continue
				}

				var idleDrone net.Conn
				droneMU.Lock()
				for dConn, proc := range drones {
					if proc == nil {
						idleDrone = dConn
						break
					}
				}

				if idleDrone != nil {
					log.Printf("[BROKER] Emprestando drone ocioso. Redirecionando para %s\n", targetBrokerAddr)
					idleDrone.Write([]byte("REDIRECT/" + targetBrokerAddr + "\n"))
					delete(drones, idleDrone)
					droneMU.Unlock()
				} else {
					droneMU.Unlock()
					log.Printf("[SISTEMA] Pedido do vizinho %s recusado: Nenhum drone livre.\n", targetBrokerAddr)
					conn.Write([]byte("RESP_NO_DRONE\n"))
				}
			}
		}

		log.Printf("A conexão com o broker %s foi interrompida. Removendo da lista de adjacentes.\n", brokerIP)
		adjMU.Lock()
		delete(adjacentBrokers, brokerIP)
		delete(knownDroneAddrs, brokerIP)
		adjMU.Unlock()

		time.Sleep(5 * time.Second)
	}
}

// genProcessId garante a criação de um hash simples ou identificador seguro a partir
// do endereço do cliente acoplado a um indexador numérico para rastreio global.
func genProcessId(conn net.Conn) string {
	clientMU.Lock()
	cont := strconv.Itoa(clientes[conn])
	clientes[conn]++
	clientMU.Unlock()
	ip := conn.RemoteAddr().String()
	return fmt.Sprintf("%s-%04s", ip, cont)
}

// monitorAndAskForDrones atua como um supervisor assíncrono que sonda brokers adjacentes num
// sistema de fila circular pedindo drones emprestados sempre que houver
// processos encalhados na estrutura sem que existam workers imediatos.
//
// -ips: Lista de endereços de vizinhos
func monitorAndAskForDrones(ips []string) {
	index := 0
	for {
		time.Sleep(10 * time.Second)

		queueMU.RLock()
		numProc := len(Queue)
		queueMU.RUnlock()

		if numProc > 0 && len(ips) > 0 {
			target := ips[index]
			adjMU.RLock()
			conn, ok := adjacentBrokers[target]
			adjMU.RUnlock()

			if ok {
				log.Printf("[SISTEMA] Fila com %d processos pendentes. Pedindo drone extra ao Broker %s\n", numProc, target)
				minhaPorta := strings.TrimPrefix(portaDrones, ":")
				conn.Write([]byte("REQ_DRONE/" + minhaPorta + "\n"))
			}

			index = (index + 1) % len(ips)
		}
	}
}
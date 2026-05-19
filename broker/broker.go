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

	// Dicionário de drones conectados. Se o ponteiro for nil, o drone está livre.
	drones  = make(map[net.Conn]*Process)
	droneMU = sync.RWMutex{}


	Queue = make(PriorityQueue, 0) // Inicializa a fila de processos

	adjacentBrokers = make(map[string]net.Conn)
	
	// NOVO: Dicionário que mapeia a conexão com o endereço exato do drone vizinho
	knownDroneAddrs = make(map[string]string) 
	adjMU           = sync.RWMutex{}
	
	portaDrones string // O endereço (porta) onde este broker ouve drones
)

type Process struct {
	Client   string
	ID       string
	Priority int
	TimeLeft int
}

type PriorityQueue []Process

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	return pq[i].TimeLeft < pq[j].TimeLeft
}

func (pq PriorityQueue) Swap(i, j int) { pq[i], pq[j] = pq[j], pq[i] }

func (pq *PriorityQueue) Push(x any) { *pq = append(*pq, x.(Process)) }

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("ERR: Incorrect command usage\nCorrect usage: go run server.go <p2p_port> <client_port> <drone_port> <broker1_ip:port> ...")
	}

	brokerIPs := os.Args[4:]
	portaBrokers := ":" + os.Args[1]
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

		log.Printf("--- Status Atual ---")
		log.Printf("Fila de Processos (%d):", len(Queue))
		
		// 1. Cria uma cópia exata da fila atual
		filaExibicao := make([]Process, len(Queue))
		copy(filaExibicao, Queue)

		// 2. Ordena a cópia linearmente seguindo a mesma regra do seu Heap
		sort.Slice(filaExibicao, func(i, j int) bool {
			if filaExibicao[i].Priority != filaExibicao[j].Priority {
				return filaExibicao[i].Priority > filaExibicao[j].Priority
			}
			return filaExibicao[i].TimeLeft < filaExibicao[j].TimeLeft
		})

		// 3. Printa a cópia (agora visualmente ordenada)
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
		for conn := range clientes{
			log.Printf("  - Sensor %s", conn.RemoteAddr().String())
		}

		log.Printf("--------------------")
		log.Printf("Brokers vizinhos (%d):", len(adjacentBrokers))
		for addr := range adjacentBrokers{
			log.Printf("  - Broker %s", addr)
		}

		droneMU.RUnlock()
		queueMU.RUnlock()
	}
}

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

func isHigherPriority(a, b Process) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return a.TimeLeft < b.TimeLeft
}

func isWorsePriority(a, b Process) bool {
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	return a.TimeLeft > b.TimeLeft
}

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

func handleBroker(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	ipPedinte, _, _ := net.SplitHostPort(remoteAddr)

	adjMU.Lock()
	adjacentBrokers[remoteAddr] = conn
	adjMU.Unlock()

	// --- HANDSHAKE INICIAL --- Envia minha porta de drones para o broker que conectou
	minhaPorta := strings.TrimPrefix(portaDrones, ":")
	conn.Write([]byte("HELLO_BROKER/" + minhaPorta + "\n"))

	defer func() {
		log.Printf("[BROKER] Conexão encerrada: %s\n", remoteAddr)
		adjMU.Lock()
		delete(adjacentBrokers, remoteAddr)
		delete(knownDroneAddrs, remoteAddr) // Limpa o vizinho
		adjMU.Unlock()
		conn.Close()
	}()

	log.Printf("[BROKER] Novo broker conectado: %s\n", remoteAddr)
	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		linha := strings.TrimSpace(scanner.Text())
		parts := strings.Split(linha, "/")

		// --- RECEBIMENTO DO HANDSHAKE DO VIZINHO ---
		if len(parts) >= 2 && parts[0] == "HELLO_BROKER" {
			portaDoVizinho := parts[1]
			addrDroneVizinho := ipPedinte + ":" + portaDoVizinho
			
			adjMU.Lock()
			knownDroneAddrs[remoteAddr] = addrDroneVizinho
			adjMU.Unlock()
			
			log.Printf("[HANDSHAKE] Vizinho %s registrou sua porta de drones: %s\n", ipPedinte, addrDroneVizinho)
			continue
		}

		if len(parts) >= 2 && parts[0] == "REQ_DRONE" {
			targetBrokerPort := parts[1]
			targetBrokerAddr := ipPedinte + ":" + targetBrokerPort // Monta o endereço dinâmico!

			queueMU.RLock()
			hasProcesses := len(Queue) > 0
			queueMU.RUnlock()

			if hasProcesses {
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
				conn.Write([]byte("RESP_NO_DRONE\n"))
			}
		}
	}
}

func handleClient(conn net.Conn) {
	defer func() {
		log.Printf("[CLIENTE] Conexão encerrada: %s\n", conn.RemoteAddr().String())
		conn.Close()
	}()

	clientIP := conn.RemoteAddr().String()
	log.Printf("[CLIENTE] Novo cliente conectado: %s\n", clientIP)

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

			// Tenta despachar a fila imediatamente apos receber um novo processo
			dispatch()
		}
	}
}

func handleDrone(conn net.Conn) {
	droneMU.Lock()
	drones[conn] = nil
	droneMU.Unlock()

	// --- HANDSHAKE DRONE-BROKER ---
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
	// ---------------------------------

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

		// --- HANDSHAKE INICIAL ---
		minhaPorta := strings.TrimPrefix(portaDrones, ":")
		conn.Write([]byte("HELLO_BROKER/" + minhaPorta + "\n"))

		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			linha := scanner.Text()
			
			// --- RECEBIMENTO DO HANDSHAKE DO VIZINHO ---
			if strings.HasPrefix(linha, "HELLO_BROKER/") {
				portaDoVizinho := strings.Split(linha, "/")[1]
				ip, _, _ := net.SplitHostPort(brokerIP)
				addrDroneVizinho := ip + ":" + portaDoVizinho
				
				adjMU.Lock()
				knownDroneAddrs[brokerIP] = addrDroneVizinho
				adjMU.Unlock()
				log.Printf("[HANDSHAKE] Vizinho %s registrou sua porta de drones: %s\n", ip, addrDroneVizinho)
				continue
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

func genProcessId(conn net.Conn) string {
	clientMU.Lock()
	cont := strconv.Itoa(clientes[conn])
	clientes[conn]++
	clientMU.Unlock()
	ip := conn.RemoteAddr().String()
	return fmt.Sprintf("%s-%04s", ip, cont)
}

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
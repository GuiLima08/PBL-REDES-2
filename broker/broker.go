package main

import (
	"bufio"
	"container/heap"
	"fmt"
	"log"
	"net"
	"os"
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
	adjMU           = sync.RWMutex{}
	myDroneAddr     string // O endereço (porta) onde este broker ouve drones
)

type Process struct {
	Client   string // Alterado para string (IP do cliente)
	ID       string // Identificador único do processo
	Priority int    // Prioridade de 1 a 10
	TimeLeft int    // Tempo restante para execução em segundos
}

type PriorityQueue []Process

func (pq PriorityQueue) Len() int { return len(pq) }

func (pq PriorityQueue) Less(i, j int) bool {
	if pq[i].Priority != pq[j].Priority {
		return pq[i].Priority > pq[j].Priority
	}
	return pq[i].TimeLeft < pq[j].TimeLeft
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x any) {
	*pq = append(*pq, x.(Process))
}

func (pq *PriorityQueue) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	*pq = old[0 : n-1]
	return item
}

func main() {
	if len(os.Args) < 4 {
		log.Fatalf("ERR: Incorrect command usage\nCorrect usage: go run server.go <p2p_port> <client_port> <drone_port> <broker1_ip:port> <broker2_ip:port> ... <brokerN_ip:port>")
	}

	brokerIPs := os.Args[4:]
	portaBrokers := ":" + os.Args[1]
	portaClientes := ":" + os.Args[2]
	portaDrones := ":" + os.Args[3]

	// Captura o IP real da máquina na rede (ex: 192.168.0.15)
	ipReal := getOutboundIP()
	myDroneAddr = ipReal + portaDrones
	log.Printf("[SISTEMA] IP principal desta máquina detectado como: %s", ipReal)

	// Inicializa a estrutura de heap
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
		for _, process := range Queue {
			log.Printf("  - %+v", process)
		}

		log.Printf("Drones Conectados (%d):", len(drones))
		for conn, process := range drones {
			if process == nil {
				log.Printf("  - Drone %s: LIVRE", conn.RemoteAddr().String())
			} else {
				log.Printf("  - Drone %s: Executando %s", conn.RemoteAddr().String(), process.ID)
			}
		}
		log.Printf("--------------------")

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

// Helpers para verificacao de prioridade
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

// dispatch é o cerebro do sistema. Pega os processos da fila e atribui aos drones.
func dispatch() {
	queueMU.Lock()
	droneMU.Lock()
	defer queueMU.Unlock()
	defer droneMU.Unlock()

	for len(Queue) > 0 {
		topProcess := Queue[0] // Espia o processo de maior prioridade

		var selectedDrone net.Conn
		var isFree bool
		var worstDrone net.Conn
		var worstProc *Process

		// Procura um drone livre ou identifica o drone executando o processo de menor prioridade
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

		// Se nao achou drone livre, verifica se vale a pena interromper o pior drone
		if !isFree {
			if worstProc != nil && isHigherPriority(topProcess, *worstProc) {
				selectedDrone = worstDrone
			}
		}

		// Se encontrou um drone viavel (livre ou que pode ser interrompido)
		if selectedDrone != nil {
			p := heap.Pop(&Queue).(Process)
			drones[selectedDrone] = &p // Atualiza o estado no broker

			// Envia via TCP usando o buffer definido
			msg := fmt.Sprintf("%s,%s,%d,%d\n", p.Client, p.ID, p.Priority, p.TimeLeft)
			selectedDrone.Write([]byte(msg))
		} else {
			// Se o topo da fila nao vence de nenhum drone ocupado, nenhum outro vencera
			break
		}
	}
}

func handleBroker(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()

	// 1. Registra este broker na lista de adjacentes assim que ele conecta
	adjMU.Lock()
	adjacentBrokers[remoteAddr] = conn
	adjMU.Unlock()

	defer func() {
		log.Printf("[BROKER] Conexão encerrada: %s\n", remoteAddr)
		
		// 2. Remove da lista ao desconectar para evitar chamadas fantasmas
		adjMU.Lock()
		delete(adjacentBrokers, remoteAddr)
		adjMU.Unlock()
		
		conn.Close()
	}()

	log.Printf("[BROKER] Novo broker conectado: %s\n", remoteAddr)

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		linha := strings.TrimSpace(scanner.Text())
		
		// Mantendo o seu log original para monitoramento!
		log.Printf("[BROKER %s] Recebido: %s\n", remoteAddr, linha)

		// 3. Nova lógica: interpretando a mensagem recebida
		parts := strings.Split(linha, "/")
		
		if len(parts) >= 2 && parts[0] == "REQ_DRONE" {
			targetBrokerAddr := parts[1]
			
			// Verifica se este broker tem processos próprios na fila (Prioridade Local)
			queueMU.RLock()
			hasProcesses := len(Queue) > 0
			queueMU.RUnlock()

			if hasProcesses {
				// Se tenho trabalho pendente, recuso o empréstimo
				conn.Write([]byte("RESP_NO_DRONE\n"))
				continue
			}

			// Procura um drone ocioso nos meus registros
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
				
				// Manda o comando pro drone se desconectar daqui e ir pro outro broker
				idleDrone.Write([]byte("REDIRECT/" + targetBrokerAddr + "\n"))
				
				// Libera o drone do meu mapa local, pois ele já vai fechar a conexão
				delete(drones, idleDrone)
				droneMU.Unlock()
			} else {
				// Se não achou nenhum livre
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
	// Adiciona o drone ao mapa assim que ele se conecta
	droneMU.Lock()
	drones[conn] = nil
	droneMU.Unlock()

	// Verifica se ha algo na fila que ja pode ser enviado
	dispatch()

	defer func() {
		log.Printf("[DRONE] Conexão encerrada: %s\n", conn.RemoteAddr().String())

		droneMU.Lock()
		proc := drones[conn]
		delete(drones, conn)
		droneMU.Unlock()

		// Se o drone caiu enquanto realizava um processo, devolve o processo para a fila
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

				// Libera o drone verificando se o processo concluido era de fato o que estava alocado
				droneMU.Lock()
				if drones[conn] != nil && drones[conn].ID == p.ID {
					drones[conn] = nil
				}
				droneMU.Unlock()
			} else {
				// Processo devolvido por interrupcao. Retorna para a fila.
				log.Printf("[DRONE %s] Processo interrompido retornado: %s\n", conn.RemoteAddr().String(), p.ID)
				queueMU.Lock()
				heap.Push(&Queue, p)
				queueMU.Unlock()
			}

			// O drone terminou ou devolveu o processo antigo; tenta mandar trabalho novo
			dispatch()
		}
	}
}

func dialBroker(brokerIP string) {
	for {
		// Capturamos a conexão 'conn' em vez de descartá-la com '_'
		conn, err := net.Dial("tcp", brokerIP)
		if err != nil {
			log.Printf("Erro ao conectar ao broker %s: %v", brokerIP, err)
			log.Printf("Tentando conectar ao broker %s novamente em 5 segundos...", brokerIP)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("Conexão estabelecida com o broker %s\n", brokerIP)

		// REGISTRO: Adicionamos a conexão ao mapa de adjacentes para podermos enviar REQ_DRONE
		adjMU.Lock()
		adjacentBrokers[brokerIP] = conn
		adjMU.Unlock()

		// MONITORAMENTO ATIVO: Substituímos o seu loop de Sleep por um Scanner.
		// Isso mantém a conexão aberta e permite processar mensagens (como RESP_NO_DRONE).
		scanner := bufio.NewScanner(conn)
		for scanner.Scan() {
			linha := scanner.Text()
			log.Printf("[BROKER ADJACENTE %s] Mensagem: %s\n", brokerIP, linha)
		}

		// LIMPEZA: Se o scanner sair do loop, a conexão caiu ou foi encerrada.
		log.Printf("A conexão com o broker %s foi interrompida. Removendo da lista de adjacentes.\n", brokerIP)
		adjMU.Lock()
		delete(adjacentBrokers, brokerIP)
		adjMU.Unlock()

		// Aguarda antes de tentar reconectar ao broker que caiu
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

		droneMU.RLock()
		numDrones := len(drones)
		droneMU.RUnlock()

		queueMU.RLock()
		numProc := len(Queue)
		queueMU.RUnlock()

		// Se tenho processos mas não tenho drones, peço um
		if numDrones == 0 && numProc > 0 && len(ips) > 0 {
			target := ips[index]
			adjMU.RLock()
			conn, ok := adjacentBrokers[target]
			adjMU.RUnlock()

			if ok {
				log.Printf("[SISTEMA] Pedindo drone ao Broker %s\n", target)
				conn.Write([]byte("REQ_DRONE/" + myDroneAddr + "\n"))
			}

			index = (index + 1) % len(ips)
		}
	}
}

// getOutboundIP descobre o IP principal da máquina na rede local
func getOutboundIP() string {
	// Usamos um endereço externo (Google DNS) apenas para forçar o SO 
	// a determinar a rota de saída. Nenhum pacote real é enviado.
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		// Se a máquina estiver completamente sem internet/rede, cai num fallback
		log.Printf("-!- Aviso: Não foi possível determinar o IP da rede. Usando localhost.")
		return "127.0.0.1"
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP.String()
}

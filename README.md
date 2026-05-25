# Infraestrutura Distribuída para Coordenação de Drones Autônomos

Este projeto é uma simulação de um sistema distribuído e descentralizado para o monitoramento marítimo no Estreito de Ormuz. Ele coordena o despacho de drones autônomos utilizando uma malha Híbrida: opera como Cliente-Servidor localmente e Peer-to-Peer (P2P) globalmente, eliminando qualquer Ponto Único de Falha (Single Point of Failure).

## Estrutura do Projeto

O sistema é composto por cinco módulos principais, empacotados em suas próprias imagens Docker:

* **Broker (`hormuz-broker`)**: O gestor de setor. Ele orquestra uma Fila de Prioridade local de processos, distribui missões para a frota e se conecta a outros Brokers vizinhos para emprestar recursos dinamicamente.
* **Drone (`hormuz-drone`)**: O nó trabalhador (worker). Recebe e executa processos cronometrados. Possui capacidade de *Self-Healing*: se o seu gestor cair, ele migra autonomamente para outro setor.
* **Sensor Automático (`hormuz-sensor`)**: Cliente emissor contínuo. Gera e injeta requisições de monitoramento (processos) com prioridades e tempos aleatórios no sistema.
* **Usuário Manual (`hormuz-user`)**: Interface de Terminal (TUI) interativa que permite a um operador humano formatar e enviar missões críticas customizadas diretamente para o Broker.
* **Race Test (`hormuz-racetest`)**: Ferramenta de teste de estresse que injeta cargas maciças de conexões simultâneas para validar a integridade dos Mutexes e a ausência de *Race Conditions*.

---

### Detalhamento dos Pacotes e Bibliotecas

O repositório foi organizado na seguinte estrutura, onde cada componente atua como um microsserviço independente.

    .
    ├── README.md
    ├── broker/
    |     ├── broker.go          # Código-fonte do Gestor de Setor (Nó P2P)
    |     └── Dockerfile         # Build stage do broker
    ├── drone/
    |     ├── drone.go           # Código-fonte do Veículo Autônomo (Worker)
    |     └── Dockerfile         # Build stage do drone
    ├── user/
    |     ├── auto/
    |     |    ├── sensor.go     # Código-fonte do Gerador de Carga Automático
    |     |    └── Dockerfile    # Build stage do sensor
    |     └── manual/
    |          ├── user.go       # Código-fonte da Aplicação Cliente (Painel)
    |          └── Dockerfile    # Build stage do painel manual
    └── teste/
          ├── raceTest.go        # Código de teste de concorrência e estresse
          └── Dockerfile         # Build stage do teste

* **Pacotes Go (`package main`):** Devido à natureza distribuída, todos os arquivos `.go` operam no pacote `main` e geram binários independentes.
* **Bibliotecas Utilizadas:** O sistema não utiliza frameworks externos de mensageria (como RabbitMQ ou Kafka). O roteamento distribuído foi construído do zero utilizando a **Standard Library do Go**:
    * `net`: Construção e manipulação dos *Sockets* TCP em toda a topologia.
    * `sync`: Exclusão mútua distribuída local com `RWMutex` (proteção de Heaps e Dicionários).
    * `container/heap`: Estruturação da Fila de Prioridade distribuída por setor.
    * `bufio` e `strings`: *Parsing* do protocolo customizado de texto plano.
    * `time`: Controle de preempção de drones e rotinas de *Timeout* e *Retries*.

### Configurações de Ambiente (Docker)
A compilação do ecossistema é automatizada via Docker utilizando a técnica de **Multi-stage Build**:
1. **Estágio Builder (`golang:alpine`):** Compila o código-fonte Go em binários estáticos, lidando com as dependências do SO.
2. **Estágio Final (`alpine:latest`):** Isola os binários em contêineres minimalistas de baixíssimo consumo de armazenamento e CPU, prontos para execução distribuída na nuvem ou laboratório.

---

## Fluxo de Dados e Comunicação

O roteamento da malha utiliza TCP de ponta a ponta, suportado por um protocolo em texto plano altamente otimizado para as seguintes regras de negócio:

1.  **Comunicação Inter-Setores (P2P entre Brokers):** * Os Brokers realizam um *Handshake* (`HELLO_BROKER`) informando suas portas de escuta. 
    * Caso um setor fique sem drones para uma emergência, ele emite um sinal de socorro (`REQ_DRONE`). O vizinho, se ocioso, responde forçando um drone a migrar para a área sobrecarregada (`REDIRECT`).
2.  **Comunicação Local (Clientes e Drones):**
    * Drones conectam-se ativamente e aprendem o endereço dos vizinhos (`NEIGHBORS/`) para fins de contingência.
    * Sensores e Usuários emitem alertas no formato `P/<prioridade>,<tempo>`. Eles implementam *Retries* bloqueantes: se a rede cair durante o envio, a rotina congela e reenvia a missão até a confirmação de entrega.

---

## Como Utilizar (Guia de Execução)

### Opção 1: Download do Docker Hub (Recomendado)
O ecossistema já está pré-compilado e hospedado publicamente. Você pode baixar as imagens diretamente:

    docker pull bdaemonis/hormuz-broker:latest
    docker pull bdaemonis/hormuz-drone:latest
    docker pull bdaemonis/hormuz-sensor:latest
    docker pull bdaemonis/hormuz-user:latest
    docker pull bdaemonis/hormuz-racetest:latest

### Opção 2: Compilação Local
Para compilar localmente, abra o terminal na raiz do projeto e execute:

    docker build -t bdaemonis/hormuz-broker:latest -f broker/Dockerfile broker/
    docker build -t bdaemonis/hormuz-drone:latest -f drone/Dockerfile drone/
    docker build -t bdaemonis/hormuz-sensor:latest -f user/auto/Dockerfile user/auto/
    docker build -t bdaemonis/hormuz-user:latest -f user/manual/Dockerfile user/manual/
    docker build -t bdaemonis/hormuz-racetest:latest -f teste/Dockerfile teste/

---

### 1. Iniciando os Gestores de Setor (Brokers)
Os Brokers devem ser os primeiros a subir. A sintaxe requer: `<porta_p2p> <porta_clientes> <porta_drones> [ips_vizinhos...]`.
*Lembre-se de mapear as 3 portas no Docker via `-p`.*

    # Exemplo subindo o Broker 1 (sem vizinhos inicialmente)
    docker run -p 8001:8001 -p 8002:8002 -p 8003:8003 bdaemonis/hormuz-broker:latest 8001 8002 8003
    
    # Exemplo subindo o Broker 2 (informando o IP do Broker 1 como vizinho)
    docker run -p 9001:9001 -p 9002:9002 -p 9003:9003 bdaemonis/hormuz-broker:latest 9001 9002 9003 <ip_broker_1>:8001

### 2. Ativando a Frota Autônoma (Drones)
Os drones devem ser apontados para a porta de drones de um Broker específico. Use o modo *detached* (`-d`).

    docker run --rm -d bdaemonis/hormuz-drone:latest <ip_broker>:<porta_drones>

### 3. Injetando Carga Contínua (Sensores Automáticos)
Para iniciar a simulação ambiental, aponte os sensores para a porta de clientes do Broker desejado.

    docker run --rm -d bdaemonis/hormuz-sensor:latest <ip_broker>:<porta_clientes>

### 4. Acessando o Despacho Manual (Painel do Usuário)
Para enviar ocorrências específicas, execute o operador de forma interativa (`-it`):

    docker run -it --rm bdaemonis/hormuz-user:latest <ip_broker>:<porta_clientes>

### 5. Executando o Teste de Estresse (Race Test)
Para validar a proteção de concorrência disparando conexões massivas de uma vez só:

    docker run -it --rm bdaemonis/hormuz-racetest:latest -addr <ip_broker>:<porta_clientes> -users 50 -msgs 10

---

## Casos de Uso (Arquiteturais)

* **Resiliência (Self-Healing):** Conecte um Drone ao Broker A. Derrube o contêiner do Broker A. Observe nos logs o Drone assumindo controle autônomo e migrando sua conexão para o Broker B em segundos.
* **Preempção de Missão:** Envie via Painel de Usuário uma missão de longa duração com Prioridade 1 (baixa). Em seguida, envie uma missão com Prioridade 10 (alta). O Broker interromperá o voo atual do drone, devolverá a missão 1 para a fila, e o alocará imediatamente para a missão 10.
* **Balanço de Carga P2P:** Inunde um Broker de requisições sem conectar drones a ele. Conecte drones ociosos no Broker vizinho. Observe o log do sistema realizando o empréstimo redirecionando os drones geograficamente para socorrer a fila superlotada.

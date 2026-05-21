package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Realiza o parsing das flags de linha de comando para configurar
// os parâmetros da simulação, inicia o relógio de marcação,
// dispara as goroutines concorrentes e aguarda a conclusão de todos
// os envios antes de imprimir o relatório de desempenho.
func main() {
	addr := flag.String("addr", "127.0.0.1:8002", "Endereço e porta de Clientes do Broker")
	users := flag.Int("users", 50, "Número de usuários simultâneos (goroutines)")
	msgs := flag.Int("msgs", 10, "Número de processos que CADA usuário vai enviar")
	fixedPri := flag.Int("pri", 0, "Prioridade fixa (1 a 10). Se 0, será aleatória.")
	fixedTime := flag.Int("time", 0, "Tempo fixo (segundos). Se 0, será aleatório.")

	flag.Parse()

	fmt.Println("==================================================")
	fmt.Println(" INICIANDO RACE TEST DISTRIBUÍDO")
	fmt.Printf("Alvo: %s\n", *addr)
	fmt.Printf("Usuários concorrentes: %d\n", *users)
	fmt.Printf("Processos por usuário: %d\n", *msgs)
	if *fixedPri > 0 {
		fmt.Printf("Prioridade: FIXA (%d)\n", *fixedPri)
	} else {
		fmt.Println("Prioridade: ALEATÓRIA (1-10)")
	}
	if *fixedTime > 0 {
		fmt.Printf("Tempo: FIXO (%ds)\n", *fixedTime)
	} else {
		fmt.Println("Tempo: ALEATÓRIO (1-30s)")
	}
	fmt.Println("==================================================")

	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 1; i <= *users; i++ {
		wg.Add(1)
		go simulateUser(i, *addr, *msgs, *fixedPri, *fixedTime, &wg)
	}

	wg.Wait()

	duracao := time.Since(startTime)
	fmt.Println("==================================================")
	fmt.Printf(" TESTE FINALIZADO!\n")
	fmt.Printf("Total de processos enviados: %d\n", (*users)*(*msgs))
	fmt.Printf("Tempo de execução do teste: %v\n", duracao)
	fmt.Println("==================================================")
}

// Atua como um cliente individual autônomo que se conecta ao Broker no
// endereço addr e dispara uma quantidade msgs de requisições consecutivas.
//
// Os parâmetros fixedPri e fixedTime determinam a prioridade e a duração de cada tarefa;
// caso possuam valor zero, a função gera valores aleatórios a cada envio. O id serve
// para identificar a goroutine nos logs de erro, enquanto o wg permite sinalizar a
// conclusão das tarefas para a rotina principal. A função também implementa micro-pausas
// aleatórias para emular latência de rede real e evitar a aglutinação de pacotes (packet coalescing)
// no buffer TCP.
func simulateUser(id int, addr string, msgs, fixedPri, fixedTime int, wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("[Usuário %d] Falha ao conectar: %v\n", id, err)
		return
	}
	defer conn.Close()

	for i := 0; i < msgs; i++ {
		pri := fixedPri
		if pri <= 0 || pri > 10 {
			pri = rand.Intn(10) + 1
		}

		duration := fixedTime
		if duration <= 0 {
			duration = rand.Intn(30) + 1
		}

		msg := fmt.Sprintf("P/%d,%d\n", pri, duration)

		_, err := conn.Write([]byte(msg))
		if err != nil {
			log.Printf("[Usuário %d] Erro ao enviar dado na iteração %d: %v\n", id, i, err)
			return
		}

		time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
	}
}
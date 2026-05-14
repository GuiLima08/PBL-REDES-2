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

func main() {
	// Flags de linha de comando para customizar o teste
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

	// Lança 'N' goroutines simulando usuários conectando ao mesmo tempo
	for i := 1; i <= *users; i++ {
		wg.Add(1)
		go simulateUser(i, *addr, *msgs, *fixedPri, *fixedTime, &wg)
	}

	// Aguarda todos os usuários terminarem seus envios
	wg.Wait()
	
	duracao := time.Since(startTime)
	fmt.Println("==================================================")
	fmt.Printf(" TESTE FINALIZADO!\n")
	fmt.Printf("Total de processos enviados: %d\n", (*users)*(*msgs))
	fmt.Printf("Tempo de execução do teste: %v\n", duracao)
	fmt.Println("==================================================")
}

func simulateUser(id int, addr string, msgs, fixedPri, fixedTime int, wg *sync.WaitGroup) {
	defer wg.Done()

	// Tenta conectar ao Broker
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Printf("[Usuário %d] Falha ao conectar: %v\n", id, err)
		return
	}
	defer conn.Close()

	for i := 0; i < msgs; i++ {
		pri := fixedPri
		if pri <= 0 || pri > 10 {
			pri = rand.Intn(10) + 1 // Gera de 1 a 10
		}

		duration := fixedTime
		if duration <= 0 {
			duration = rand.Intn(30) + 1 // Gera de 1 a 30 segundos
		}

		// Formata a mensagem exatamente como o user.go original
		msg := fmt.Sprintf("P/%d,%d\n", pri, duration)
		
		_, err := conn.Write([]byte(msg))
		if err != nil {
			log.Printf("[Usuário %d] Erro ao enviar dado na iteração %d: %v\n", id, i, err)
			return
		}

		// Adiciona um micro-atraso aleatório (0 a 10 milissegundos) entre os envios do mesmo usuário.
		// Isso evita que o buffer TCP junte tudo numa string só (packet coalescing) 
		// e garante que as requisições cheguem "embaralhadas" com as de outros usuários.
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(10)))
	}
}
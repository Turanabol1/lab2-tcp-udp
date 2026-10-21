package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	tcpPort = ":9000"
	udpPort = ":9001"
)

type Server struct {
	mu      sync.Mutex
	clients map[net.Conn]bool
	wg      sync.WaitGroup
}

func NewServer() *Server {
	return &Server{clients: make(map[net.Conn]bool)}
}

func (s *Server) addClient(conn net.Conn) {
	s.mu.Lock()
	s.clients[conn] = true
	s.mu.Unlock()
}

func (s *Server) removeClient(conn net.Conn) {
	s.mu.Lock()
	delete(s.clients, conn)
	s.mu.Unlock()
}

func (s *Server) broadcast(msg string, except net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.clients {
		if c == except {
			continue
		}
		_, err := fmt.Fprintln(c, msg)
		if err != nil {
			log.Println("[ПОМИЛКА] не вдалося надіслати клієнту", c.RemoteAddr(), ":", err)
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer s.wg.Done()

	addr := conn.RemoteAddr().String()
	s.addClient(conn)
	fmt.Println("[ПІДКЛЮЧЕННЯ] Новий клієнт підключився:", addr)

	defer func() {
		s.removeClient(conn)
		conn.Close()
		fmt.Println("[ВІДКЛЮЧЕННЯ] Клієнт відключився:", addr)
		s.broadcast("[СИСТЕМА] Клієнт відключився: "+addr, nil)
	}()

	fmt.Fprintln(conn, "[СЕРВЕР] Ласкаво просимо до мережевого сервісу диспетчеризації!")
	s.broadcast("[СИСТЕМА] Підключився новий клієнт: "+addr, conn)

	reader := bufio.NewReader(conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		cmd := strings.ToUpper(parts[0])
		arg := ""
		if len(parts) > 1 {
			arg = strings.TrimSpace(parts[1])
		}

		switch cmd {
		case "ECHO":
			fmt.Fprintln(conn, "[ECHO]: "+arg)
		case "TIME":
			fmt.Fprintln(conn, "[TIME]: "+time.Now().Format(time.RFC3339))
		case "BROADCAST", "MSG":
			if arg == "" {
				fmt.Fprintln(conn, "[ПОМИЛКА] Порожнє повідомлення. Використання: BROADCAST <текст>")
				continue
			}
			fmt.Printf("[РОЗСИЛКА] Від %s -> \"%s\"\n", addr, arg)
			s.broadcast("[СПОВІЩЕННЯ від "+addr+"]: "+arg, conn)
		case "CLOSE", "QUIT":
			fmt.Fprintln(conn, "[СЕРВЕР] До побачення!")
			return
		default:
			fmt.Fprintln(conn, "[ПОМИЛКА] Невідома команда. Доступні: ECHO <текст>, TIME, BROADCAST <текст>, MSG <текст>, CLOSE, QUIT")
		}
	}
}

func (s *Server) listenUDP(pc net.PacketConn) {
	defer s.wg.Done()

	buffer := make([]byte, 1024)
	for {
		n, from, err := pc.ReadFrom(buffer)
		if err != nil {
			return
		}
		data := strings.TrimSpace(string(buffer[:n]))
		fmt.Printf("[UDP ТЕЛЕМЕТРІЯ] Отримано %d байт від %s -> \"%s\"\n", n, from.String(), data)
		s.broadcast("[TELEMETRY from "+from.String()+"]: "+data, nil)
	}
}

func (s *Server) shutdown(tcpListener net.Listener, udpConn net.PacketConn) {
	s.broadcast("[СИСТЕМА]: SERVER_SHUTTING_DOWN", nil)

	tcpListener.Close()
	udpConn.Close()

	s.mu.Lock()
	count := len(s.clients)
	for c := range s.clients {
		c.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	fmt.Printf("[СЕРВЕР] Сповіщено активних клієнтів: %d. З'єднання безпечно закриті.\n", count)
}

func main() {
	fmt.Println("=== Мережевий сервіс диспетчеризації (TCP: 9000, UDP: 9001) ===")

	server := NewServer()

	tcpListener, err := net.Listen("tcp", tcpPort)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("[СЕРВЕР] TCP-слухач успішно відкрито на порту 9000")

	udpConn, err := net.ListenPacket("udp", udpPort)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("[СЕРВЕР] UDP-слухач телеметрії відкрито на порту 9001")

	server.wg.Add(1)
	go server.listenUDP(udpConn)

	server.wg.Add(1)
	go func() {
		defer server.wg.Done()
		for {
			conn, err := tcpListener.Accept()
			if err != nil {
				return
			}
			server.wg.Add(1)
			go server.handleConnection(conn)
		}
	}()

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	<-stopChan

	fmt.Println()
	fmt.Println("[СЕРВЕР] Отримано сигнал завершення роботи. Запуск Graceful Shutdown...")
	server.shutdown(tcpListener, udpConn)
	fmt.Println("[СЕРВЕР] Роботу сервера успішно завершено.")
}

package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	SERVER_HOST = "localhost"
	SERVER_TYPE = "tcp"
)

type Node struct {
	server_port string
	mu          sync.Mutex
	all_conn    map[string]net.Conn
}

func delayAgent(min int, max int) {
	r := rand.Intn(max-min) + min
	time.Sleep(time.Duration(r) * time.Second)
}

func parsemessage(message string) (string, string) {
	result := strings.Split(message, ":")
	message_type := strings.Trim(result[0], " ")
	new_val := strings.Trim(result[3], " ")
	return message_type, new_val
}
func (node *Node) listenClient(connection net.Conn, id string) {
	for {
		buffer := make([]byte, 1024)
		mLen, err := connection.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
			delete(node.all_conn, id)
			break
		}

		// msg := "PREPARE: MSG FROM : " + port + ": <new-val> ;"
		messages := strings.Split(string(buffer[:mLen]), ";")
		for _, message := range messages {
			if message == "" {
				continue
			}
			fmt.Println("MESSAGE RECEIVED: ", message)
			message_type, new_val := parsemessage(message)
			if message_type == "ACK-UPDATE" {
				fmt.Println("UPDATED THE DATA with " + new_val)
			} else if message_type == "READ-DONE" {
				fmt.Println("READ VALUE : ", new_val)
			}
		}
	}
}

func (node *Node) BroadCastMessage(message string) {
	fmt.Println("broadcasting to replicas")
	for port, conn := range node.all_conn {
		fmt.Println("Sending Message to - ", port)
		go node.SendMessage(conn, message)
	}

}

func (node *Node) SendMessage(conn net.Conn, message string) {
	// delayAgent(1, 2)
	_, err := conn.Write([]byte(message))
	if err != nil {
		panic("Error sending message ;( ")
	}
}

func (node *Node) establishConnections(wg *sync.WaitGroup, server_ports []string, my_port string) {
	fmt.Println("TRYING TO ESTABLISH CONNECTIONS WITH SERVERS")
	for {
		no_done := 0
		for _, port := range server_ports {
			node.mu.Lock()
			_, ok := node.all_conn[port]
			node.mu.Unlock()
			if ok {
				fmt.Println("Already a connection is present to - ", port)
				no_done++
			} else {
				conn, err := net.Dial(SERVER_TYPE, SERVER_HOST+":"+port)
				if err != nil {
					fmt.Println("Error occured in connection with port: ", port)
				} else {
					node.mu.Lock()
					node.all_conn[port] = conn
					node.mu.Unlock()
					_, err = conn.Write([]byte(my_port))
					go node.listenClient(conn, port)
					if err != nil {
						panic("Error sending message ;( ")
					}
				}
			}
		}
		if no_done == len(server_ports) {
			fmt.Println("All Connections Ready")
			break
		}
	}
}

func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	node := Node{all_conn: make(map[string]net.Conn), server_port: os.Args[1]}
	node.establishConnections(&wg, os.Args[2:], os.Args[1])
	go node.SendMessage(node.all_conn["80"], "WRITE: MSG FROM : "+node.server_port+" : "+" KRISH "+";")

	time.Sleep(5 * time.Second)

	node.SendMessage(node.all_conn["80"], "READ:::;")
	node.SendMessage(node.all_conn["81"], "READ:::;")
	node.SendMessage(node.all_conn["82"], "READ:::;")

	time.Sleep(5 * time.Second)
	go node.SendMessage(node.all_conn["81"], "WRITE: MSG FROM : "+node.server_port+" : "+" ARPIT "+";")
	time.Sleep(10 * time.Second)

	node.SendMessage(node.all_conn["80"], "READ:::;")
	node.SendMessage(node.all_conn["81"], "READ:::;")
	node.SendMessage(node.all_conn["82"], "READ:::;")

	time.Sleep(5 * time.Second)
	go node.SendMessage(node.all_conn["82"], "WRITE: MSG FROM : "+node.server_port+" : "+" ANIME "+";")
	time.Sleep(5 * time.Second)

	node.SendMessage(node.all_conn["80"], "READ:::;")
	node.SendMessage(node.all_conn["81"], "READ:::;")
	node.SendMessage(node.all_conn["82"], "READ:::;")
	wg.Wait()
}

//TODO: Since acks can be in out of order. i.e. the ack of diff processes come in  diff orders.

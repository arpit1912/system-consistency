package main

import (
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	SERVER_HOST = "localhost"
	SERVER_TYPE = "tcp"
)

type Node struct {
	server_port           string
	primary_port          string
	mu                    sync.Mutex
	all_conn              map[string]net.Conn
	data                  string
	reciever__port        string
	local_sequence_number int
	message_queue         map[int]string
}

// func (node *Node) ClockIndex(port string) int {
// 	val, _ := strconv.Atoi(port)
// 	return val - 80
// }

func (node *Node) RecieveMessage(wg *sync.WaitGroup, port string) {
	defer wg.Done()
	fmt.Println("Searching for available port...")
	conn, err := net.Listen(SERVER_TYPE, SERVER_HOST+":"+port)

	if err != nil {
		fmt.Println(port, " is not available to listen ")
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Println("Listening on " + SERVER_HOST + ":" + port)

	for {
		client_conn, err := conn.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
			os.Exit(1)
		}
		fmt.Println("Client connected")
		buffer := make([]byte, 1024)
		mLen, err := client_conn.Read(buffer)
		if err != nil {
			fmt.Println("Error reading:", err.Error())
		}
		id := string(buffer[:mLen])
		fmt.Println("Received Connection Request From ", id)
		node.mu.Lock()
		node.all_conn[id] = client_conn
		node.mu.Unlock()
		go node.listenClient(client_conn, id)

	}

}

func delayAgent(min int, max int) {
	r := rand.Intn(max-min) + min
	time.Sleep(time.Duration(r) * time.Second)
}

func parsemessage(message string) (string, string, string) {
	result := strings.Split(message, ":")
	message_type := strings.Trim(result[0], " ")
	seq_num := strings.Trim(result[2], " ")
	new_val := strings.Trim(result[3], " ")
	return message_type, seq_num, new_val
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

		// msg := "WRITE: MSG FROM : " + port + ": <new-val> ;"
		messages := strings.Split(string(buffer[:mLen]), ";")
		for _, message := range messages {
			if message == "" {
				continue
			}
			fmt.Println("MESSAGE RECEIVED: ", message)
			message_type, seq_num_str, new_val := parsemessage(message)
			if message_type == "WRITE" {
				node.reciever__port = id
				//TODO: What if multiple WRITE, store receiver port in map
				node.SendMessage(node.all_conn[node.primary_port], message)
			} else if message_type == "UPDATE" {
				seq_num, _ := strconv.Atoi(seq_num_str)

				if node.local_sequence_number+1 == seq_num {
					fmt.Println("UPDATING to " + new_val)
					node.data = new_val
					node.local_sequence_number++
					msg := "ACK: SEQ_NUM: " + seq_num_str + ":;"
					node.SendMessage(node.all_conn[node.primary_port], msg)

					for {
						if data_val, found := node.message_queue[node.local_sequence_number+1]; found {
							node.data = data_val
							node.local_sequence_number++
							msg := "ACK: SEQ_NUM: " + strconv.Itoa(node.local_sequence_number) + ":;"
							node.SendMessage(node.all_conn[node.primary_port], msg)
							delete(node.message_queue, node.local_sequence_number)
						} else {
							break
						}
					}
				} else {
					fmt.Println("Storing in message queue!")
					node.message_queue[seq_num] = new_val
				}
			} else if message_type == "ACK-UPDATE" {
				node.SendMessage(node.all_conn[node.reciever__port], message)
			} else if message_type == "READ" {
				msg := "READ-DONE:::" + node.data + ";"
				node.SendMessage(node.all_conn[id], msg)
			}

		}
	}

}

func (node *Node) SendMessage(conn net.Conn, message string) {
	// delayAgent(10, 15)
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
	node := Node{all_conn: make(map[string]net.Conn), server_port: os.Args[1], primary_port: os.Args[2], local_sequence_number: 0, message_queue: make(map[int]string)}
	go node.RecieveMessage(&wg, os.Args[1])
	node.establishConnections(&wg, os.Args[3:], os.Args[1])
	wg.Wait()
}

// go run replica.go 8081 8080 8080 8082 8083...
// go run replica.go <port_no> <primary_port> <all other server ports>

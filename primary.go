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
	server_port    string
	mu             sync.Mutex
	seq_number     int
	all_conn       map[string]net.Conn
	data           string
	ack_recieved   map[string]int
	total_replicas int
	reciever__port map[string]string
	replica_ports  []string
	fp             *os.File
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
	seq_num_str := strings.Trim(result[2], " ")
	new_val := strings.Trim(result[3], " ")
	return message_type, new_val, seq_num_str
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
			//fmt.Println("MESSAGE RECEIVED: ", message)
			message_type, new_val, _ := parsemessage(message)
			if message_type == "WRITE" {
				fmt.Fprintln(node.fp, "Received WRITE from PORT : ", id, " Value : ", new_val)
				node.data = new_val
				node.seq_number++
				msg := "UPDATE: Seq number: " + strconv.Itoa(node.seq_number) + " : " + new_val + " ;"
				node.reciever__port[strconv.Itoa(node.seq_number)] = id
				node.BroadCastMessage(msg)
			} else if message_type == "ACK" {
				_, _, seq_num_str := parsemessage(message)

				fmt.Fprintln(node.fp, "Received ACK from PORT : ", id, " for message with seq_num : ", seq_num_str)

				node.mu.Lock()
				node.ack_recieved[seq_num_str]++
				node.mu.Unlock()

				for seq_num, ack_num := range node.ack_recieved {

					if node.total_replicas == ack_num {
						fmt.Fprintln(node.fp, "Received ACK from all Replicas for seq_num : ", seq_num_str, " and data value : ", node.data, ".Sending the ACK-UPDATE to Port No: ", node.reciever__port[seq_num_str])
						msg := "ACK-UPDATE:::" + node.data + ";"
						node.SendMessage(node.all_conn[node.reciever__port[seq_num_str]], msg)
						node.ack_recieved[seq_num] = 0
						//TODO: Reset the ack_received value
					}
				}
			} else if message_type == "READ" {
				fmt.Fprintln(node.fp, "Received READ from PORT : ", id, " . Returning value : ", node.data)
				msg := "READ-DONE:::" + node.data + ";"
				node.SendMessage(node.all_conn[id], msg)
			}

		}
	}

}

func (node *Node) BroadCastMessage(message string) {
	fmt.Fprintln(node.fp, "Broadcasting to replicas the message: ", message)
	for _, port := range node.replica_ports {
		// fmt.Println("Sending Message to - ", port, "MESSAGE : ", message)
		go node.SendMessage(node.all_conn[port], message)
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
	tot_replicas, _ := strconv.Atoi(os.Args[2])
	fp, _ := os.Create(os.Args[1] + ".txt")
	node := Node{all_conn: make(map[string]net.Conn), server_port: os.Args[1], total_replicas: tot_replicas, ack_recieved: make(map[string]int), reciever__port: make(map[string]string), replica_ports: os.Args[3:], fp: fp}
	go node.RecieveMessage(&wg, os.Args[1])
	node.establishConnections(&wg, os.Args[3:], os.Args[1])
	wg.Wait()
}

// go run primary.go 8080 2 8081 8082
// go run primary.go <port_no> <total_replicas> <all replica ports>

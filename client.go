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
	client_port  string
	mu           sync.Mutex
	all_conn     map[string]net.Conn
	server_ports []string
	fp           *os.File
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
			message_type, new_val := parsemessage(message)
			if message_type == "ACK-UPDATE" {
				fmt.Fprintln(node.fp, "UPDATED THE DATA IN ALL SERVERS: "+new_val)
			} else if message_type == "READ-DONE" {
				fmt.Fprintln(node.fp, "READ VALUE : ", new_val)
			}
		}
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

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func generate_random_string() string {
	b := make([]byte, 7)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

func main() {
	rand.Seed(time.Now().Unix())
	var wg sync.WaitGroup
	wg.Add(1)
	prob, _ := strconv.ParseFloat(os.Args[2], 32)
	fp, _ := os.Create(os.Args[1] + ".txt")
	node := Node{all_conn: make(map[string]net.Conn), client_port: os.Args[1], server_ports: os.Args[3:], fp: fp}
	defer fp.Close()
	node.establishConnections(&wg, node.server_ports, os.Args[1])

	for i := 0; i < 8; i++ {
		if rand.Float64() < prob {
			//WRITE
			writing_port := node.server_ports[rand.Intn(len(node.server_ports))]
			writing_val := generate_random_string()
			fmt.Fprintln(node.fp, "Going to write : ", writing_val, "- at PORT : ", writing_port)
			go node.SendMessage(node.all_conn[writing_port], "WRITE: MSG FROM : "+node.client_port+" : "+writing_val+";")

		} else {
			sending_port := node.server_ports[rand.Intn(len(node.server_ports))]
			fmt.Fprintln(node.fp, "Going to read from PORT :", sending_port)
			go node.SendMessage(node.all_conn[sending_port], "READ:::;")
		}
		time.Sleep(5 * time.Second)
	}
	wg.Wait()
}

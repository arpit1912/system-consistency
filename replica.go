package main
import (
	"fmt"
	"net"
	"sync"
	"os"
	"time"
	"math/rand"
	"strings"
	"strconv"
)
const (
        SERVER_HOST = "localhost"
        SERVER_TYPE = "tcp"
)

type Node struct {
	server_port string
	primary_port string
	mu sync.Mutex
	all_conn map[string] net.Conn
	data string
	reciever__port string
	local_sequence_number int
	message_queue map[int] string
}
// func (node *Node) ClockIndex(port string) int {
// 	val, _ := strconv.Atoi(port)
// 	return val - 80
// }

func (node *Node) RecieveMessage (wg *sync.WaitGroup, port string) {
	defer wg.Done()
	fmt.Println("Searching for available port...")
	conn, err := net.Listen(SERVER_TYPE, SERVER_HOST + ":" + port)

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
		id:= string(buffer[:mLen])
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

func parsemessage(message string) (string,string, string) {
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
				delete(node.all_conn, id);
				break
		}
		
		// msg := "REQUEST: MSG FROM : " + port + ": <new-val> ;"
		messages := strings.Split(string(buffer[:mLen]),";")
		for _,message := range(messages) {
			message_type, seq_num_str, new_val := parsemessage(message)
			fmt.Println(seq_num_str)
			if message_type == "REQUEST" {
				node.reciever__port = id
				node.SendMessage(node.all_conn[node.primary_port], message)
			} else if message_type == "UPDATE" {
				seq_num, _ := strconv.Atoi(seq_num_str)
				 
				if node.local_sequence_number + 1 == seq_num {
					fmt.Println("UPDATING to " + new_val)
					node.data = new_val 
					node.local_sequence_number++
					msg := "ACK: SEQ_NUM: " + seq_num_str + ";"
					node.SendMessage(node.all_conn[node.primary_port], msg)

					for {
						if data_val, found := node.message_queue[node.local_sequence_number + 1]; found {
							node.data = data_val
							node.local_sequence_number++
							msg := "ACK: SEQ_NUM: " + strconv.Itoa(node.local_sequence_number) + ";"
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
				node.SendMessage(node.all_conn[node.reciever__port], node.data)
			}
			
			
		}
	}
	
}

func (node *Node) SendMessage(conn net.Conn, message string) {
	delayAgent(10,15)
	_, err := conn.Write([]byte(message))
	if err != nil {
		panic("Error sending message ;( ")
	}
}
func main() {
	var wg sync.WaitGroup
	wg.Add(1)
	node := Node{all_conn : make(map[string] net.Conn),  server_port : os.Args[1], primary_port: os.Args[2], local_sequence_number : 0, message_queue: make(map[int] string)}
	go node.RecieveMessage(&wg, os.Args[1])
	wg.Wait()
}
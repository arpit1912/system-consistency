package main
import (
	"fmt"
	"net"
	"sync"
	"os"
	"time"
	"math/rand"
	"strconv"
)
const (
        SERVER_HOST = "localhost"
        SERVER_TYPE = "tcp"
)

type Node struct {
	server_port string
	mu sync.Mutex
	seq_number int
	all_conn map[string] net.Conn
	data string
	ack_recieved int
	total_replicas int
	reciever__port string
}
func (node *Node) ClockIndex(port string) int {
	val, _ := strconv.Atoi(port)
	return val - 80
}

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

func parsemessage(message string) (string,string) {
	result := strings.Split(message, ":")
	message_type = strings.Trim(result[0], " ")
	new_val := strings.Trim(result[3], " ")
	return message_type, new_val
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
		
		// msg := "PREPARE: MSG FROM : " + port + ": <new-val> ;"
		messages := strings.Split(string(buffer[:mLen]),";")
		for _,message := range(messages) {
			node.seq_number++;
			type, new_val := parsemessage(message)
			if type == "PREPARE" {
				node.data = new_val
				msg := "UPDATE: Seq number: " + strconv.Itoa(node.seq_number) + " : " + message + " ;"
				node.reciever__port = id
				node.BroadCastMessage(msg)	
			} else if type == "ACK" {
				node.mu.Lock()
				node.ack_recieved++;
				node.mu.UnLock()
				if total_replicas == ack_recieved {
					msg := "ACK-UPDATE;"
					node.SendMessage(node.all_conn[node.reciever__port], msg)
				}
			}
			
			
		}
	}
	
}

func (node *Node) BroadCastMessage(message string) {
	fmt.Println("broadcasting to replicas")
	for port, conn := range node.all_conn {
		fmt.Println("Sending Message to - " , port, " Seq_Number: ", node.seq_number)
		go node.SendMessage(conn, message)
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
	node := Node{all_conn : make(map[string] net.Conn),  server_port : os.Args[1]}
	go node.RecieveMessage(&wg, os.Args[1])
	wg.Wait()
}
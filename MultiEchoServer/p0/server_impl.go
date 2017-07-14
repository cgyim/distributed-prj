
// Implementation of a MultiEchoServer. Students should write their code in this file.
//cgyimustc@gmail.com
package p0

import (
	"fmt"
	"strconv"
	"bufio"
	"net"
	"io"
)

type multiEchoServer struct {
	// TODO: implement this!
	listener *net.Listener
	count int


}

// New creates and returns (but does not start) a new MultiEchoServer.
func New() MultiEchoServer {
	// TODO: implement this!
	var new_server *multiEchoServer = new(multiEchoServer)
	//listener := new(net.Listener)
	//new_server.listener2chan = make(map[net.Conn]*chan string)
	//new_server.listener = listener
	new_server.count = 0
	new_server.listener = nil
	return new_server
}

func (mes *multiEchoServer) Start(port int) error {
	// TODO: implement this!
	l, err := net.Listen("tcp","localhost:"+ strconv.Itoa(port))
	listener2chan := make(map[net.Conn]*chan string)
	mes.listener = &l
        if err != nil {
		return err
	}else {
		for {   //per connection to client
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("ACCEPT ERROR,server may down!")
				return err
			}
			mes.count += 1
			fmt.Printf("New Connection Found, current connections: %d\n",mes.count)
			rw := bufio.NewReader(bufio.NewReader(conn))
			message_100queue := make(chan string ,100)
			listener2chan[conn]= &message_100queue
			go func(){   //write to client
				for {
					_, err := io.WriteString(conn,<- message_100queue)
					if err != nil {
						fmt.Println("error write")
						continue
					}

				}
			}()
			go func(){          //per connection read a line  from client and send to all channels that registered
				defer func(){
					mes.count -= 1
					fmt.Printf("Connection Disconnect....Current Connections : %d\n", mes.count)
				}()
				defer conn.Close()
				defer delete(listener2chan,conn)
				for {       //read all lines
					b , _,err := rw.ReadLine()  //data ready to read
					if err != nil {
						if err == io.EOF{
							break
						}
						fmt.Println("can not read this line")
						continue
					}else{
						for _,chan_ptr := range listener2chan {
							if len(*chan_ptr) == 100 {
								continue
							} else {
								*chan_ptr <- string(b)
							}
						}
					}
				}

			}()
		}
	}
}
func (mes *multiEchoServer) Close() {
	// TODO: implement this!
	listener := *mes.listener
	err := listener.Close()
	if err != nil {
		panic("Cannot close listening port")
	}
}
func (mes *multiEchoServer) Count() int {
	// TODO: implement this!
	return mes.count
}


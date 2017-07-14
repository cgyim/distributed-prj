package main


import (
	"io"
	"log"
	"net"
	"os"
	//"bufio"
	//"fmt"
)
func mustcopy(dst io.Writer,src io.Reader) {
	if _, err := io.Copy(dst, src); err != nil {
		log.Fatal(err)
	}
}



func main(){
	conn, err := net.Dial("tcp","localhost:9999")
	if err != nil {
		log.Fatal(err)

	}

	defer conn.Close()
	go mustcopy(os.Stdout,conn)
	io.Copy(conn,os.Stdin)

}
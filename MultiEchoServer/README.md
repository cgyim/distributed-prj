
# MultiEchoServer
MultiEchoServer simulated group chat, support client slow read , a try of github.com/cmu440/p0
# Details
1.Deploy a map[conn.net]\*chan string to record current connections and 100 strings buffered channels for each connections.
###
2.No public channels to restore messages received from all clients, instead, each client connected and directly write into all connection channels, 2 goroutines to handle each connections. One for read strings from buffered channels and write to client, the other is readline from connection and write to all buffered channels(the number of channels equals the current number of connections),so that each client can get all messages from all the client.
###
#3.The method to check if a client is slow, seems ugly, to check if the channel is full , if the channel is full, current message is discard. No built-in "select" used. 
###
# Usage 

`cd $GOPATH/src/github.com/`
###
`mkdir -p $GOPATH/src/github.com/cgyim1992`
###
`cd $GOPATH/src/github.com/cgyim1992`
###
`git clone https://github.com/cgyim1992/distributed-prj`
###
`go build distributed-prj/MultiEchoServer/srunner.go `
###
`go build distributed-prj/MultiEchoServer/crunner.go `
###
`distributed-prj/MultiEchoServer/srunner`
###
`distributed-prj/MultiEchoServer/crunner`

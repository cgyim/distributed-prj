package main


import (
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/mysql"
	//"github.com/parnurzeal/gorequest"
	"github.com/siddontang/go-mysql/client"
	"os"
	"context"
	"strings"
	"strconv"
	"net"
	//"fmt"
)
// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
// flavor is mysql or mariadb
const (
	RemoteServerAddr string = "http://localhost:9997"
)
type PostData struct {
	File string
	DateTime string
	BinLogRawData []byte
}


func main() {

	var (
		Current_FILE *os.File
		Current_Handling_BinFile string
	)

	//raw_byte_chan := make(chan PostData,100)

	cfg := replication.BinlogSyncerConfig{
		ServerID: 1,
		Flavor:   "mysql",
		Host:     "127.0.0.1",
		Port:     3306,
		User:     "root",
		Password: "",
	}

	syncer := replication.NewBinlogSyncer(&cfg)
	//todo rq := gorequest.New()

	//read current binlog ,if there is newer binlog  ,first backup the remain part of origin binlog
	//then ,backup all the binlog from origin to latest binlog ,and individually write to different binlogs
	//accordingly,through http send file



        conn , err := client.Connect("127.0.0.1:3306","root","","test")
        if err != nil {
		panic("Can not Connect to DB!")
	}
	r , _ := conn.Execute(`select latest_url from latest where project_id = 2`)
	last_position, _ := r.GetStringByName(0,"latest_url")
        last_file , last_pos := strings.Split(last_position,"/")[0],strings.Split(last_position,"/")[1]
	pos , _ := strconv.ParseUint(last_pos,10,32)
	// Start sync with specified binlog file and position
	//todo StartToRead_Bin_File := last_file
	streamer, err := syncer.StartSync(mysql.Position{last_file, uint32(pos)})

	if err != nil {
		//record  latest position , origin binlog file ,and timestamp into latest;
		//example:     mysql-bin.000001/1546}
		if err, ok := err.(net.Error); ok && err.Timeout(){
			 panic("I/O Timeout!")
		}
	}
	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"

	for {
		ev, _ := streamer.GetEvent(context.Background())

		if ev.Header.EventType == replication.ROTATE_EVENT {
			Rotate_Bin_File := string(ev.Event.(*replication.RotateEvent).NextLogName)

			//	if rotate bin log is a new binlog ,newer than current binlog(which is the latest
			//	 bin log that server receive)
			if  Rotate_Bin_File > Current_Handling_BinFile  {
				Current_Handling_BinFile = Rotate_Bin_File
				if Current_FILE != nil {
					Current_FILE.Close()
				}
				New_FILE , err := os.Create("/home/vagrant/"+Rotate_Bin_File)
				if err != nil{
					os.Exit(1)
				}
				Current_FILE = New_FILE
			}
		}
		Current_FILE.Write(ev.RawData)
		//current_pos := ev.Header.LogPos
		//fmt.Println("current event: ",Current_Bin_File,"====>",current_pos)
		//resp , _ , httperr := rq.Post(RemoteServerAddr).
		//Type("multipart").
		//SendStruct(PostData{DefaultBinLog+'/'+})
		//fmt.Print(ev.RawData)
		ev.Dump(os.Stdout)
	}
}



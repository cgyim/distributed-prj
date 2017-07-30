package main



//this is for test in local system



/*
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
	Size int
	LogPos  int
	FileName string
}

//func SendToServer(ev *replication.BinlogEvent) error {
//
//
//
//}

func main() {

	var (
		Current_FILE *os.File
		Current_Handling_BinFile string
		Rotate_Bin_File string
		//Delivered  int
		//Failed int

	)
	//rotate event occupy first 4 bytes in a new binlog file
	// its format is fixed in mysql-server 5.5.44 on ubuntu 14.04.1-log and constant.
	//if your mysql-server version is different from this ,the bytes could be different.

	RotateEvent4Bytes := []byte{0xfe,0x62,0x69,0x6e}
	BinLogStillWrittenSwitch := true
	//channel to send data
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1,
		Flavor:   "mysql",
		Host:     "192.168.33.11",
		Port:     3306,
		User:     "root",
		Password: "",
	}

	syncer := replication.NewBinlogSyncer(&cfg)
	//rq := gorequest.New()
	conn , err := client.Connect("192.168.33.11:3306","root","","test")
        if err != nil {
		panic("Can not Connect to DB!")
	}
	r , _ := conn.Execute(`select latest_url from latest where project_id = 2`)

	last_position, _ := r.GetStringByName(0,"latest_url")
        last_file , last_pos := strings.Split(last_position,"/")[0],strings.Split(last_position,"/")[1]
	pos , _ := strconv.ParseUint(last_pos,10,32)

	// Start sync with specified binlog file and position
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
        LOOP:
	for {
		ev, _ := streamer.GetEvent(context.Background())
		ev.Dump(os.Stdout)
		if ev.Header.EventType == replication.ROTATE_EVENT  {
			if ev.Header.LogPos != uint32(0) && ev.Header.Timestamp > uint32(0){ //not a truly new file
				Current_FILE.Write(ev.RawData)
			//	err := SendToServer(ev)
			//	if err != nil {
			//		Failed += 1
			//	}else {
			//		Delivered += 1
			//	}
				continue LOOP
			}
			Rotate_Bin_File = string(ev.Event.(*replication.RotateEvent).NextLogName)
			//	if rotate bin log is a new binlog ,newer than current binlog(which is the latest
			//	 bin log that server receive)
			if  Rotate_Bin_File > Current_Handling_BinFile  {   //new file found
				BinLogStillWrittenSwitch = true
				Current_Handling_BinFile = Rotate_Bin_File
				if Current_FILE != nil {
					Current_FILE.Close()
				}
				New_FILE , err := os.Create("/home/vagrant/"+Rotate_Bin_File)
				if err != nil{
					os.Exit(1)
				}
				Current_FILE = New_FILE
				Current_FILE.Write(RotateEvent4Bytes)
			}else{     //not a new file but a rotate event, all next message discard
				BinLogStillWrittenSwitch = false
			}
			continue LOOP

		}
		if BinLogStillWrittenSwitch {
			Current_FILE.Write(ev.RawData)

			//todo add ev.Header.EventSize
			//todo add ev.Header.LogPos
		}
		//current_pos := ev.Header.LogPos
		//fmt.Println("current event: ",Current_Bin_File,"====>",current_pos)
		//resp , _ , httperr := rq.Post(RemoteServerAddr).
		//Type("multipart").
		//SendStruct(PostData{DefaultBinLog+'/'+})
		//fmt.Print(ev.RawData)

	}
}



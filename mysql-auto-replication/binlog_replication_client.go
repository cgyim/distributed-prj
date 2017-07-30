package main


import (
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/parnurzeal/gorequest"
	"github.com/siddontang/go-mysql/client"
	"os"
	"context"
	"strings"
	"strconv"
	"net"
	"time"
	"fmt"
)
// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
// flavor is mysql or mariadb
const (
	RemoteServerAddr string = "http://localhost:9997"
	HttpRetryTime = 3
	HttpRetryInterval = 1 * time.Second

)

var RotateEvent4Bytes []byte = []byte{0xfe,0x62,0x69,0x6e}

type PostData struct {
	LogPos  uint32
	FileName string
}
type Counter struct {
	Delivered int
	Failed int
}

func (c *Counter)WriteDelieverdInfo(conn *client.Conn) error{
	datestr := time.Now().Format("2006-01-02")
	_ , err := conn.Execute("update stat set delivered = ?, failed = ?, date = ? where id = 2",c.Delivered,c.Failed,datestr)
	if err != nil{
		return err
	}
	return nil
}

func SendToServer(logpos uint32,current_file string, raw *[]byte) []error {
	rq := gorequest.New()
	_, _, httperr := rq.Post(RemoteServerAddr).
		Type("multipart").
	//Send(`{"url": "` + ori_url + `"}`).
	//Send(`{"file": "` + file + `"}`).
	//Send(`{"commit_time": "` + commit_time + `"}`).
		SendStruct(PostData{logpos,current_file}).
	//Send(data).Set("json_fieldname", `raw`).
		SendFile(*raw, "", "raw").
		Retry(HttpRetryTime, HttpRetryInterval).
		End()

	if httperr != nil {
		return httperr
	}
	return nil
}
func GetLastTimePosition() (*client.Conn, error ,string, uint64){
	conn , err := client.Connect("192.168.33.11:3306","root","","test")
	if err != nil {
		panic("Can not Connect to DB!")
	}
	r , _ := conn.Execute(`select latest_url from latest where project_id = 2`)
	last_position, _ := r.GetStringByName(0,"latest_url")
	last_file , last_pos := strings.Split(last_position,"/")[0],strings.Split(last_position,"/")[1]
	pos , _ := strconv.ParseUint(last_pos,10,32)
	return conn, nil , last_file , pos
}


func (c *Counter)SendEventData(streamer *replication.BinlogStreamer, last_file string) {
	Current_Handling_BinFile := last_file
	Rotate_Bin_File  := ""
	ROTATE_FORMAT_TO_NEW_FILE := true
LOOP:
	for {
		ev, _ := streamer.GetEvent(context.Background())
		ev.Dump(os.Stdout)
		if ev.Header.EventType == replication.ROTATE_EVENT  {
			if ev.Header.LogPos != uint32(0) && ev.Header.Timestamp > uint32(0){ //not a truly new file
				//Current_FILE.Write(ev.RawData)
				err := SendToServer(ev.Header.LogPos,Current_Handling_BinFile,&ev.RawData)
				if err != nil {
					c.Failed += 1
				}else {
					c.Delivered += 1
				}
				continue LOOP
			}
			Rotate_Bin_File = string(ev.Event.(*replication.RotateEvent).NextLogName)
			//	if rotate bin log is a new binlog ,newer than current binlog(which is the latest
			//	 bin log that server receive)
			if  Rotate_Bin_File > Current_Handling_BinFile  {   //new file found
				ROTATE_FORMAT_TO_NEW_FILE = true
				Current_Handling_BinFile = Rotate_Bin_File
				//Current_FILE.Write(RotateEvent4Bytes)
				SendToServer(uint32(4),Current_Handling_BinFile,&RotateEvent4Bytes)
			}else{     //not a new file but a rotate event, all next message
				// except operation events should be discard
				ROTATE_FORMAT_TO_NEW_FILE = false
			}
			continue LOOP

		}
		if ev.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
			err := SendToServer(ev.Header.LogPos,Current_Handling_BinFile,&ev.RawData)
			if err != nil {
				c.Failed += 1
			}else {
				c.Delivered += 1
			}
			ROTATE_FORMAT_TO_NEW_FILE = true
		}else {
			if ROTATE_FORMAT_TO_NEW_FILE {
				err := SendToServer(ev.Header.LogPos,Current_Handling_BinFile,&ev.RawData)
				if err != nil {
					c.Failed += 1
				}else {
					c.Delivered += 1
				}
			}
			continue LOOP
		}


	}






}
func main() {


	//rotate event occupy first 4 bytes in a new binlog file
	// its format is fixed in mysql-server 5.5.44 on ubuntu 14.04.1-log and constant.
	//if your mysql-server version is different from this ,the bytes could be different.
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1,
		Flavor:   "mysql",
		Host:     "192.168.33.11",
		Port:     3306,
		User:     "root",
		Password: "",
	}
	ticker := time.NewTicker(time.Second * 15)
	c := new(Counter)
	syncer := replication.NewBinlogSyncer(&cfg)
	conn , _ , last_file, pos := GetLastTimePosition()
	go func(){
		for tick:= range ticker.C{
			err := c.WriteDelieverdInfo(conn)
			if err != nil{
				fmt.Println("Connection Issue, Can not Write Delivered Stat!  @  ", tick)
			}
		}
	}()
	// Start sync with specified binlog file and position

	//record  latest position , origin binlog file ,and timestamp into latest;
	//example:     mysql-bin.000001/1546}
	streamer, err := syncer.StartSync(mysql.Position{last_file, uint32(pos)})

	if err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout(){
			panic("I/O Timeout!")
		}
	}
	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"
	c.SendEventData(streamer,last_file)
}



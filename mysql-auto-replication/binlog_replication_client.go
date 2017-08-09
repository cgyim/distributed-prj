package main


import (
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/parnurzeal/gorequest"
	"github.com/siddontang/go-mysql/client"
	"context"
	"strings"
	"strconv"
	"net"
	"time"
	"fmt"
	"os"
)
// Create a binlog syncer with a unique server id, the server id must be different from other MySQL's.
// flavor is mysql or mariadb
const (
	RemoteServerAddr string = "http://localhost:8088/v1/deliver/mysql/"
	HttpRetryTime = 3
	HttpRetryInterval = 1 * time.Second

)

var (
	RotateEvent4Bytes []byte = []byte{0xfe,0x62,0x69,0x6e}
	NEW_FILE_FLAG_START bool  = false
)


type PostData struct {
	LogPos  uint32  `json:"size"`
	FileName string   `json:"file"`   //mysql-bin.000001
	CommitTime  string `json:"commit_time"`
	Origin     string  `json:"url"`   //mysql-bin.000001/0
}


func SendToServer(logpos uint32,current_file string,raw *[]byte) []error {
	//ct := strconv.FormatUint(uint64(commit_time),10)
	//ct := time.Unix(int64(commit_time),0).Format("2006-01-02 15:04:05")
	ct := time.Now().Format("2006-01-02 15:04:05")
	logpos_str := strconv.FormatUint(uint64(logpos),10)
	rq := gorequest.New()
	_, _, httperr := rq.Post(RemoteServerAddr).
		Type("multipart").
	//Send(`{"url": "` + ori_url + `"}`).
	//Send(`{"file": "` + file + `"}`).
	//Send(`{"commit_time": "` + commit_time + `"}`).
		SendStruct(PostData{logpos,current_file,ct,current_file + "/" +logpos_str}).
	//Send(data).Set("json_fieldname", `raw`).
		SendFile(*raw, "", "raw").
		Retry(HttpRetryTime, HttpRetryInterval).
		End()
	if httperr != nil {
		return httperr
	}
	return nil
}
func GetLastTimePosition() (error ,string, uint64){
	conn , err := client.Connect("192.168.33.11:3306","root","","cn_archive")
	if err != nil {
		panic("Can not Connect to DB!")
	}
	r , _ := conn.Execute(`select latest_url from latest where project_id = 2`)
	last_file , last_pos := "" , "0"
	last_position, _ := r.GetStringByName(0,"latest_url")
	if last_position != "" {
		last_file, last_pos = strings.Split(last_position, "/")[0], strings.Split(last_position, "/")[1]
	}
	if last_pos == "0" {
		NEW_FILE_FLAG_START = true
		fmt.Println("NEW_FILE START IN POSITION 0")
	}
	pos , _ := strconv.ParseUint(last_pos,10,32)
	return  nil , last_file , pos
}


func SendEventData(streamer *replication.BinlogStreamer, last_file string) {
	count := 0

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
				SendToServer(ev.Header.LogPos,Current_Handling_BinFile,&ev.RawData)
				count += 1
				fmt.Println("=========>", count)
				continue LOOP
			}
			Rotate_Bin_File = string(ev.Event.(*replication.RotateEvent).NextLogName)
			//	if rotate bin log is a new binlog ,newer than current binlog(which is the latest
			//	 bin log that server receive)
			if  Rotate_Bin_File > Current_Handling_BinFile || NEW_FILE_FLAG_START {   //new file found
				ROTATE_FORMAT_TO_NEW_FILE = true
				Current_Handling_BinFile = Rotate_Bin_File
				//Current_FILE.Write(RotateEvent4Bytes)
				SendToServer(uint32(4),Current_Handling_BinFile,&RotateEvent4Bytes)
				count += 1
				fmt.Println("=========>", count)
			}else{     //not a new file but a rotate event, all next message
				// except operation events should be discard
				ROTATE_FORMAT_TO_NEW_FILE = false
			}
			continue LOOP

		}
		if ev.Header.EventType != replication.FORMAT_DESCRIPTION_EVENT {
			SendToServer(ev.Header.LogPos,Current_Handling_BinFile,&ev.RawData)
			count += 1
			fmt.Println("=========>", count)
			ROTATE_FORMAT_TO_NEW_FILE = true
		}else {
			if ROTATE_FORMAT_TO_NEW_FILE {
				SendToServer(ev.Header.LogPos,Current_Handling_BinFile,&ev.RawData)
				count += 1
				fmt.Println("=========>", count)
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
		Host:     "192.168.33.10",
		Port:     3306,
		User:     "root",
		Password: "",
	}
	syncer := replication.NewBinlogSyncer(&cfg)
	_ , last_file, pos := GetLastTimePosition()
	// Start sync with specified binlog file and position

	//record  latest position , origin binlog file ,and timestamp into latest;
	//example:     mysql-bin.000001/1546}
	streamer, err := syncer.StartSync(mysql.Position{Name:last_file, Pos:uint32(pos)})

	if err != nil {
		if err, ok := err.(net.Error); ok && err.Timeout(){
			panic("I/O Timeout!")
		}
	}
	// or you can start a gtid replication like
	// streamer, _ := syncer.StartSyncGTID(gtidSet)
	// the mysql GTID set likes this "de278ad0-2106-11e4-9f8e-6edd0ca20947:1-2"
	// the mariadb GTID set likes this "0-1-100"
	SendEventData(streamer,last_file)
}



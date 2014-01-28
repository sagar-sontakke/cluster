/*
 * Written by: Sagar Sontakke
 * Description: This is a clustering library that provide some abstractions for the setting up clusters.
 * It provides functions related to parsing input config file, setting up a server, input/output message
 * channels for the server, send/receive message functions, structure for server/mesage objects
 */

package cluster

import (
	zmq "github.com/pebbe/zmq4"
	"fmt"
	"os"
	"time"
	"strconv"
	"bufio"
	"strings"
)

var Peers []int
var msgCounter int64 = 1
var PeerAddress = make(map[int]string)

const(
	REQUEST_TIMEOUT = 1000 * time.Millisecond
	MAX_RETRIES	= 3
	BROADCAST	= -1
)

/*
 * The structure to encapsulate the message, contains:
 * 1. Node is of receipient 2. Unique ID for the message 3. Actual message
 */

type Envelope struct {
	Pid int
	MsgId int64
	Msg string
}

/*
 * Structure for server object. Contains the server specific info like ID of that server,
 * server ip/port address, IDs and address of peer servers, message input/output channels
 * and message ID counts.
 */

type Servernode struct {
	ServerId int
	ServerAddress string
	PeersId []int
	PeerServers map[int]string
	Serveroutchannel chan *Envelope
	Serverinchannel chan *Envelope
	MessageId int64	
}

/*
 * The server object impliments the Server interface
 */

type Server interface {
	GetPid() int
	GetPeers() []int
	Outbox() chan *Envelope
	Inbox() chan *Envelope
}

/* Get PID of the server */

func (s Servernode) GetPid() int {
	return s.ServerId
}

/* Get IDs of the peer nodes */

func (s Servernode) GetPeers() []int {
	return s.PeersId
}

/* Set ID of the server */

func SetOwnId(s Servernode, i int) {
	s.ServerId = i
	return
}

/* Set a new server object and return a pointer to it */

func New(nodeid int) *Servernode{
	var s Servernode
	s.ServerId = nodeid
	s.ServerAddress = PeerAddress[nodeid]
	s.PeersId = Peers
	s.PeerServers = PeerAddress
	s.Serveroutchannel = make(chan *Envelope)
	s.Serverinchannel = make(chan *Envelope)
	s.MessageId = int64(nodeid * 1000)
	return &s
}

/* Queue the Inbox channel for a server */

func QueueInbox(s Servernode, e Envelope, in chan *Envelope) {
	in <- &e
}

/* Return the Inbox channel for a server */

func (s Servernode) Inbox() chan *Envelope {
	return s.Serverinchannel
}

/* Return the Outbox channel for a server */

func (s Servernode) Outbox() chan *Envelope {
	return s.Serveroutchannel
}

/* Send messages that are queued in the outbox channel to the correspnding servers */

func SendMsgToPeers(s Servernode) {

	for {
		//time.Sleep(3000 * time.Millisecond)
		select {
			case msg, valid := <- s.Outbox():
				if valid == false {
					return
				} else {
					targetID := msg.Pid
					asClient, _ := zmq.NewSocket(zmq.REQ)
					asClient.Connect(s.PeerServers[targetID])
					asClient.SendMessage(msg)
					//fmt.Println("SENDING: (Src,Dst) --> (",s.ServerId,",",targetID,") (Message:",*msg,")")
				}
		}
	}
	return
}

/* Receive the messages that come from the peer servers and queue them to the inbox channel */

func RecMsgFromPeers(s *zmq.Socket, ser Servernode) {
	var e Envelope
	for {
		msg, err := s.RecvMessage(0)
		a1 := strings.Split(msg[0],"&{")
		a2 := strings.Split(a1[1]," ")
		pid,_ := strconv.Atoi(a2[0])
		mid,_ := strconv.Atoi(a2[1])
		msgid := int64(mid)
		text := ""

		for b := 2; b<len(a2); b++ {
			text = text +" "+ a2[b]
		}
		text1 := text[1:]
		msgtext := strings.Split(text1,"}")[0]

		e.Pid = pid
		e.MsgId = msgid
		e.Msg = msgtext
		go QueueInbox(ser,e,ser.Inbox())

		if err != nil {
			break
		}
		s.SendMessage(msg)
	}	
}

/* Parse the given input cluster config file and collect all teh data for the cluster formation */

func ParseConfig(filename string) (int) {
	fconf, _ := os.OpenFile(filename,os.O_RDONLY,0600)
	rdconf := bufio.NewReader(fconf)
	nline, err := rdconf.ReadString('\n')

	for {
		if err != nil {
			break
		}

		line := strings.Split(nline,"\n")

		if strings.Contains(nline,"#") == false && line[0] != "" {
			text := string(line[0])
			pair := strings.Split(text,",")
			id := strings.TrimSpace(string(pair[0]))
			address := strings.TrimSpace(string(pair[1]))

			nodeid, _ := strconv.Atoi(id)

			for k:=0; k<len(Peers);k++ {
				if Peers[k] == nodeid {
					fmt.Println(strconv.Itoa(nodeid)+": duplicate node ID. Node IDs should be unique")
					return 1
				}
				for _, value := range PeerAddress {
					if value == address {
					fmt.Println(address+": duplicate IP:port. IP:port should be unique for nodes")
						return 2
					}
				}
			}
			Peers = append(Peers,nodeid)
			PeerAddress[nodeid] = address
		}
		nline, err = rdconf.ReadString('\n')
	}
	return 0
}

/*
 * Written by: Sagar Sontakke
 * Description: This is a test script that tests the functionalities of cluster interface that is
 * provided. Here we run the multiple servers that are specified in the cluster-config file. Then we send
 * multiple messages among these servers and test whether they are getting correctly sent/received.
 */

package cluster

import (
	zmq "github.com/pebbe/zmq4"
	"fmt"
	"time"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
)

/*
 * Constant values for the test loops.
 *	1. RETRIES: How many times to retry the loop
 *	2. BROADCAST: Whether the messages are broadcasted or point-to-point
 *	3. MSG_DELAY: Depaly after every 10th message in milliseconds
 *	4. TIMEOUT: Timeout after n seconds
 */

const (
	RETRIES		= 20
	BROADCAST_MSGS	= false
	MSG_DELAY	= 500			/* in milliseconds */
	TIMEOUT		= 1000			/* in seconds */
	CONFIG_FILE	= "clusterconf.txt"	/* the cluster configuration file */
)

/* Variables to record message flows */

var sentlog = make(map[int]int)
var receivelog = make(map[int]int)
var totalsent int64 = 0
var totalreceived int64 = 0
var totalexpected int64 = -1
var WAIT_TILL int64

func Test_cluster(t *testing.T) {

	/*
	 * The ParseConfig() parses the given clusterconfig.txt file and gets the data in
	 */

	if ParseConfig(CONFIG_FILE) != 0 {
		return
	}

	var serv [50] *Servernode
	var server [50] *zmq.Socket


	/*
	 * Initialise all the server objects corresponding to those which are parsed from the config file
	 */

	for i:=0; i<len(Peers); i++ {
		serv[i] = New(Peers[i])
		SetOwnId(*serv[i],i+1)
		server[i], _ = zmq.NewSocket(zmq.REP)
		add := PeerAddress[serv[i].ServerId]
		server[i].Bind(add)
	}
	time.Sleep(0 * time.Second)

	if BROADCAST_MSGS == true {
		totalexpected = int64((RETRIES * len(Peers) * (len(Peers) - 1)))
	} else {
		totalexpected = int64(RETRIES * len(Peers))
	}

	WAIT_TILL = (int64(totalexpected / 10) * MSG_DELAY)
	WAIT_TILL = int64(WAIT_TILL / 1000) + TIMEOUT

	/*
	 * Run the go-routines for send/receive the messages
	 */

	for i:=0; i<len(Peers); i++ {
		go SendMsgToPeers(*serv[i])
		go RecMsgFromPeers(server[i], *serv[i])
		go checkInbox(*serv[i])
	}

	/*
	 * Loops for the test messages
	 */	

	for i:=0; i < RETRIES; i++ {
		for k:=0; k < len(Peers); k++ {
			pid := k
			if BROADCAST_MSGS == false {
				if k+1 >= len(Peers) {
					pid = Peers[0]
				} else {
					pid = Peers[k+1]
				}
			} else {
				pid = BROADCAST
			}

			msgid := serv[k].MessageId
			msgtext := strconv.Itoa(serv[k].ServerId)
			if pid == BROADCAST {
				for a:=0; a < len(Peers); a++ {
					pid = Peers[a]
					if pid != serv[k].ServerId {
						serv[k].Outbox() <- &Envelope{pid,msgid,msgtext}
						serv[k].MessageId = serv[k].MessageId + 1
						sentlog[pid] = sentlog[pid] + 1
						atomic.AddInt64(&totalsent, 1)
						//totalsent = totalsent + 1

						if (totalsent % 10) == 0 {
							time.Sleep(MSG_DELAY * time.Millisecond)
						}
					}
				}
			} else {
				if pid != serv[k].ServerId {
					serv[k].MessageId = serv[k].MessageId + 1
					serv[k].Outbox() <- &Envelope{pid,msgid,msgtext}

					sentlog[pid] = sentlog[pid] + 1
					atomic.AddInt64(&totalsent, 1)
					//totalsent = totalsent + 1

					if (totalsent % 10) == 0 {
						time.Sleep(MSG_DELAY * time.Millisecond)
					}
				}
			}
			
		}
	}


	/*
	 * Print the message send/receive status every n seconds
	 */

	for {
		select {
				case <- time.After(5 * time.Second):
					fmt.Println("sent:",totalsent,"received:",totalreceived," -- timeout after 1000 secs")
		}
	}
}

/*
 * Go routine that runs for the messages that are in Inbox channel
 */

func checkInbox(s Servernode) {
	for {
		select {
			case _ = <- s.Inbox():
				//fmt.Printf("RECEIVE: Pid --> %d,   (Message: %s)\n", env.Pid,env.Msg)
				receivelog[s.ServerId] = receivelog[s.ServerId] + 1
				atomic.AddInt64(&totalreceived, 1)
				//totalreceived = totalreceived + 1
				if (totalreceived == totalsent) && (totalreceived == totalexpected) {
					fmt.Println("PASS: Message details --> sent:",totalsent,"received:",totalreceived)
					os.Exit(0)
				}

			case <- time.After(time.Duration(WAIT_TILL) * time.Second):
				fmt.Println("FAIL: Timeout! Message details --> sent:",totalsent,"received:",totalreceived)
		}
	}
}

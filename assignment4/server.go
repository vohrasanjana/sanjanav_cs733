package main

import (
	"bufio"
	"fmt"
	"github.com/sanjana/CS733/assignment4/fs"
	"net"
	"os"
	"strconv"
	"encoding/json"
	"github.com/sanjana/CS733/assignment4/raft"


)

var crlf = []byte{'\r', '\n'}

func check(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func reply(conn *net.TCPConn, msg *fs.Msg) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func serve(rn *raft.RaftNode, conn *net.TCPConn) {
	reader := bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr := fs.GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				conn.Close()
				break
			}
		}
		//append to raft
		if string(msg.Kind)!= "r" {
			toSend,err := json.Marshal(msg)
			check(err)
			rn.Append(toSend)
			com := <-rn.CommitChannel();
				

			if com.Success!= true {
				reply(conn, &fs.Msg{Kind: 'M'})
				conn.Close()
				break
			}
			err = json.Unmarshal(com.Data,&msg)
			check(err)

		}
		
		response := fs.ProcessMsg(msg)
		if !reply(conn, response) {
			conn.Close()
			break
		}
	}
}

func serverMain(id int,adr []string,conf *raft.Config) (*raft.RaftNode) {
	tcpaddr, err := net.ResolveTCPAddr("tcp", adr[id-1])
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)
	check(err)
	rf := raft.New(id, conf)
	//client_chan_map := make(map[int]chan *fs.Msg)
	go func(rfnode *raft.RaftNode) {
		for {	//listen to client
			tcp_conn, err := tcp_acceptor.AcceptTCP()
			check(err)
			go serve(rfnode, tcp_conn)
		}
	}(rf)
	return rf
}



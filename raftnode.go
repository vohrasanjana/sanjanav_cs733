package main
import ("fmt"
	"github.com/cs733-iitb/cluster"
	)

type RaftNode struct { // implements Node interface
	eventCh         chan Event
	timeoutCh      	chan Time
}
type Event interface {}
type Time interface {}
type Action interface {}


func (rn *RaftNode) Append(data string) {
	rn.eventCh <- ClientAppend{data: data}
}

func (rn *RaftNode) processEvents() {
	for {
		var ev Event
		select {
			case x, ok := <- rn.eventCh:  {ev = x}
			case x, ok := <- rn.timeoutCh: {ev = Timeout{}}
		}
		actions := sm.ProcessEvent(ev)
		doActions(actions)
	}
}

func doActions(actions []Action) {
	for _, act := range actions {
		act_name := reflect.TypeOf(act).Name()
		switch act_name {
			case "Send" : {
				to := act.(Send).dest
				event := reflect.TypeOf(act.(Send).rEvent)
				OutBox() <- &cluster.Envelope{Pid: to, MsgId: i, Msg: event}
			}
		/*	case "StateStore" : {
				event := act.(StateStore)
				OutBox() <- &cluster.Envelope{Pid: to, MsgId: i, Msg: event
			}
			case "TimeOut" : {
			}
		*/
		}
		i++
	}	
}

func main() {
	conf := cluster.Config{
		Peers: []cluster.PeerConfig{
			{Id: 100, Address: "localhost:7070"},
			{Id: 200, Address: "localhost:8080"},
			{Id: 300, Address: "localhost:9090"},
		}}
	s1, _ := cluster.New(100,conf)
	s2, _ := cluster.New(200,conf)
	s3, _ := cluster.New(300,conf)
	s1.Outbox() <- &cluster.Envelope{Pid: 200, MsgId: 1, Msg: "This is a unicast to 200"}
	s1.Outbox() <- &cluster.Envelope{Pid: cluster.BROADCAST, MsgId: 1, Msg: "This is a broadcast"}
	
	env := <-s2.Inbox()
	fmt.Println(env.Msg)
	env = <-s2.Inbox()
	fmt.Println(env.Msg)
	env = <-s3.Inbox()
	fmt.Println(env.Msg)


}


	

 


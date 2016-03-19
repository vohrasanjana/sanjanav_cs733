package main

import (//"strconv"
	"fmt"
	"github.com/cs733-iitb/cluster"
	"time"
	//"strconv"
	"github.com/cs733-iitb/cluster/mock"
	"reflect"
	 "encoding/gob"
	"math/rand"
)
const RANGE int = 500 
type Event interface {}

type Node interface
{
	// Client's message to Raft node
	Append([]byte)

	// A channel for client to listen on. What goes into Append must come out of here at some point.
	CommitChannel() chan CommitInfo

	// Last known committed index in the log.  This could be -1 until the system stabilizes.
	CommittedIndex() int

	// Returns the data at a log index, or an error.
	Get(index int) (error, []byte)

	// Node's id
	Id() int
	
	// Id of leader. -1 if unknown
	LeaderId() int
	
	// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
	Shutdown()
}

func (rf *RaftNode) Append(data []byte) {
	rf.ClientInput <- ClientAppend{data}
}

func (rf *RaftNode) CommitChannel() <-chan CommitInfo {
	return rf.commitCh
}

func (rf *RaftNode) CommittedIndex() int {
	return rf.sm.commitIndex
}

func (rf *RaftNode) Id() int{
	return rf.id
}

func LeaderId(rn []*RaftNode) int{
	for _,rf := range rn{
		if rf.sm.state == leader{
			return rf.sm.Id
		}
	}
	return 1	//will forward to leader
}

func (rf *RaftNode) ShutDown() {

}
// data goes in via Append, comes out as CommitInfo from the node's CommitChannel
// Index is valid only if err == nil

 

type CommitInfo struct {
	Index	int	// or int .. whatever you have in your code
	success	bool	// Err can be errred
	Data   	[]byte
	
}
// This is an example structure for Config .. change it to your convenience.
type Config struct{
	cluster		*mock.MockCluster	// Information about all servers, including this.
	Id	  	int		// this node's id. One of the cluster's entries should match.
	LogDir		string		// Log file directory for this node
	ElecTimeCand	int
	ElecTimeFoll	int
	HeartbeatTimeout int
}

type NetConfig struct {
	Id	int
	Host	string
	Port	int
}

type RaftNode struct {
	id		int
	//leaderId	int
	eventCh		chan Event
	timeout		*time.Timer
	sm		*StateMachine
	server		cluster.Server
	conf		*Config
	//heartbeat	*time.Ticker
	commitCh	chan CommitInfo
	ClientInput	chan Event
	
	
}
func StartRafts(rafts []*RaftNode) {
	

	//rafts[0].eventCh <- Timeout{}
	//rafts[0].doActions([]Action{Alarm{rafts[0].conf.ElecTimeFoll}})
	
	go func(){
	Running(rafts[0])
	}()
	go func(){
	Running(rafts[1])
	}()
	go func(){
	Running(rafts[2])
	}()
	//time.Sleep(time.Duration(2) * time.Second)
	
}
func New(id int, rc *Config) (*RaftNode) {
	log := make([]LogEntry,10)
	
	/*Peers := make([]int,0)
	srvr_peers := make([]cluster.PeerConfig,len(rc.cluster))
	for _,cs := range rc.cluster {
		Peers = append(Peers,cs.Id)

		srvr_peers = append(srvr_peers, cluster.PeerConfig{cs.Id, cs.Host+":"+strconv.Itoa(cs.Port)})
	}
	////fmt.Println("in new for node ",id)
	conf := cluster.Config{srvr_peers,1000,1000}
	
	srvr, _ := cluster.New(id, conf)*/
	rand.Seed(time.Now().UTC().UnixNano() * int64(id))
	return &RaftNode{	id:		id,
				eventCh: 	make(chan Event,500), 
				timeout : 	time.AfterFunc(time.Duration(rc.ElecTimeCand) * time.Millisecond, func(){}), 
				sm: 		&StateMachine{Id: id, state: follower, leaderId: -1, peers: rc.cluster.Servers[id].Peers(), totalServers: len(rc.cluster.Servers[id].Peers()), currentTerm: 0, lastLogIndex: 0, lastLogTerm: 0, /*lastApplied: 0,*/ votedFor: -1, commitIndex: 0, Log: log, ElecTC: rc.ElecTimeCand, ElecTF:rc.ElecTimeFoll, HeartT: rc.HeartbeatTimeout}, 
				//server : 	srvr, 
				conf: 		rc, 
				//heartbeat: 	time.NewTicker(time.Duration(rc.HeartbeatTimeout) * time.Millisecond), 
				commitCh: 	make(chan CommitInfo,1000),
				ClientInput:	make(chan Event,500),
			}
	
}

	




func makeRafts() ([]*RaftNode, error){

	elec_cand := 1000
	elec_foll := 2000
	hb_t := 100
	gob.Register(Timeout{})
	gob.Register(VoteRequest{})
	gob.Register(VoteResponse{})
	gob.Register(AppendEntriesResponse{})
	gob.Register(AppendEntriesRequest{})
	gob.Register(Send{})
	gob.Register(Commit{})
	gob.Register(ClientAppend{})

	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
		{Id:1}, {Id:2}, {Id:3},
	}}

	cluster, err := mock.NewCluster(clconfig)
	if err != nil {return nil, err}

	
	/*raftConfig := Config{
		ElectionTimeout: 1000, // millis
		HeartbeatTimeout: 250,
	}*/
	conf := make([]Config,0)
	
	for id := 1; id <= 3; id++{	

		conf = append(conf, Config{cluster,id,"~/github.com/sanjana/CS733/assignment3/logs/",elec_cand,elec_foll,hb_t})
	}

	rfNodes := make([]*RaftNode, len(clconfig.Peers))

	// Create a raft node, and give the corresponding "Server" object from the
	// cluster to help it communicate with the others.
	for id := 1; id <= 3; id++ {
		raftNode := New(id, &conf[id-1]) // 

		// Give each raftNode its own "Server" from the cluster.
		raftNode.server = cluster.Servers[id]
			
		rfNodes[id-1] = raftNode
	}

	receivedVotes := make([][]int,10) 

	for i := range rfNodes {
        	receivedVotes[i+1] = make([]int, 10)
    	}

	yesVotes 	= make([][]int,len(rfNodes)+1)
	for i := range rfNodes {
        	yesVotes[i+1] = make([]int, 10)
    	}

	noVotes 	= make([][]int,len(rfNodes)+1)
	for i := range rfNodes {
        	noVotes[i+1] = make([]int, 10)
    	}

	appended = make([][]int, 10)
	for i := range rfNodes {
        	appended[i+1] = make([]int, 10)
    	}

	nextIndex = make([]int, 10)
	matchIndex = make([]int, len(rfNodes)+1)
	
	for i:= range rfNodes {	
//		t := (rfNodes[i].conf.ElecTimeFoll + rand.Intn(RANGE))
		//fmt.Println(t)
		rfNodes[i].doActions([]Action{Alarm{rfNodes[i].conf.ElecTimeFoll}})
	}
	StartRafts(rfNodes)

	return rfNodes, nil
}


func (rf *RaftNode) doActions(actions []Action) {
		for _,act := range actions {
							
				var res resEvent
				act_name := reflect.TypeOf(act).Name()
				//fmt.Println(act_name)
				switch act_name {
					case "Send" :
						to := act.(Send).Dest
						//fmt.Print(" to ",to)
						y := reflect.TypeOf(act.(Send).REvent)
						switch y.Name() {
							case "VoteResponse"	:
								res = act.(Send).REvent.(VoteResponse)
								//fmt.Println(to, " got Vote  Response")
							case "VoteRequest" 	:
								res = act.(Send).REvent.(VoteRequest)
								fmt.Println(to," got  vote  req")
							case "AppendEntriesRequest" :
								res = act.(Send).REvent.(AppendEntriesRequest)
								//fmt.Println(to, " got append  req")
							case "AppendEntriesResponse":
								res = act.(Send).REvent.(AppendEntriesResponse)
								//fmt.Println(to, " got append  resp")
							case "ClientAppend":
								res = act.(Send).REvent.(ClientAppend)
								
							default :
								res = act
							}
						rf.server.Outbox() <- &cluster.Envelope{Pid: to, Msg: res}	
					case "StateStore" :
						//store := act.(StateStore)
						//fmt.Println(" statestore",store)
						//_,_ = state.WriteString(strconv.Itoa(store.Id)+" "+strconv.Itoa(store.state)+" "+strconv.Itoa(store.LeaderId)+" "+strconv.Itoa(store.Term))
			
					case "Alarm" :
						
						rf.timeout.Stop()

						t := act.(Alarm).timeout
						if t != rf.conf.HeartbeatTimeout {
							t = (t + rand.Intn(RANGE))
						}
						//if rf.sm.state != 3 {
							//fmt.Println(rf.id," has set alaaarrrrmmmm for time :::: ",t, " at ",time.Now())
						//}		
						rf.timeout = time.AfterFunc(time.Duration(t) * time.Millisecond, func(){
						if rf.sm.state != leader {
							fmt.Println(rf.id, " sending timeout after ",t, " at ",time.Now())
						}							
						rf.timeout.Stop()
						//fmt.Println("------",rf.timeout.Stop())
						rf.eventCh <- Timeout{}})
					case "Commit":
						fmt.Println("committed")
						ev := act.(Commit)					
						rf.commitCh <- CommitInfo{ev.Index,ev.Success,ev.Data}
					case "LogStore" :
						//logst := act.(LogStore)
						//fmt.Println("logstore")
						//fmt.Println(logst.Id,logst.Log)
					default :
						res = act
				}
			}

}

func Running(rf *RaftNode) {	
	
	actions := make([]Action,0)
	//log := make([]LogEntry,100)
	
	sm := rf.sm
	////fmt.Print("in running ")
	
	for {
		select {
		case env := <-rf.server.Inbox():
			//fmt.Print(rf.id," got ")
			
			//fmt.Println(reflect.TypeOf(env.Msg))
			actions = sm.processEvents(env.Msg)
			
			rf.doActions(actions)
		case <-rf.eventCh:
			//fmt.Println("by alarm timeout ",sm.Id, "--",time.Now())
			actions = sm.processEvents(Timeout{})
			rf.doActions(actions)
		case en := <-rf.ClientInput:
			fmt.Println("------------------------------------------------------")
			actions = sm.processEvents(en)
			rf.doActions(actions)
		/*case com := <-rf.commitCh:
			fmt.Println(sm.Id," committed at index ",com.Index)*/
		}
	}
	
	
}

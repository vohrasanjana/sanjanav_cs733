package raft

import ("fmt"
	"github.com/cs733-iitb/cluster"
	"time"
	"strconv"
	"encoding/gob"
	"io/ioutil"
	"github.com/cs733-iitb/log"
	"github.com/cs733-iitb/cluster/mock"
	"reflect"
	"strings"
	"math/rand"
)
const RANGE int = 100 
const NUMRAFTS int = 5

type Event interface {}
type Shut struct{}
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
	ShutDown()
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
	for {
	
		for _,rf := range rn{
			if rf.sm.state == leader{
				return rf.sm.Id
			}
		}
	}
}

func (rf *RaftNode) ShutDown() {
	rf.ShutCh <- Shut{}

}

type CommitInfo struct {			//Commit information from node
	Index	int	
	Success	bool	
	Data   	[]byte
	
}

type Config struct{
	Cluster		*mock.MockCluster	// Information about all servers, including this.
	Id			int		// this node's id. One of the cluster's entries should match.
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
	eventCh		chan Event			//Event Channel to listen to events
	Timeout		*time.Timer			//Timer for election and heartbeat timeouts
	sm		*StateMachine
	server		cluster.Server
	Conf		*Config
	commitCh	chan CommitInfo			//Channel for commits
	ClientInput	chan Event			//Input from client comes in via this channel
	StateFile	string				//File for storing last updated state of raft node
	ShutCh 		chan Shut			
}


func makeRafts() ([]*RaftNode, *mock.MockCluster, error){

	elec_cand := 1000
	elec_foll := 1500
	hb_t := 100
	
	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
		{Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
	}}

	cluster, err := mock.NewCluster(clconfig)
	if err != nil {return nil, nil, err}
	
	conf := make([]Config,0)
	
	for id := 1; id <= NUMRAFTS; id++{	
	
		conf = append(conf, Config{cluster,id,elec_cand,elec_foll,hb_t})
	}

	rfNodes := make([]*RaftNode, len(clconfig.Peers))

								// Create a raft node, and give the corresponding "Server" object 									// from the cluster to help it communicate with the others.
	for id := 1; id <= NUMRAFTS; id++ {
		raftNode := New(id, &conf[id-1]) 

		// Give each raftNode its own "Server" from the cluster.
		raftNode.server = cluster.Servers[id]
			
		rfNodes[id-1] = raftNode
	}

	
	InitSM(5)						//Initialization of state machine variables
	for i:= range rfNodes {					//Initial election timeout on the nodes
		rfNodes[i].DoActions([]Action{Alarm{rfNodes[i].Conf.ElecTimeFoll}})
	}
	
	return rfNodes, cluster, nil
}

func New(id int, rc *Config) (*RaftNode) {			//Create a Raft Node with id and configuration given

	gob.Register(Timeout{})					//Registering all structures for communication
	gob.Register(VoteRequest{})
	gob.Register(VoteResponse{})
	gob.Register(LogEntry{})
	gob.Register(AppendEntriesResponse{})
	gob.Register(AppendEntriesRequest{})
	gob.Register(Send{})
	gob.Register(Commit{})
	gob.Register(ClientAppend{})

	filename := "rflog"+strconv.Itoa(id)			//Log file
	lg, _ := log.Open(filename)

	sf:= "state_"+strconv.Itoa(id)				//State file
	ost,err := ioutil.ReadFile(sf)
	stmc := &StateMachine{}
	if err != nil || len(ost) <= 0 {			//New Node, state file doesn't exist
		stmc = NewSM(id,follower,-1,rc.Cluster.Servers[id].Peers(), len(rc.Cluster.Servers[id].Peers()), 0, -1, 0, -1, -1, lg, rc.ElecTimeCand, rc.ElecTimeFoll, rc.HeartbeatTimeout)

	} else {
		oldSt := strings.Split(string(ost), " ")	//Restarting a node, from old saved state for recovery
		var oldState [6]int
		for i := range oldSt {
			oldState[i], err = strconv.Atoi(oldSt[i])
			if err != nil{
				fmt.Println("ERROR:STATE FILE CORRUPTED ", i)
				panic(err)
			}
		}
		stmc = NewSM(id,oldState[1],oldState[2],rc.Cluster.Servers[id].Peers(), len(rc.Cluster.Servers[id].Peers()), oldState[3], -1, 0, oldState[4], oldState[5], lg, rc.ElecTimeCand, rc.ElecTimeFoll, rc.HeartbeatTimeout)
								//State Machine instance

	}
	rand.Seed(time.Now().UTC().UnixNano() * int64(id))
	rn := &RaftNode{	id:		id,
				eventCh: 	make(chan Event,50000), 
				Timeout : 	time.AfterFunc(time.Duration(rc.ElecTimeCand) * time.Millisecond, func(){}), /*for first timeout*/
				sm: 		stmc, 
				Conf: 		rc, 
				commitCh: 	make(chan CommitInfo,50000),
				ClientInput:	make(chan Event,50000),
				StateFile: sf,
				ShutCh:		make(chan Shut,10000),

	}
	rn.server = rc.Cluster.Servers[id]
	

	go func(){
		Running(rn)
	}()

	rn.DoActions([]Action{Alarm{rn.Conf.ElecTimeFoll}})

	
	return rn
	
}

func (rf *RaftNode) DoActions(actions []Action) {				//Perform actions according to message recieved
		for _,act := range actions {
							
				var res resEvent
				act_name := reflect.TypeOf(act).Name()
				switch act_name {
					case "Send" :				//send events to other nodes

						to := act.(Send).Dest		//destination node
						y := reflect.TypeOf(act.(Send).REvent)
						

						switch y.Name() {

							case "VoteResponse"	:
								res = act.(Send).REvent.(VoteResponse)
								//got Vote  Response
							case "VoteRequest" 	:
								res = act.(Send).REvent.(VoteRequest)
								//got  vote  req
							case "AppendEntriesRequest" :
								res = act.(Send).REvent.(AppendEntriesRequest)
								//got append  req
							case "AppendEntriesResponse":
								res = act.(Send).REvent.(AppendEntriesResponse)
								//got append  resp
								
							default :
								res = act
							}
						rf.server.Outbox() <- &cluster.Envelope{Pid: to, Msg: res}	
					case "StateStore" :			//Store latest state for recovery
						store := act.(StateStore)
						err := ioutil.WriteFile(rf.StateFile, []byte(strconv.Itoa(store.Id)+" "+strconv.Itoa(store.state)+" "+strconv.Itoa(store.LeaderId)+" "+strconv.Itoa(store.Term)+" "+strconv.Itoa(store.votedFor)+" "+strconv.Itoa(store.commitIndex)), 0666)

						if err != nil {
    							fmt.Println("ERROR WRITING TO STATE FILE ")
    							panic(err)
						}
						
						
			
					case "Alarm" :				//Timeouts
						
						rf.Timeout.Stop()

						t := act.(Alarm).Timeout
						if t != rf.Conf.HeartbeatTimeout {
							t = (t + rand.Intn(RANGE))
						}
						rf.Timeout = time.AfterFunc(time.Duration(t) * time.Millisecond, func(){
						rf.Timeout.Stop()
						rf.eventCh <- Timeout{}})
					
					case "Commit":				//Send commit information on commit channel
						ev := act.(Commit)					
						rf.commitCh <- CommitInfo{ev.Index,ev.Success,ev.Data}
					
					case "LogStore" :
						logst := act.(LogStore)
						filename := "rflog"+strconv.Itoa(logst.Id)
						lg, _ := log.Open(filename)
						defer lg.Close()
						lg.Append(logst)
						
					default :
						res = act
				}
			}

}

func Running(rf *RaftNode) {			//parallelly runs for all raft nodes, listens on inbox, events and shutdown channel
	
	actions := make([]Action,0)
	
	sm := rf.sm
	
	for {
		select {
		case env := <-rf.server.Inbox():
			
			actions = sm.processEvents(env.Msg)
			
			rf.DoActions(actions)
		case <-rf.eventCh:
			actions = sm.processEvents(Timeout{})
			rf.DoActions(actions)
		case en := <-rf.ClientInput:
			actions = sm.processEvents(en)
			
			rf.DoActions(actions)
		case <-rf.ShutCh:
			rf.sm.state = follower
			rf.server.Close()
			rf.Timeout.Stop()

			return;
		
		}
	}
	
	
}

package raft
import ("fmt"
	"math"
	"github.com/cs733-iitb/log"	
)


const (						//three states of raft node
	follower	int = 1
	candidate	int = 2
	leader		int = 3
)

type Action interface {}
var receivedVotes 	[][]int 
var yesVotes 		[][]int
var noVotes 		[][]int
var nextIndex 		[]int64
var matchIndex		[]int
var appended		[][]int
var flag 		int
type Send struct {
	Dest		int		
	REvent		resEvent
}
type resEvent interface{}
type LogEntry struct {
	Term 		int
	data		[]byte
}
type ClientAppend struct {
	Data		[]byte
}
type Commit struct {
	Index		int
	Success 	bool
	Data		[]byte
}
type VoteRequest struct {
	Term		int
	CandidateId	int
	LastLogIndex	int
	LastLogTerm	int
}

type VoteResponse struct {
	Term		int
	VoteGranted	bool
	From		int
}
type Timeout struct{}
type AppendEntriesRequest struct {
	From		int
	Term		int
	LeaderId	int
	PrevLogIndex	int
	PrevLogTerm	int
	Entries		[]byte
	LeaderCommit	int
}

type AppendEntriesResponse struct {
	Term		int
	From		int
	Success		bool
	LastIndex	int
	Data		[]byte
}

type LogStore struct {
	Id		int
	Log 		[]LogEntry
}

type StateStore struct {
	Id			int
	state 		int
	LeaderId	int
	Term		int
	votedFor	int
	commitIndex	int
}
type Alarm struct {
	Timeout		int
}

type StateMachine struct {
	Id		int
	state		int
	leaderId	int
	peers		[]int
	totalServers	int
	currentTerm	int
	lastLogIndex	int
	lastLogTerm	int
	votedFor	int
	commitIndex	int
	Log		*log.Log
	ElecTC		int
	ElecTF		int
	HeartT 		int
}
//function to initialize a state machine - may be new or from an old, saved state

func NewSM(Id int, state int, leaderId int, peers []int, totalServers int, currentTerm int, lastLogIndex int, 	lastLogTerm int, votedFor int, commitIndex int, Log *log.Log, ElecTC int, ElecTF int, HeartT int)  (sm *StateMachine) {
	
	stmc := &StateMachine{		Id: Id,
					state: state, 
					leaderId: leaderId, 
					peers: peers, 
					totalServers: totalServers, 
					currentTerm: currentTerm, lastLogIndex: lastLogIndex, lastLogTerm: lastLogTerm, votedFor: votedFor, commitIndex: commitIndex,
					Log: Log, 
					ElecTC: ElecTC, 
					ElecTF:ElecTF, HeartT: HeartT}

	
	return stmc

}

func InitSM(rfNodes int){
	receivedVotes := make([][]int,100) 

	for i :=0;i<rfNodes;i++ {
        	receivedVotes[i+1] = make([]int, 100)
    	}

	yesVotes 	= make([][]int,(rfNodes+1))
	for i :=0;i<rfNodes;i++{
		   	yesVotes[i+1] = make([]int, 100)
    	}

	noVotes 	= make([][]int,(rfNodes+1))
	for i :=0;i<rfNodes;i++{
        	noVotes[i+1] = make([]int, 100)
    	}

	appended = make([][]int, 10)
	for i :=0;i<rfNodes;i++{
        	appended[i+1] = make([]int, 100)
    	}

	nextIndex = make([]int64, 10)
	matchIndex = make([]int, (rfNodes+1))
	
}


type Signal struct {
	Content interface{}
}

func (sm *StateMachine) CheckCommit(index int) bool {				//check for leader to commit, appends on majority nodes
	flag := 0;
	for i:=1;i<=NUMRAFTS;i++ {
		if(matchIndex[i]>=index){
			flag++
		}
	}
	if flag >sm.totalServers/2 {
		return true
	}
	return false
}


func (sm *StateMachine) processEvents(ev interface{}) []Action {		//process events according to state of the node 
						
action := make([]Action,0) 
flag = 0
switch sm.state {
	
	case candidate:
		
		 {
			switch ev.(type) {
				case Timeout:					//Candidate Timeout, increments term and restarts election
					sm.currentTerm++
					sm.state = candidate
					sm.votedFor = sm.Id
					yesVotes[sm.Id][sm.currentTerm] ++
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm,sm.votedFor, sm.commitIndex})
					action = append(action,Alarm{sm.ElecTC})		
					for _, sId := range sm.peers {
						if sId != sm.Id {
							action = append(action,Send{sId, VoteRequest{sm.currentTerm, sm.Id, sm.lastLogIndex, sm.lastLogTerm}})
						}
					}
		
					
				case VoteRequest:
					res := ev.(VoteRequest)
					if sm.currentTerm >= res.Term 	{
					action = append(action, Send{res.CandidateId, VoteResponse{sm.currentTerm, false, sm.Id}})
					} else {
						sm.currentTerm = res.Term
						sm.votedFor = res.CandidateId
						sm.state = follower
						action = append(action,Alarm{sm.ElecTF})	
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
					
						action = append(action, Send{res.CandidateId, VoteResponse{sm.currentTerm, true, sm.Id}})
					}
					
				case VoteResponse:

					res := ev.(VoteResponse)
								
					if res.Term != sm.currentTerm {
						
						break
						
					}

					if res.VoteGranted {				//becomes leader if majority nodes vote
						yesVotes[sm.Id][sm.currentTerm] ++
						
						if yesVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 {
							sm.state = leader
							sm.leaderId = sm.Id
							for i := 0; i<sm.totalServers; i++ {
								nextIndex[i] = sm.Log.GetLastIndex()
								
							}
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm,sm.votedFor, sm.commitIndex})
							
							action = append(action, Alarm{sm.HeartT})
							for _, sId := range sm.peers {	//heartbeats
								if sId != sm.Id {
									action = append(action,Send{sId, AppendEntriesRequest{sm.Id,sm.currentTerm, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, []byte{}, sm.commitIndex}})
								}
							}
		
		
						}
					} else {					//becomes follower if majority nodes reject
						
						noVotes[sm.Id][sm.currentTerm]++
						if noVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 {
							sm.votedFor = -1
							sm.state = follower
							sm.leaderId = -1
							action = append(action,Alarm{sm.ElecTF})	
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
					
						}
						
					}

				case ClientAppend:					//forward to leader
					
					
					res := ev.(ClientAppend)
					action = append(action, Send{sm.leaderId, ClientAppend{res.Data}})

				case AppendEntriesRequest:				//may be heartbeat or data append from leader
					res := ev.(AppendEntriesRequest)
					if res.Term < sm.currentTerm {
						action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
						
						break
					}
					if res.Term > sm.currentTerm {
						sm.currentTerm = res.Term
						sm.votedFor = -1
						sm.leaderId = res.LeaderId
						flag = 1
						sm.state = follower
						action = append(action,Alarm{sm.ElecTF})	
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})	
						
					}	
					if res.PrevLogIndex >=0 {
						temp,err := sm.Log.Get(int64(res.PrevLogIndex))
						if err!=nil {
							panic(err)
						}
						if temp.(LogEntry).Term != res.PrevLogTerm {
							action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
							action = append(action, Alarm{sm.ElecTF})
							break
						}
					}
					sm.commitIndex = int(math.Min(float64(res.LeaderCommit),float64(sm.lastLogIndex)))
									
					action = append(action, StateStore{sm.Id,sm.state, sm.leaderId,sm.currentTerm,sm.votedFor,sm.commitIndex})
					if len(res.Entries) == 0 { //heartbeat
						if sm.leaderId != res.LeaderId {
							sm.leaderId = res.LeaderId
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm,sm.votedFor, sm.commitIndex})
							
						}
						action = append(action, Alarm{sm.ElecTF}) // set election timeout
						for i:= sm.commitIndex+1;i<= res.LeaderCommit;i++ {	
							
							//commit for all entries that leader has committed

							temp,err := sm.Log.Get(int64(i))
							if err!= nil{
								panic(err)
							}
							action = append(action, Commit{i,true,temp.(LogEntry).data})
							flag = 1
						}	
						sm.commitIndex = res.LeaderCommit
									
						action = append(action, StateStore{sm.Id,sm.state, sm.leaderId,sm.currentTerm,sm.votedFor,sm.commitIndex})
					
					} else {
						
						
						if res.PrevLogIndex == 0 && sm.lastLogIndex == 0 { //empty log
							newEntry := LogEntry{res.Term,res.Entries}
							sm.Log.Append(newEntry)
							sm.lastLogIndex ++
							temp,err := sm.Log.Get(int64(sm.lastLogIndex))
							if err!= nil {
								panic(err)
							}
							sm.lastLogTerm = temp.(LogEntry).Term
							action = append(action, Alarm{sm.ElecTF})
							action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
						} else if sm.lastLogIndex >= 0 && sm.lastLogIndex >= res.PrevLogIndex {
							if sm.lastLogIndex == res.PrevLogIndex {
								temp,err := sm.Log.Get(int64(res.PrevLogIndex))
								
								if err != nil {
									panic(err)
								}
								if res.PrevLogTerm == temp.(LogEntry).Term {
									sm.Log.Append(LogEntry{res.Term,res.Entries})
									sm.lastLogIndex ++
									temp,err :=  sm.Log.Get(int64(sm.lastLogIndex))
									if err != nil {
										panic(err)
									}
									sm.lastLogTerm =temp.(LogEntry).Term
									action = append(action, Alarm{sm.ElecTC})
									action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
									break
								} 
							}
						} else {
							action = append(action, Alarm{sm.ElecTC})
							action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
							
						}
							
					}
			}
		}
			
	case follower:
		
			
			switch ev.(type) {
				case Timeout:
					
					sm.currentTerm++
					sm.state = candidate
					sm.votedFor = sm.Id
					yesVotes[sm.Id][sm.currentTerm] ++
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
					action = append(action, Alarm{sm.ElecTC})			 
					for _, sId := range sm.peers {
						if sId != sm.Id {
							action = append(action,Send{sId, VoteRequest{sm.currentTerm, sm.Id, sm.lastLogIndex, sm.lastLogTerm}})
						}
					}
					
					
					
				case VoteRequest:

					res := ev.(VoteRequest)
					dest := res.CandidateId
						
					if res.Term < sm.currentTerm {
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}


					if res.Term > sm.currentTerm {
						sm.currentTerm = res.Term
						sm.votedFor = -1
						flag = 1
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
						
					}

					if sm.votedFor != -1 { //already voted
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}
					if sm.lastLogIndex >=0 {
						temp,err:= sm.Log.Get(int64(sm.lastLogIndex))
						if err != nil {
							panic(err)
						}
						if res.LastLogTerm < temp.(LogEntry).Term { // not uptodate
							action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
							break
						}
					}
					if res.LastLogIndex < sm.lastLogIndex { // old
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}

					sm.votedFor = res.CandidateId
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
					action = append(action, Alarm{sm.ElecTF})
					action = append(action, Send{dest, VoteResponse{sm.currentTerm, true, sm.Id}})
				
				case ClientAppend:
					
					res := ev.(ClientAppend)
					action = append(action, Send{sm.leaderId, ClientAppend{res.Data}})
				
				case AppendEntriesResponse:
					
				case AppendEntriesRequest:				//maybe heartbeat or data from leader
					res := ev.(AppendEntriesRequest)
					if res.Term < sm.currentTerm {
						action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
						break
					}  
					if res.Term > sm.currentTerm {
						if (sm.currentTerm != res.Term || sm.leaderId != res.LeaderId ){
							sm.currentTerm = res.Term
							sm.leaderId = res.LeaderId
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
						
						}
						action = append(action, Alarm{sm.ElecTF})
						
					}
					if res.PrevLogIndex >= 0 && res.PrevLogIndex <=sm.lastLogIndex && len(res.Entries) != 0 { 
					
						temp,err := sm.Log.Get(int64(res.PrevLogIndex)) 
						
						if err != nil {
							fmt.Println("FAILED READING LOG AT ", res.PrevLogIndex)
							panic(err)
						}
						if temp.(LogEntry).Term != res.PrevLogTerm {
							action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
							action = append(action, Alarm{sm.ElecTF})
							break
						} else {
							err = sm.Log.TruncateToEnd(int64(res.PrevLogIndex+1))
							sm.lastLogIndex = res.PrevLogIndex
							sm.lastLogTerm = res.PrevLogTerm
						}
					} 
					/*for i:= sm.commitIndex+1;i<= res.LeaderCommit;i++ {
							
						temp,err := sm.Log.Get(int64(i))
						if err != nil {
							fmt.Println(sm.Id,i)
							panic(err)
						}
						
						action = append(action, Commit{i,true,temp.(LogEntry).data})
						flag = 1
					}*/	
					sm.commitIndex = int(math.Min(float64(res.LeaderCommit),float64(sm.lastLogIndex)))
								
					action = append(action, StateStore{sm.Id,sm.state, sm.leaderId,sm.currentTerm,sm.votedFor,sm.commitIndex})
							
					if len(res.Entries) == 0 { //heartbeat
						if sm.leaderId != res.LeaderId {
							sm.leaderId = res.LeaderId
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm,sm.votedFor, sm.commitIndex})
							
						}
						action = append(action, Alarm{sm.ElecTF})
						
					} else {
						sm.currentTerm = res.Term
						if res.PrevLogIndex == -1 && sm.lastLogIndex == -1 { //empty log
							newEntry := LogEntry{res.Term,res.Entries}
							sm.Log.Append(newEntry)
							sm.lastLogIndex ++
							temp,err := sm.Log.Get(int64(sm.lastLogIndex))
							if err != nil {
								fmt.Println("FAILED READING LOG AT ",sm.lastLogIndex)
								panic(err)
							}
							sm.lastLogTerm = temp.(LogEntry).Term	
							action = append(action, Alarm{sm.ElecTF})
							action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
							
						} else if res.PrevLogIndex >= 0 && sm.lastLogIndex >= res.PrevLogIndex {
							if sm.lastLogIndex == res.PrevLogIndex {
								
								temp, err := sm.Log.Get(int64(res.PrevLogIndex))
								if err != nil {
									panic(err)
								}
								if res.PrevLogTerm == temp.(LogEntry).Term {
									sm.Log.Append(LogEntry{res.Term,res.Entries})
									sm.lastLogIndex ++
									le,err := sm.Log.Get(int64(sm.lastLogIndex))
									if err!= nil {
										panic(err)
									}
									sm.lastLogTerm = le.(LogEntry).Term 
									action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
									action = append(action, Alarm{sm.ElecTF})
									
								} else {
									
									action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
									action = append(action, Alarm{sm.ElecTF})
									
								}
							} else{
								action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
								action = append(action, Alarm{sm.ElecTF})
							}
						} else{ 
							action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
							action = append(action, Alarm{sm.ElecTF})
								
						}
					}
			}
		
	case leader:
		 
			switch ev.(type) {
			case AppendEntriesRequest:
				res := ev.(AppendEntriesRequest)
				if res.Term > sm.currentTerm {
					sm.currentTerm = res.Term
					sm.votedFor = -1
					sm.state = follower
					sm.leaderId = -1
					flag = 1
					action = append(action,Alarm{sm.ElecTF})	
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
				} else {
					action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
					
				}
			case VoteRequest:
				res := ev.(VoteRequest)
				if res.Term > sm.currentTerm {
					sm.currentTerm = res.Term 
					sm.votedFor = res.CandidateId
					sm.state = follower
					sm.leaderId = -1
					flag = 1
					action = append(action,Alarm{sm.ElecTF})	
					action = append(action, Send{res.CandidateId, VoteResponse{sm.currentTerm, true, sm.Id}})

					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
				} else {

					action = append(action, Send{res.CandidateId, VoteResponse{sm.currentTerm, false, sm.Id}})
					
				}
			case AppendEntriesResponse:					//check for majority responses and commit
				res := ev.(AppendEntriesResponse)
				if res.Term > sm.currentTerm {
					sm.state = follower
					sm.currentTerm = res.Term
					sm.leaderId = -1
					flag = 1
					sm.votedFor = -1
					action = append(action,Alarm{sm.ElecTF})	
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm, sm.votedFor, sm.commitIndex})
					
					break

				}
				if !res.Success {
					
					var lastTerm int
					if res.LastIndex >= 0{
						temp,err := sm.Log.Get(int64(res.LastIndex)) // prev log entry
						if err != nil {
						panic(err)
						}
						lastTerm = temp.(LogEntry).Term 
						
					} else {
						lastTerm = 0

					}		
					for i:=res.LastIndex+1;i<=sm.lastLogIndex;i++{
						
						temp,err := sm.Log.Get(int64(i))
						if err != nil {
							panic(err)
						}	
						
						dat := temp.(LogEntry).data

						t := temp.(LogEntry).Term	
						lastTerm = t
						action = append(action,Send{res.From,AppendEntriesRequest{sm.Id,t,sm.leaderId,i-1,lastTerm,dat,sm.commitIndex}})							
					}
					
					
					break
					
				}
				nextIndex[res.From] = int64(res.LastIndex)
				matchIndex[res.From] = res.LastIndex
				if sm.commitIndex < res.LastIndex { // not already committed for this index
					if sm.CheckCommit(res.LastIndex) {

						sm.commitIndex = res.LastIndex
					
						temp,err := sm.Log.Get(int64(sm.commitIndex))
						if err != nil{
							panic(err)
						}
						com := temp.(LogEntry).data
						action = append(action, Commit{sm.commitIndex,true,com})
						action = append(action, StateStore{sm.Id,sm.state, sm.leaderId,sm.currentTerm,sm.votedFor,sm.commitIndex})
					
					}
				}
			case Timeout:
				for _, sId := range sm.peers {				//Sending heartbeats
					if sId != sm.Id {
										
						action = append(action,Send{sId, AppendEntriesRequest{sm.Id, sm.currentTerm, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, []byte{}, sm.commitIndex}})
					}
				}
				action = append(action, Alarm{sm.HeartT})
			case ClientAppend:						
				//append request from client, forward to all nodes for replication
				res := ev.(ClientAppend)
				appended[sm.Id][sm.currentTerm] = 0
				newEntry := LogEntry{sm.currentTerm,res.Data}
				err:= sm.Log.Append(newEntry)
				if err!= nil{
					panic(err)
				}

				lastInd := sm.lastLogIndex
				sm.lastLogIndex ++
				lastTerm := sm.lastLogTerm

				temp,err := sm.Log.Get(int64(sm.lastLogIndex))
				if err != nil{
					fmt.Println("FAILED READING LOG AT ",sm.lastLogIndex)
					panic(err)
				}
				sm.lastLogTerm = temp.(LogEntry).Term
				matchIndex[sm.Id] = sm.lastLogIndex
				
				for _, sId := range sm.peers {
					if sId != sm.Id {
						action = append(action,Send{sId, AppendEntriesRequest{sm.Id, sm.currentTerm, sm.leaderId, lastInd, lastTerm, res.Data, sm.commitIndex}})
						
						
					}
				}
			
				action = append(action, Alarm{sm.HeartT})
										
	 
				
			}
			
	}
return action
}


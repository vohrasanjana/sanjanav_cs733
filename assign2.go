package main
import ("fmt")


const (
	follower	int = 1
	candidate	int = 2
	leader		int = 3
	clientId	int = 10000
)

type Action interface {}
var receivedVotes 	[][]int 
var yesVotes 		[][]int
var noVotes 		[][]int
var nextIndex 		[]int
var matchIndex		[]int
var appended		[][]int
var flag 		int
type Send struct {
	dest		int		//serverId, -1 -> statestore/logStore
	rEvent		resEvent
}
type resEvent interface{}
type LogEntry struct {
	term 		int
	committed	bool
	index		int
}
type ClientAppend struct {}
type Commit struct {
	Index		int
	success 	bool
}
type VoteRequest struct {
	term		int
	candidateId	int
	lastLogIndex	int
	lastLogTerm	int
}

type VoteResponse struct {
	term		int
	voteGranted	bool
	from		int
}
type Timeout struct{}
type AppendEntriesRequest struct {
	term		int
	leaderId	int
	prevLogIndex	int
	prevLogTerm	int
	entries		bool
	leaderCommit	int
}

type AppendEntriesResponse struct {
	term		int
	from		int
	success		bool
	lastIndex	int
}

type LogStore struct {
	Id		int
	Log 		[]LogEntry
}

type StateStore struct {
	Id		int
	state 		int
	leaderId	int
	term		int
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
	//lastApplied	int
	votedFor	int
	commitIndex	int
	Log		[]LogEntry
}

type Signal struct {
	Content interface{}
}





func (sm *StateMachine) processEvents(ev interface{}) []Action {
						
action := make([]Action,0) 
flag = 0
switch sm.state {
	case candidate:
		
		 {
			//ev := <-Events
			switch ev.(type) {
				case Timeout:
					
					
					sm.currentTerm++
					sm.state = candidate
					
					
					sm.votedFor = sm.Id
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
					for _, sId := range sm.peers {
						if sId != sm.Id {
							//fmt.Println("here")
							action = append(action,Send{sId, VoteRequest{sm.currentTerm, sm.Id, sm.lastLogIndex, sm.lastLogTerm}})
						}
					}
		
					
				case VoteRequest:
					res := ev.(VoteRequest)
					if sm.currentTerm >= res.term 	{
					action = append(action, Send{res.candidateId, VoteResponse{sm.currentTerm, false, sm.Id}})
					} else {
						sm.currentTerm = res.term
						sm.votedFor = res.candidateId
						sm.state = follower
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						action = append(action, Send{res.candidateId, VoteResponse{sm.currentTerm, true, sm.Id}})
					}
				case VoteResponse:

					res := ev.(VoteResponse)
						
								
					if res.term != sm.currentTerm {
						
						break
						
					}

					if res.voteGranted {
						yesVotes[sm.Id][sm.currentTerm] ++
						
			
						if yesVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 {
							sm.state = leader
							sm.leaderId = sm.Id
							for i := 0; i<sm.totalServers; i++ {
								nextIndex[i] = len(sm.Log)
								matchIndex[i] = -1
							}
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
							for _, sId := range sm.peers {//heartbeats
								if sId != sm.Id {
									action = append(action,Send{sId, AppendEntriesRequest{sm.currentTerm, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, false, sm.commitIndex}})
								}
							}
		
		
						}
					} else {
						

						noVotes[sm.Id][sm.currentTerm]++
						if noVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 {
							sm.votedFor = -1
							sm.state = follower
							sm.leaderId = -1
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						}
						
					}

				case ClientAppend:
					action = append(action, Send{sm.leaderId, ClientAppend{}})

				case AppendEntriesRequest:
					res := ev.(AppendEntriesRequest)
					if res.term < sm.currentTerm {
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
						
						break
					}//else {
					if res.term > sm.currentTerm {
						sm.currentTerm = res.term
						sm.votedFor = -1
						sm.leaderId = res.leaderId
						flag = 1
						sm.state = follower
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						
						
					}	

					if sm.Log[res.prevLogIndex].term != res.prevLogTerm {
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
						
						break
					}//else {
	
					if res.entries == false { //heartbeat
						sm.leaderId = res.leaderId
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, true, sm.lastLogIndex}})
					} else {
						
						
						if res.prevLogIndex == 0 && sm.lastLogIndex == 0 { //empty log
							newEntry := LogEntry{res.term, false, res.prevLogIndex+1}
							sm.Log[res.prevLogIndex+1] = newEntry
							sm.lastLogIndex ++
							sm.lastLogTerm = sm.Log[sm.lastLogIndex].term
							action = append(action, LogStore{sm.Id,sm.Log})
							action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex}})
							
						} else if sm.lastLogIndex > 0 && sm.lastLogIndex >= res.prevLogIndex {
							if sm.lastLogIndex == res.prevLogIndex {
								if res.prevLogTerm == sm.Log[res.prevLogIndex].term {
									sm.Log[res.prevLogIndex +1] = LogEntry{res.term, false, res.prevLogIndex+1}
									sm.lastLogIndex ++
									sm.lastLogTerm = sm.Log[sm.lastLogIndex].term
									action = append(action, LogStore{sm.Id,sm.Log})
									action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex}})
									break
								} 
							}
						}  
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
							
					}
			}
		}
			
	case follower:
		{
			switch ev.(type) {
				case Timeout:
					sm.currentTerm++
					sm.state = candidate
					
					sm.votedFor = sm.Id
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
										
					for _, sId := range sm.peers {
						if sId != sm.Id {
							action = append(action,Send{sId, VoteRequest{sm.currentTerm, sm.Id, sm.lastLogIndex, sm.lastLogTerm}})
						}
					}
		
					
				case VoteRequest:
					res := ev.(VoteRequest)
					dest := res.candidateId
					if res.term < sm.currentTerm {
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}


					if res.term > sm.currentTerm {
						sm.currentTerm = res.term
						sm.votedFor = -1
						flag = 1
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
					}

					if sm.votedFor != -1 { //already voted
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}

					if res.lastLogTerm < sm.Log[sm.lastLogIndex].term { // not uptodate
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}

					if res.lastLogIndex < sm.lastLogIndex { // old
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}
					sm.votedFor = res.candidateId
					action = append(action, Send{dest, VoteResponse{sm.currentTerm, true, sm.Id}})
				

				case ClientAppend:
					action = append(action, Send{sm.leaderId, ClientAppend{}})

				case AppendEntriesRequest:
					res := ev.(AppendEntriesRequest)
					if res.term < sm.currentTerm {
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
						break
					} //else 
					if res.term > sm.currentTerm {
						sm.currentTerm = res.term
						sm.leaderId = res.leaderId
						flag = 1
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					}
					if sm.Log[res.prevLogIndex].term != res.prevLogTerm {
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
						break
					}	
					if res.entries == false { //heartbeat
						sm.leaderId = res.leaderId
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, true, sm.lastLogIndex}})
					} else {
						sm.currentTerm = res.term
						if res.prevLogIndex == 0 && sm.lastLogIndex == 0 { //empty log
							newEntry := LogEntry{res.term, false, res.prevLogIndex+1}
							sm.Log[res.prevLogIndex+1] = newEntry
							sm.lastLogIndex ++
							sm.lastLogTerm = sm.Log[sm.lastLogIndex].term	
							action = append(action, LogStore{sm.Id,sm.Log})
							action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex}})
						} else if sm.lastLogIndex > 0 && sm.lastLogIndex >= res.prevLogIndex {
							if sm.lastLogIndex == res.prevLogIndex {
								if res.prevLogTerm == sm.Log[res.prevLogIndex].term {
										sm.Log[res.prevLogIndex +1] = LogEntry{res.term, false, res.prevLogIndex+1}
										sm.lastLogIndex ++
										sm.lastLogTerm = sm.Log[sm.lastLogIndex].term
										action = append(action, LogStore{sm.Id,sm.Log})
										action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex}})
								} 
							}
						}  
							action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
							
					}
			}
		}
	case leader:
		 
			switch ev.(type) {
			case AppendEntriesRequest:
				res := ev.(AppendEntriesRequest)
				if res.term > sm.currentTerm {
					sm.currentTerm = res.term
					sm.votedFor = -1
					sm.state = follower
					sm.leaderId = -1
					flag = 1
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
				} else {
					action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
					
				}
			case VoteRequest:
				res := ev.(VoteRequest)
				if res.term > sm.currentTerm {
					sm.currentTerm = res.term 
					sm.votedFor = -1
					sm.state = follower
					sm.leaderId = -1
					flag = 1
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
				} else {

					action = append(action, Send{res.candidateId, VoteResponse{sm.currentTerm, false, sm.Id}})
					
				}
			case AppendEntriesResponse:
				res := ev.(AppendEntriesResponse)
				if res.term > sm.currentTerm {
					sm.state = follower
					sm.currentTerm = res.term
					sm.leaderId = -1
					flag = 1
					sm.votedFor = -1
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
					break
				}
				if !res.success {
					if nextIndex[res.from] > 0 {
						nextIndex[res.from] --
					} else {
						nextIndex[res.from] = 0
					}		
					break
					//continue
				}
				nextIndex[res.from]++
				matchIndex[res.from]++
				appended[sm.Id][sm.currentTerm]++
				if appended[sm.Id][sm.currentTerm] > sm.totalServers/2 {
					sm.Log[res.lastIndex].committed = true

					action = append(action, LogStore{sm.Id,sm.Log})
							
					fmt.Println("yo")
				
					sm.commitIndex++
					action = append(action, Send{clientId, Commit{sm.commitIndex,true}})
				}
			case Timeout:
				action = append(action, Send{sm.Id,ClientAppend{}})
			case ClientAppend:
					newEntry := LogEntry{sm.currentTerm, false, sm.lastLogIndex+1}
					sm.Log[sm.lastLogIndex+1] = newEntry
					sm.lastLogIndex ++
					sm.lastLogTerm = sm.Log[sm.lastLogIndex].term
					action = append(action, LogStore{sm.Id,sm.Log})
								
					for _, sId := range sm.peers {
						if sId != sm.Id {
							if sm.lastLogIndex >= nextIndex[sId] {
					
								action = append(action,Send{sId, AppendEntriesRequest{sm.currentTerm, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, true, sm.commitIndex}})
							}
						}
					}
							 
				
			}
			
	}
return action
}
	
func main() {
	
	p := make([]int,5)
	for i:=0 ; i<5; i++ {
		p[i] = i+1
	}
	
	log := make([]LogEntry,100)
	
	var sm = StateMachine{
		Id: 2, 
		state: candidate, 
		leaderId: -1, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 0, 
		lastLogIndex: 0, 
		lastLogTerm: 0, 
		//lastApplied: 0, 
		votedFor: -1, 
		commitIndex: 0, 
		Log: log}

	receivedVotes 	= make([][]int,sm.totalServers+1) 

	for i := range sm.peers {
        	receivedVotes[i] = make([]int, 10)
    	}

	yesVotes 	= make([][]int,sm.totalServers+1)
	for i := range sm.peers {
        	yesVotes[i] = make([]int, 10)
    	}

	noVotes 	= make([][]int,sm.totalServers+1)
	for i := range sm.peers {
        	noVotes[i] = make([]int, 10)
    	}

	appended = make([][]int, 10)
	for i := range sm.peers {
        	appended[i] = make([]int, 10)
    	}

	nextIndex = make([]int, 10)
	matchIndex = make([]int, sm.totalServers+1)
		
}

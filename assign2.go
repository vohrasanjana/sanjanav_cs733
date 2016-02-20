package main
import ("fmt"
	//"reflect"
	//"strconv"
)


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
type ClientResponse struct {
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
//var Events chan interface{}
type Signal struct {
	Content interface{}
}



//////////////////////////////////
/*
type Result struct {
	name	string
	event	resEvent
}

func getActionIndex(action []Action, i int) resEvent {
	var res resEvent
	act := action[i]
	act_name := reflect.TypeOf(act).Name()
	switch act_name {
		case "Send" :
			//to := act.(Send).dest
			y := reflect.TypeOf(act.(Send).rEvent)
			switch y.Name() {
				case "VoteResponse"	:
					res = act.(Send).rEvent.(VoteResponse)
					
				case "VoteRequest" 	:
					res = act.(Send).rEvent.(VoteRequest)
				case "AppendEntriesRequest" :
					res = act.(Send).rEvent.(AppendEntriesRequest)
				case "AppendEntriesResponse":
					res = act.(Send).rEvent.(AppendEntriesResponse)
				default :
					res = act
			}
		case "StateStore" :
			res = act.(StateStore)

		case "LogStore" :
			res = act.(LogStore)
		default :
			res = act
	}
	return res
}
func expect(a string, b string) bool {
	if a != b {
		fmt.Printf("Expected %s, found %s", b, a) 
		return false
	}
	return true
}

*/
///////////////////////////






func (sm *StateMachine) processEvents(ev interface{}) []Action {
						
action := make([]Action,0) 
flag = 0
switch sm.state {
	case candidate:
		
		 {
			//ev := <-Events
			switch ev.(type) {
				case Timeout:
					
					//sm.state = follower

					//fmt.Print("\n timeout")
					sm.currentTerm++
					sm.state = candidate
					//fmt.Print("\n timeout ", sm.currentTerm)
					
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
				/*case VoteRequest:
					res := ev.(VoteRequest)
					dest := res.candidateId
					 if res.term < sm.currentTerm {
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}


					if res.term > sm.currentTerm { // should by-pass next case
						sm.currentTerm = res.term
						sm.votedFor = -1
						sm.state = follower
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
					}

					if votedFor != -1 { //already voted-- will happen if not more recent
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}

					if res.lastLogTerm < sm.Log[len(sm.Log)-1].term { // not uptodate
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}

					if res.lastLogIndex < len(sm.Log)-1 { // old
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}
					sm.votedFor = res.candidateId
					sm.state = follower
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					action = append(action, Send{dest, VoteResponse{sm.currentTerm, true, sm.Id}})
				*/
				case VoteResponse:

					res := ev.(VoteResponse)
//					receivedVotes[sm.Id][sm.currentTerm] ++ ------caused error. IDK WHY
						
								
					if res.term != sm.currentTerm {
						
						break
						
					}

					if res.voteGranted {
						yesVotes[sm.Id][sm.currentTerm] ++
						//fmt.Println("vote Granted")
			
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
						//fmt.Println("vote NOT Granted")

						noVotes[sm.Id][sm.currentTerm]++
						if noVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 {
							sm.votedFor = -1
							sm.state = follower
							sm.leaderId = -1
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						}
						//action???
					}
				case AppendEntriesRequest:
					res := ev.(AppendEntriesRequest)
					if res.term < sm.currentTerm {
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
						//fmt.Println("here")
						break
					}//else {
					if res.term > sm.currentTerm {
						sm.currentTerm = res.term
						sm.votedFor = -1
						sm.leaderId = res.leaderId
						flag = 1
						sm.state = follower
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						//fmt.Println("here")
						
					}	

					if sm.Log[res.prevLogIndex].term != res.prevLogTerm {
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
						//fmt.Println("here")
						break
					}//else {
	
					if res.entries == false { //heartbeat
						sm.leaderId = res.leaderId
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, true, sm.lastLogIndex}})
					} else {
						//sm.currentTerm = res.term
						
						if res.prevLogIndex == 0 && sm.lastLogIndex == 0 { //empty log
							//fmt.Println("here")
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
			//ev := <-Events
			switch ev.(type) {
				case Timeout:
					sm.currentTerm++
					sm.state = candidate
					//fmt.Print("\n timeout")
					
					//receivedVotes[sm.Id][sm.currentTerm] = 0
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
		 
			//ev := <-Events
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
					//action = append(action, Send{res.leaderId, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex}})
					
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
					
					//action = append(action, Send{res.candidateId, VoteResponse{sm.currentTerm, false, sm.Id}})
					
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
					action = append(action, Send{clientId, ClientResponse{true}})
				}
			case Timeout:
			case ClientAppend:
					fmt.Println("hii")
					newEntry := LogEntry{sm.currentTerm, false, sm.lastLogIndex+1}
					sm.Log[sm.lastLogIndex+1] = newEntry
					sm.lastLogIndex ++
					sm.lastLogTerm = sm.Log[sm.lastLogIndex].term
					action = append(action, LogStore{sm.Id,sm.Log})
								
					for _, sId := range sm.peers {//heartbeats
						if sId != sm.Id {
							if sm.lastLogIndex >= nextIndex[sId] {
					
								action = append(action,Send{sId, AppendEntriesRequest{sm.currentTerm, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, true, sm.commitIndex}})
							}
						}
					}
							 
				
	/////handle AppendEntriesRequest/Response
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
	/*
	action := make([]Action,0)
	
	vr := VoteRequest{0,1,0,0}
	action =  sm.processEvents(vr) //first case
	
	if (candidateVoteRequest(sm, vr, action) != true) {
		fmt.Println("candidate vote request failed")
	}
	
	vr = VoteRequest{2,1,0,0}
	action =  sm.processEvents(vr) //second case
	//fmt.Println(sm.currentTerm, sm.votedFor,sm.state)
	*/
	
	
	
	
	
	
	/*
	//Events=make(chan interface{},10)
	
	p := make([]int,5)
	action := make([]Action,0)
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
	
	to := Timeout{}
	//Events <- to
	action = sm.processEvents(to)
	for _, act := range action {
		act_name := reflect.TypeOf(act).Name()
		switch act_name{
			case "Send" :
				to := act.(Send).dest
				y := reflect.TypeOf(act.rEvent)
				switch y.Name() {
					case "VoteResponse"	:
					case "VoteRequest" 	:
					case "AppendEntriesRequest" :
					case "AppendEntriesResponse":
				}
			case "StateStore" :
				act_s := act.(StateStore)
			case "LogStore" :
				
		}

	fmt.Println(sm.currentTerm, sm.votedFor,sm.state)

	action = make([]Action,0)
	vr := VoteRequest{1,1,0,0}
	action = sm.processEvents(vr)
	fmt.Println(sm.currentTerm, sm.votedFor,sm.state)
	fmt.Print("check")
	fmt.Print(action)
	//vr1 := VoteRequest{3,1,0,0}
	//action = append(action, sm.processEvents(vr1))
	/*vs := VoteResponse{1,true,2}
	action = append(action, sm.processEvents(vs))
	//fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state,receivedVotes[sm.Id][sm.currentTerm])
	vs = VoteResponse{1,true,3}
	action = append(action, sm.processEvents(vs))
	
	vs = VoteResponse{3,true,4}
	action = append(action, sm.processEvents(vs))	
	
	vs = VoteResponse{1,false,4}
	action = append(action, sm.processEvents(vs))	
	vs = VoteResponse{1,true,5}
	action = append(action, sm.processEvents(vs))
	//vs := VoteResponse{3,true,2}
	*/
	/*vs := VoteResponse{1,false,2}
	action = append(action, sm.processEvents(vs))
	//fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state,receivedVotes[sm.Id][sm.currentTerm])
	vs = VoteResponse{1,false,3}
	action = append(action, sm.processEvents(vs))
	
	vs = VoteResponse{3,false,4}
	action = append(action, sm.processEvents(vs))	
	
	vs = VoteResponse{1,false,4}
	action = append(action, sm.processEvents(vs))	
	vs = VoteResponse{1,true,5}
	action = append(action, sm.processEvents(vs))
	//vs := VoteResponse{3,true,2}
	
	//var data []int
	fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state, yesVotes[sm.Id][sm.currentTerm])
	ar := AppendEntriesRequest{0,1,0,0,true,0}
	//ar := AppendEntriesRequest{term:0, leaderId:1, prevLogIndex:0, prevLogTerm: 0, entries true, leaderCommit:0}
	
	action = append(action, sm.processEvents(ar))
	fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state, sm.leaderId)
	ar = AppendEntriesRequest{3,1,0,0,true,0}
	//ar := AppendEntriesRequest{term:3, leaderId:1, prevLogIndex:0, prevLogTerm: 0, entries true, leaderCommit:0}
	//ar = AppendEntriesRequest{3,1,0,1,true,0}
	
	action = append(action, sm.processEvents(ar))
	sm.state = leader
	fmt.Println("kkk")
	fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm)
	action = append(action, sm.processEvents(ClientAppend{}))
	fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm)
	action = append(action, sm.processEvents(ClientAppend{}))
	fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, sm.commitIndex)
	action = append(action, sm.processEvents(AppendEntriesResponse{3,1,true,2}))
	action = append(action, sm.processEvents(AppendEntriesResponse{3,3,true,2}))
	action = append(action, sm.processEvents(AppendEntriesResponse{3,4,true,2}))
	//action = append(action, sm.processEvents(AppendEntriesResponse{3,5,true,2}))
	fmt.Println(sm.Id, sm.currentTerm, sm.votedFor,sm.state, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, sm.commitIndex)
*/	
}

package main
import ("fmt"
	//"reflect"
	"time"
)


const (
	follower	int = 1
	candidate	int = 2
	leader		int = 3
	//clientId	int = 10000
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
	Dest		int		//serverId, -1 -> statestore/logStore
	REvent		resEvent
}
type resEvent interface{}
type LogEntry struct {
	Term 		int
	committed	bool
	index		int
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
//	data		[]byte
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
	Id		int
	state 		int
	LeaderId	int
	Term		int
}
type Alarm struct {
	timeout		int
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
	ElecTC		int
	ElecTF		int
	HeartT 		int
}

type Signal struct {
	Content interface{}
}

func (sm *StateMachine) PrintLog(){
	for i:=1;;i++{
		if sm.Log[i].index > 0 {
			fmt.Print("Log --- ",sm.Id,"   ")
			fmt.Println(sm.Log[i].Term,sm.Log[i].committed,sm.Log[i].index,string(sm.Log[i].data))
		} else {
			return
		}
	}
}



func (sm *StateMachine) processEvents(ev interface{}) []Action {
						
action := make([]Action,0) 
flag = 0
////fmt.Println(sm.state)	
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
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						action = append(action, Send{res.CandidateId, VoteResponse{sm.currentTerm, true, sm.Id}})
					}
					
				case VoteResponse:

					res := ev.(VoteResponse)
					//fmt.Println(sm.Id, " has been voted")
								
					if res.Term != sm.currentTerm {
						
						break
						
					}

					if res.VoteGranted {
						//fmt.Println("here also ",sm.Id,"  ",sm.currentTerm)
						yesVotes[sm.Id][sm.currentTerm] ++
						fmt.Println(sm.Id," got vote ----------------------", yesVotes[sm.Id][sm.currentTerm])
						if yesVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 {
							sm.state = leader
							sm.leaderId = sm.Id
							for i := 0; i<sm.totalServers; i++ {
								nextIndex[i] = len(sm.Log)
								matchIndex[i] = -1
							}
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
							fmt.Println(sm.Id," yaya i am the leader sending heartbeats")
							
							action = append(action, Alarm{sm.HeartT})
							for _, sId := range sm.peers {//heartbeats
								if sId != sm.Id {
									action = append(action,Send{sId, AppendEntriesRequest{sm.Id,sm.currentTerm, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, []byte{}, sm.commitIndex}})
								}
							}
		
		
						}
					} else {
						
						//fmt.Println("here ",sm.Id,"  ",sm.currentTerm)
						noVotes[sm.Id][sm.currentTerm]++
						if noVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 {
							sm.votedFor = -1
							sm.state = follower
							sm.leaderId = -1
							action = append(action,Alarm{sm.ElecTF})	
							action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
						}
						
					}

				case ClientAppend:
					
					fmt.Println(sm.Id,"i, candidate, got client append////////////////////////sending to ",sm.leaderId)
					action = append(action, Send{sm.leaderId, ClientAppend{}})

				case AppendEntriesRequest:
					res := ev.(AppendEntriesRequest)
					if len(res.Entries) > 0 {
					  fmt.Println(sm.Id, " got AppendEntryRequest!!")
						
					}
					if res.Term < sm.currentTerm {
						action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
						
						break
					}//else {
					if res.Term > sm.currentTerm {
						sm.currentTerm = res.Term
						sm.votedFor = -1
						sm.leaderId = res.LeaderId
						flag = 1
						sm.state = follower
						action = append(action,Alarm{sm.ElecTF})	
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})	
						
					}	

					if sm.Log[res.PrevLogIndex].Term != res.PrevLogTerm {
						action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
						action = append(action, Alarm{sm.ElecTF})
						break
					}//else {
					sm.commitIndex = res.LeaderCommit
					flag:= 0
					for i:= 1;i<=sm.commitIndex;i++ {
						if(sm.Log[i].committed != true) {

							sm.Log[i].committed = true
							flag = 1
						}
					}	
					if flag != 0 {
						action = append(action, LogStore{sm.Id,sm.Log})
					}
					if len(res.Entries) == 0 { //heartbeat
						sm.leaderId = res.LeaderId
						//sm.commitIndex = res.LeaderCommit
						fmt.Println(" heartbeat in ",sm.Id," from ",sm.leaderId)
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
						action = append(action, Alarm{sm.ElecTF}) // set election timeout
						//action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, true, sm.lastLogIndex}})
					} else {
						
						
						if res.PrevLogIndex == 0 && sm.lastLogIndex == 0 { //empty log
							newEntry := LogEntry{res.Term, false, res.PrevLogIndex+1,res.Entries}
							sm.Log[res.PrevLogIndex+1] = newEntry
							sm.lastLogIndex ++
							sm.lastLogTerm = sm.Log[sm.lastLogIndex].Term
							action = append(action, LogStore{sm.Id,sm.Log})
							action = append(action, Alarm{sm.ElecTF})
							action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
							//fmt.Println(sm.Id," log == ",sm.Log) 
							sm.PrintLog()
						} else if sm.lastLogIndex > 0 && sm.lastLogIndex >= res.PrevLogIndex {
							if sm.lastLogIndex == res.PrevLogIndex {
								if res.PrevLogTerm == sm.Log[res.PrevLogIndex].Term {
									sm.Log[res.PrevLogIndex +1] = LogEntry{res.Term, false, res.PrevLogIndex+1,res.Entries}
									sm.lastLogIndex ++
									sm.lastLogTerm = sm.Log[sm.lastLogIndex].Term
									action = append(action, LogStore{sm.Id,sm.Log})
									action = append(action, Alarm{sm.ElecTC})
									action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
									//fmt.Println(sm.Id," log == ",sm.Log) 
									sm.PrintLog()
									break
								} 
							}
						} else {
							fmt.Println("but cant store it ")
							action = append(action, Alarm{sm.ElecTC})
							action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
							//fmt.Println(sm.Id," log == ",sm.Log) 
							sm.PrintLog()
						}
							
					}
			}
		}
			
	case follower:
		{
			////fmt.Println(reflect.TypeOf(ev))
	
			switch ev.(type) {
				case Timeout:
					
					sm.currentTerm++
					sm.state = candidate
					fmt.Println(sm.Id," timed out")
					sm.votedFor = sm.Id
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					//action = append(action, Alarm{sm.ElecT})					
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
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
						
					}

					if sm.votedFor != -1 { //already voted
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}

					if res.LastLogTerm < sm.Log[sm.lastLogIndex].Term { // not uptodate
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}

					if res.LastLogIndex < sm.lastLogIndex { // old
						action = append(action, Send{dest, VoteResponse{sm.currentTerm, false, sm.Id}})
						break
					}
					sm.votedFor = res.CandidateId
					fmt.Println(sm.Id, " voting to ", dest)
					action = append(action, Alarm{sm.ElecTF})
					action = append(action, Send{dest, VoteResponse{sm.currentTerm, true, sm.Id}})
				

				case ClientAppend:
					
					fmt.Println(sm.Id,"i got client append//////////////////////////////////////////// sending to ",sm.leaderId)
					action = append(action, Send{sm.leaderId, ClientAppend{}})

				case AppendEntriesRequest:
					res := ev.(AppendEntriesRequest)
					if len(res.Entries) > 0 {
					  fmt.Println(sm.Id, " got AppendEntryRequest!!")
						
					}
					if res.Term < sm.currentTerm {
						action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
						fmt.Println("1")
						break
					} //else 
					//action = append(action, Alarm{sm.ElecTF})
					if res.Term > sm.currentTerm {
						sm.currentTerm = res.Term
						sm.leaderId = res.LeaderId
						flag = 1
						action = append(action, Alarm{sm.ElecTF})
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
						fmt.Println("2")
					}
					if sm.Log[res.PrevLogIndex].Term != res.PrevLogTerm {
						fmt.Println(sm.Id,res.From, sm.Log[res.PrevLogIndex].Term, "----", res.PrevLogTerm)
						action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
						action = append(action, Alarm{sm.ElecTF})
						fmt.Println("3")
						break
					}	
					if len(res.Entries) == 0 { //heartbeat
						sm.leaderId = res.LeaderId
						//fmt.Println("for me --",sm.Id,"-- leader is -- ",sm.leaderId) 
						action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
						action = append(action, Alarm{sm.ElecTF})
						sm.commitIndex = res.LeaderCommit
						for i:= 1;i<=sm.commitIndex;i++ {
							if(sm.Log[i].committed != true) {

							sm.Log[i].committed = true
							flag = 1
							}
						}	
						if flag != 0 {
							action = append(action, LogStore{sm.Id,sm.Log})
						}
						//fmt.Println("4")
						//action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, true, sm.lastLogIndex}})
					} else {
						sm.currentTerm = res.Term
						sm.commitIndex = res.LeaderCommit
						for i:= 1;i<=sm.commitIndex;i++ {
							if(sm.Log[i].committed != true) {

								sm.Log[i].committed = true
								flag = 1
							}
						}	
						if flag != 0 {
							action = append(action, LogStore{sm.Id,sm.Log})
						}
						if res.PrevLogIndex == 0 && sm.lastLogIndex == 0 { //empty log
							newEntry := LogEntry{res.Term, false, res.PrevLogIndex+1,res.Entries}
							sm.Log[res.PrevLogIndex+1] = newEntry
							sm.lastLogIndex ++
							sm.lastLogTerm = sm.Log[sm.lastLogIndex].Term	
							action = append(action, LogStore{sm.Id,sm.Log})
							action = append(action, Alarm{sm.ElecTF})
							action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
							//fmt.Println(sm.Id," log == ",sm.Log) 
							sm.PrintLog()
							//fmt.Println("5")
						} else if sm.lastLogIndex > 0 && sm.lastLogIndex >= res.PrevLogIndex {
							if sm.lastLogIndex == res.PrevLogIndex {
								if res.PrevLogTerm == sm.Log[res.PrevLogIndex].Term {
										sm.Log[res.PrevLogIndex +1] = LogEntry{res.Term, false, res.PrevLogIndex+1,res.Entries}
										sm.lastLogIndex ++
										sm.lastLogTerm = sm.Log[sm.lastLogIndex].Term
										action = append(action, LogStore{sm.Id,sm.Log})
										action = append(action, Send{res.From, AppendEntriesResponse{sm.lastLogTerm, sm.Id, true, sm.lastLogIndex,res.Entries}})
										action = append(action, Alarm{sm.ElecTF})
										//fmt.Println(sm.Id," log == ",sm.Log) 
										sm.PrintLog()
								} else {
									fmt.Println(" but can't store it ")
									action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
									action = append(action, Alarm{sm.ElecTF})
									//fmt.Println(sm.Id," log == ",sm.Log) 
									sm.PrintLog()
								}
							} else{
								fmt.Println(" but can't store it ")
								action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
								action = append(action, Alarm{sm.ElecTF})
							}
						} else{ 
							fmt.Println(" but can't store it ")
							action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
							action = append(action, Alarm{sm.ElecTF})
							fmt.Println(sm.Id," log == ",sm.Log) 
							sm.PrintLog()		
						}
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
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
				} else {
					action = append(action, Send{res.From, AppendEntriesResponse{sm.currentTerm, sm.Id, false, sm.lastLogIndex,res.Entries}})
					
				}
			case VoteRequest:
				res := ev.(VoteRequest)
				if res.Term > sm.currentTerm {
					sm.currentTerm = res.Term 
					sm.votedFor = -1
					sm.state = follower
					sm.leaderId = -1
					flag = 1
					action = append(action,Alarm{sm.ElecTF})	
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
				} else {

					action = append(action, Send{res.CandidateId, VoteResponse{sm.currentTerm, false, sm.Id}})
					
				}
			case AppendEntriesResponse:
				res := ev.(AppendEntriesResponse)
				fmt.Println(sm.Id," got a response ",res.Success)
				if res.Term > sm.currentTerm {
					sm.state = follower
					sm.currentTerm = res.Term
					sm.leaderId = -1
					flag = 1
					sm.votedFor = -1
					action = append(action,Alarm{sm.ElecTF})	
					action = append(action, StateStore{sm.Id,sm.state,sm.leaderId,sm.currentTerm})
					
					break
				}
				if !res.Success {
					if nextIndex[res.From] > 0 {
						nextIndex[res.From] --
					} else {
						nextIndex[res.From] = 0
					}		
					break
					
				}
				nextIndex[res.From]++
				////fmt.Println("yayyyyy",res.From,nextIndex[res.From])
				//matchIndex[res.From]++
				//fmt.Println("hereeeeee: ", appended[sm.Id][sm.currentTerm]+1)
				appended[sm.Id][sm.currentTerm]++
				if appended[sm.Id][sm.currentTerm] > sm.totalServers/2 {
					sm.Log[res.LastIndex].committed = true

					action = append(action, LogStore{sm.Id,sm.Log})
							
					////fmt.Println("yo")
				
					sm.commitIndex++
					action = append(action, Commit{sm.commitIndex,true,res.Data})
				}
			case Timeout:
				//fmt.Println(sm.Id," sending heartbeats")
				for _, sId := range sm.peers {//heartbeats
					if sId != sm.Id {
						action = append(action,Send{sId, AppendEntriesRequest{sm.Id, sm.currentTerm, sm.leaderId, sm.lastLogIndex, sm.lastLogTerm, []byte{}, sm.commitIndex}})
					}
				}
				action = append(action, Alarm{sm.HeartT})
			case ClientAppend:
				res := ev.(ClientAppend)
				appended[sm.Id][sm.currentTerm] = 0
				newEntry := LogEntry{sm.currentTerm, false, sm.lastLogIndex+1,res.Data}
				sm.Log[sm.lastLogIndex+1] = newEntry
				lastInd := sm.lastLogIndex
				sm.lastLogIndex ++
				lastTerm := sm.lastLogTerm
				sm.lastLogTerm = sm.Log[sm.lastLogIndex].Term
				action = append(action, LogStore{sm.Id,sm.Log})
				fmt.Println("yuhoooooo!!!!!///////////////////--leader got append ",sm.Id,time.Now())
				//action = append(action, Alarm{sm.HeartT})
				
				for _, sId := range sm.peers {
					if sId != sm.Id {
						//if sm.lastLogIndex >= nextIndex[sId] {
							fmt.Println(sm.Id, "sending to ",sId)
							action = append(action,Send{sId, AppendEntriesRequest{sm.Id, sm.currentTerm, sm.leaderId, lastInd, lastTerm, res.Data, sm.commitIndex}})
						
						//}
					}
				}
			
				action = append(action, Alarm{sm.HeartT})
							 
				
			}
			
	}
return action
}

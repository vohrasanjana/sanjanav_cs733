package main
import ("strconv"
	//"time"
	"testing"
	"fmt"
	"reflect")

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
				case "ClientAppend":
					res = act.(Send).rEvent.(ClientAppend)
					
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
		fmt.Println("Expected %v, found %v", b, a) // t.Error is visible when running `go test -verbose`
		return false
	}
	return true
}

func serverTimeout(sm StateMachine,action []Action) bool {
	r := getActionIndex(action,0)
	count := 0
	if (reflect.TypeOf(r).Name() == "StateStore") {
		x := r.(StateStore)
		if (expect(strconv.Itoa(x.state),strconv.Itoa(candidate))) && (expect(strconv.Itoa(x.term),"1")) && (expect(strconv.Itoa(sm.votedFor),strconv.Itoa(sm.Id))) {
			if len(action) == sm.totalServers {
			
				for i:=1; i< sm.totalServers; i++ {
					r = getActionIndex(action, i)
					if (reflect.TypeOf(r).Name() == "VoteRequest") {					
						count++
					}
				}
				if (count == sm.totalServers-1) {
					return true
				}
			}
		}				
	}
	return false
}


func candidateVoteRequest(sm StateMachine, vr VoteRequest, action []Action) bool {
	

	r := getActionIndex(action,0)
	if (vr.term == 0) { // first case
		if reflect.TypeOf(r).Name() == "VoteResponse" {
			x := r.(VoteResponse)
			return expect(strconv.FormatBool(x.voteGranted),"false")
		}
	} else {
		if reflect.TypeOf(r).Name() == "StateStore" {
			x := r.(StateStore)
			if (expect(strconv.Itoa(x.state),strconv.Itoa(follower))) && (expect(strconv.Itoa(x.term),strconv.Itoa(vr.term))) && (expect(strconv.Itoa(sm.votedFor),strconv.Itoa(vr.candidateId))) {
				r = getActionIndex(action,1)
				if reflect.TypeOf(r).Name() == "VoteResponse" {
					x := r.(VoteResponse)
					return (expect(strconv.FormatBool(x.voteGranted),"true"))
				}
			}
		}
		
	}
	return false 
}
func candidateVoteResponse(sm StateMachine, vr VoteResponse, action []Action) bool {
			
	count := 0
	if len(action) == 0 { // not for this term OR vote considered, but not majority
		return vr.term == 5 ||  yesVotes[sm.Id][sm.currentTerm] <= sm.totalServers/2 || noVotes[sm.Id][sm.currentTerm] <= sm.totalServers/2
	}
	
	r := getActionIndex(action,0) //majority yes or no
	if reflect.TypeOf(r).Name() == "StateStore" {
		x := r.(StateStore)
		if vr.voteGranted == true { // majority yes, turned leader and sent heartbeats
		
			if yesVotes[sm.Id][sm.currentTerm] > sm.totalServers/2 && (expect(strconv.Itoa(x.state),strconv.Itoa(leader))) && (expect(strconv.Itoa(sm.leaderId),strconv.Itoa(sm.Id))) {
				if len(action) == sm.totalServers {
					for i:=1; i< sm.totalServers; i++ {
						r = getActionIndex(action, i)
						if (reflect.TypeOf(r).Name() == "AppendEntriesRequest") {  //n-1 heartbeats
							count++
						}
					}
					if (count == sm.totalServers-1) {
						return true
					}
				}
			}
		} else { // majority NO
			
			return (expect(strconv.Itoa(x.state),strconv.Itoa(follower))) && (expect(strconv.Itoa(x.leaderId),strconv.Itoa(-1))) && (expect(strconv.Itoa(sm.votedFor),strconv.Itoa(-1)))
		}
	}
	return false
}

func serverAppendRequest(sm StateMachine, ap AppendEntriesRequest, action []Action) bool {
	
	r := getActionIndex(action,0)
	if ap.term < sm.currentTerm {
		if reflect.TypeOf(r).Name() == "AppendEntriesResponse" { 
			x := r.(AppendEntriesResponse)
			return expect(strconv.FormatBool(x.success),"false")
		}
	}
	if flag == 1 {
			
		if reflect.TypeOf(r).Name() == "StateStore" {
			x := r.(StateStore)
			
			if (expect(strconv.Itoa(x.state),strconv.Itoa(follower))) && (expect(strconv.Itoa(x.term),strconv.Itoa(ap.term))) && (expect(strconv.Itoa(sm.votedFor),strconv.Itoa(-1))) && (expect(strconv.Itoa(sm.leaderId),strconv.Itoa(ap.leaderId))) {
			
				r = getActionIndex(action,1)
				
			} else {
				return false 
			}
		} else {
			return false
		}
	}
	if sm.Log[ap.prevLogIndex].term != ap.prevLogTerm {
		if reflect.TypeOf(r).Name() == "AppendEntriesResponse" {
			
			x := r.(AppendEntriesResponse)
			return expect(strconv.FormatBool(x.success),"false")
		} 
		return false
	}
	if ap.entries == false { //heartbeat
			
		if reflect.TypeOf(r).Name() == "StateStore" {
			x := r.(StateStore)

			if (expect(strconv.Itoa(x.leaderId),strconv.Itoa(ap.leaderId))) && (expect(strconv.Itoa(sm.leaderId),strconv.Itoa(ap.leaderId))){
				if flag ==1 {
					r = getActionIndex(action,2)						

				} else {
					r = getActionIndex(action,1)
				}
			} else {
				return false
			}
		} else {
			return false
		}
		if reflect.TypeOf(r).Name() == "AppendEntriesResponse" {
			x := r.(AppendEntriesResponse)
			return expect(strconv.FormatBool(x.success),"true")
		}
		return false 
	} else {
		if ap.prevLogIndex == 0 && sm.lastLogIndex == 0 {
			if reflect.TypeOf(r).Name() == "LogStore" {
				r = getActionIndex(action,1)
			} else {
				return false
			}
			if reflect.TypeOf(r).Name() == "AppendEntriesResponse" {
				x := r.(AppendEntriesResponse)
				return expect(strconv.FormatBool(x.success),"true")
			}
			return false
		} else if sm.lastLogIndex == ap.prevLogIndex {
			
			if ap.prevLogTerm == sm.Log[ap.prevLogIndex].term {
				if reflect.TypeOf(r).Name() == "LogStore" {
					if flag ==1 {
						r = getActionIndex(action,2)					
					} else {
						r = getActionIndex(action,1)
					}
				} else {
					return false
				}
				if reflect.TypeOf(r).Name() == "AppendEntriesResponse" {
				x := r.(AppendEntriesResponse)
				return expect(strconv.FormatBool(x.success),"true")
				} else {
					return false
				}
			}
		}
		if reflect.TypeOf(r).Name() == "AppendEntriesResponse" {
			x := r.(AppendEntriesResponse)
			return expect(strconv.FormatBool(x.success),"false")
		} else {
			return false
		}
		
	}
	return false
}

func followerVoteRequest(sm StateMachine, vr VoteRequest, action []Action) bool {
	
	r := getActionIndex(action,0)
	
	if vr.term < sm.currentTerm {
		if reflect.TypeOf(r).Name() == "VoteResponse" {
			x := r.(VoteResponse)
			return expect(strconv.FormatBool(x.voteGranted),"false")
		} 
		return false
	}
	if flag == 1 {
		if reflect.TypeOf(r).Name() == "StateStore" {
			r= getActionIndex(action,1)
		} else {
			return false
		}
	}
	if sm.votedFor != vr.candidateId {
		if reflect.TypeOf(r).Name() == "VoteResponse" {
			x := r.(VoteResponse)
			return expect(strconv.FormatBool(x.voteGranted),"false")
		} 
		return false
	}
	if reflect.TypeOf(r).Name() == "VoteResponse" {
			x := r.(VoteResponse)
			return expect(strconv.FormatBool(x.voteGranted),"true")
	} 
	return false
}


func leaderVoteRequest(sm StateMachine, vr VoteRequest, action []Action) bool {
	r := getActionIndex(action,0)
	if flag == 1 {
		if reflect.TypeOf(r).Name() == "StateStore" {
			x := r.(StateStore)
			return ((expect(strconv.Itoa(x.leaderId),strconv.Itoa(-1))) && (expect(strconv.Itoa(x.state),strconv.Itoa(follower))))
		}
		return false
	} else {
		if reflect.TypeOf(r).Name() == "VoteResponse" {
			x := r.(VoteResponse)
			return (expect(strconv.FormatBool(x.voteGranted),"false"))
		}
		return false
	}
}
	
func leaderAppendRequest(sm StateMachine, ap AppendEntriesRequest, action []Action) bool {
	r := getActionIndex(action,0)		
	if sm.leaderId == -1 {
		
		if reflect.TypeOf(r).Name() == "StateStore" {
			x := r.(StateStore)
			return ((expect(strconv.Itoa(x.leaderId),strconv.Itoa(-1))) && (expect(strconv.Itoa(x.state),strconv.Itoa(follower))))
		}
		return false
	} else {
		
		if reflect.TypeOf(r).Name() == "AppendEntriesResponse" {
			x := r.(AppendEntriesResponse)
			return (expect(strconv.FormatBool(x.success),"false"))
		}
		return false
	}
}

func leaderAppendResponse(sm StateMachine, ar AppendEntriesResponse, action []Action) bool {
	ni := nextIndex[ar.from]
	mi := matchIndex[ar.from]
	ap := appended[sm.Id][sm.currentTerm]
	action = sm.processEvents(ar)
	r := getActionIndex(action,0)		
	if flag == 1 {
		if reflect.TypeOf(r).Name() == "StateStore" {
			x := r.(StateStore)
			r = getActionIndex(action,1)		
			
			return ((expect(strconv.Itoa(x.leaderId),strconv.Itoa(-1))) && (expect(strconv.Itoa(x.state),strconv.Itoa(follower))))
		}
		return false
	}
	if !ar.success {
		if nextIndex[ar.from] > 0 {
			return (ni-1) == nextIndex[ar.from]
		}
		return true
	}
	if appended[sm.Id][sm.currentTerm] <= sm.totalServers/2 {
		return (nextIndex[ar.from] == (ni+1)) && (matchIndex[ar.from] == (mi+1)) && (appended[sm.Id][sm.currentTerm] == (ap+1))
	} else {
		if reflect.TypeOf(r).Name() == "LogStore" {
			r := getActionIndex(action,1)		
			if flag == 1 {
				r = getActionIndex(action,2)
			}
			return reflect.TypeOf(r).Name() == "Commit"
		}
		return false
	}
}

func leaderTimeout(sm StateMachine, action []Action) bool {
	r := getActionIndex(action, 0)
	return reflect.TypeOf(r).Name() == "ClientAppend"
}		
				
func testClientAppend(sm StateMachine,action []Action) bool {
	if len(action) > 0 {
			
		r := getActionIndex(action,0)		
		if sm.state == leader {
			
			count := 0
			if reflect.TypeOf(r).Name() == "LogStore" {
				if len(action) == sm.totalServers {
					for i:=1; i< sm.totalServers; i++ {
						r = getActionIndex(action, i)
						if (reflect.TypeOf(r).Name() == "AppendEntriesRequest") {  //n-1
								count++
						}
					}
					if (count == sm.totalServers-1) {
							return true
					}
				}
			}
			return false
		} else {
			return reflect.TypeOf(r).Name() == "ClientAppend" 
		}
	}
			
	return false
}
			


	


func TestCandidate(t *testing.T) {
	p := make([]int,5)
	for i:=0 ; i<5; i++ {
		p[i] = i+1
	}
	
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

	receivedVotes := make([][]int,10) 

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

	action = sm.processEvents(ClientAppend{})
	if(!testClientAppend(sm,action)) {
		fmt.Println("client append on candidate failed")
	}


	//-----------------------------------testing for candidate timeout
	
	action =  sm.processEvents(Timeout{})
	
	if(serverTimeout(sm, action) != true) {
		fmt.Println("candidate timeout failed")
	}
	//-----------------------------------testing for candidate vote request


	vr := VoteRequest{0,1,0,0}
	action =  sm.processEvents(vr) //first case
	
	if (candidateVoteRequest(sm, vr, action) != true) {
		fmt.Println("candidate vote request failed")
	}
	vr = VoteRequest{2,1,0,0}
	action =  sm.processEvents(vr) //second case
	
	if (candidateVoteRequest(sm, vr, action) != true) {
		fmt.Println("candidate vote request failed")
	}
	//-----------------------------------testing for candidate vote response
	
	//action =  sm.processEvents(Timeout{}) //-- to ensure candidate state
	sm = StateMachine{
		Id: 2, 
		state: candidate, 
		leaderId: -1, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 1, 
		lastLogIndex: 0, 
		lastLogTerm: 0, 
		//lastApplied: 0, 
		votedFor: 2, 
		commitIndex: 0, 
		Log: log}
	
	vs := VoteResponse{5,false,1}
	
	action =  sm.processEvents(vs) //first case
	if(!candidateVoteResponse(sm, vs, action)) {
		fmt.Println("Candidate vote response failed")
	}
	action = sm.processEvents(VoteResponse{1,true,1})
	if (!candidateVoteResponse(sm, VoteResponse{1,true,1}, action) && yesVotes[sm.Id][sm.currentTerm] == 1) {
		fmt.Println("Candidate vote response failed")
	}
	action = sm.processEvents(VoteResponse{1,true,3})
	action = sm.processEvents(VoteResponse{1,false,4})
	if (!candidateVoteResponse(sm, VoteResponse{1,false,4}, action) && noVotes[sm.Id][sm.currentTerm] == 1 && yesVotes[sm.Id][sm.currentTerm] == 2) { //one negative vote, non-majority yes
		fmt.Println("Candidate vote response failed")
	}
	action = sm.processEvents(VoteResponse{1,true,5})// majority yes
	if (!candidateVoteResponse(sm, VoteResponse{1,true,5}, action)) {
			fmt.Println("Candidate vote response failed")
	}
	sm.state = candidate
	sm.currentTerm = 2
	action = sm.processEvents(VoteResponse{2,false,1})
	action = sm.processEvents(VoteResponse{2,false,3})
	
	
	action = sm.processEvents(VoteResponse{2,true,4})
	action = sm.processEvents(VoteResponse{2,false,5})
	if (!candidateVoteResponse(sm, VoteResponse{2,false,5}, action)) {
		fmt.Println("Candidate vote response failed")
	}
	
	

//------------------------------candidate AppendRequest testing
	sm = StateMachine{
		Id: 2, 
		state: candidate, 
		leaderId: -1, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 2, 
		lastLogIndex: 0, 
		lastLogTerm: 0, 
		//lastApplied: 0, 
		votedFor: 2, 
		commitIndex: 0, 
		Log: log}
	
	action = sm.processEvents(AppendEntriesRequest{2,3,0,2,true,0})//case 5 empty log logstore & appendentriesResponse
sm.Log[0] = LogEntry{1,false,0}
sm.Log[1] = LogEntry{2,false,1}
sm.Log[2] = LogEntry{2,false,2}

	action = sm.processEvents(AppendEntriesRequest{1,3,0,0,false,0})//case 1
	if (!serverAppendRequest(sm, AppendEntriesRequest{1,3,0,0,false,0}, action)) {
		fmt.Println("Candidate AppendEntriesRequest failed")
	}
	action = sm.processEvents(AppendEntriesRequest{3,3,2,3,false,0})//case 2 & 3  statestore + false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{3,3,2,3,false,0}, action)) {
		fmt.Println("Candidate AppendEntriesRequest failed -2")
	}
	sm.state = candidate // was changed by earlier
	action = sm.processEvents(AppendEntriesRequest{3,3,2,2,false,0})//case 2 & 4
	if (!serverAppendRequest(sm, AppendEntriesRequest{3,3,2,2,false,0}, action)) {
		fmt.Println("Candidate AppendEntriesRequest failed -3")
	}
	sm.state = candidate // was changed by earlier
	sm.lastLogIndex = 2
	action = sm.processEvents(AppendEntriesRequest{2,3,2,2,false,0}) //case 6  logstore &resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,2,2,false,0}, action)) {
		fmt.Println("Candidate AppendEntriesRequest failed -3")
	}
	action = sm.processEvents(AppendEntriesRequest{2,3,1,1,false,0}) //  false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,1,1,false,0}, action)) {
		fmt.Println("Candidate AppendEntriesRequest failed -4")
	}
	action = sm.processEvents(AppendEntriesRequest{2,3,2,1,false,0}) //false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,2,1,false,0}, action)) {
		fmt.Println("Candidate AppendEntriesRequest failed -5")
	}
	action = sm.processEvents(AppendEntriesRequest{2,3,4,2,false,0}) //false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,4,2,false,0}, action)) {
		fmt.Println("Candidate AppendEntriesRequest failed -6")
	}
	
}	

func TestFollower(t *testing.T) {	
	
	p := make([]int,5)
	for i:=0 ; i<5; i++ {
		p[i] = i+1
	}
	
	action := make([]Action,0)
	log := make([]LogEntry,100)
	
	var sm = StateMachine{
		Id: 2, 
		state: follower, 
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

	receivedVotes := make([][]int,10) 

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

	action = sm.processEvents(ClientAppend{})
	if(!testClientAppend(sm,action)) {
		fmt.Println("client append on follower failed")
	}


	//-----------------------------------testing for follower timeout
	

	action =  sm.processEvents(Timeout{})
	
	if(serverTimeout(sm, action) != true) {
		fmt.Println("follower timeout failed")
	}
	//-----------------------------------testing voterequest on follower
	sm = StateMachine{
		Id: 2, 
		state: follower, 
		leaderId: -1, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 2, 
		lastLogIndex: 0, 
		lastLogTerm: 0, 
		//lastApplied: 0, 
		votedFor: -1, 
		commitIndex: 0, 
		Log: log}
	action = sm.processEvents(VoteRequest{1,1,0,0}) //case 1
	if(!followerVoteRequest(sm,VoteRequest{1,1,0,0},action)) {
		fmt.Println("follower vote request failed")
	}
	sm.votedFor = 4 //case 2
	action = sm.processEvents(VoteRequest{3,1,0,0})
	if(!followerVoteRequest(sm,VoteRequest{3,1,0,0},action)) {
		fmt.Println("follower vote request failed")
	}
	sm.votedFor = -1
	sm.lastLogIndex = 2
	sm.Log[2].term = 3
	action = sm.processEvents(VoteRequest{2,1,2,1})//case 3
	if(!followerVoteRequest(sm,VoteRequest{2,1,2,1},action)) {
		fmt.Println("follower vote request failed")
	}
	action = sm.processEvents(VoteRequest{2,1,1,3})//case 4
	if(!followerVoteRequest(sm,VoteRequest{2,1,1,3},action)) {
		fmt.Println("follower vote request failed")
	}
	action = sm.processEvents(VoteRequest{2,1,2,4})//case 5 --voteGranted
	if(!followerVoteRequest(sm,VoteRequest{2,1,2,4},action)) {
		fmt.Println("follower vote request failed")
	}
	

		
	//-----------------------------------testing for append entry request on follower
sm = StateMachine{
		Id: 2, 
		state: follower, 
		leaderId: -1, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 2, 
		lastLogIndex: 0, 
		lastLogTerm: 0, 
		//lastApplied: 0, 
		votedFor: -1, 
		commitIndex: 0, 
		Log: log}
	
	action = sm.processEvents(AppendEntriesRequest{2,3,0,2,true,0})//case 5 empty log logstore & appendentriesResponse
sm.Log[0] = LogEntry{1,false,0}
sm.Log[1] = LogEntry{2,false,1}
sm.Log[2] = LogEntry{2,false,2}

	action = sm.processEvents(AppendEntriesRequest{1,3,0,0,false,0})//case 1
	if (!serverAppendRequest(sm, AppendEntriesRequest{1,3,0,0,false,0}, action)) {
		fmt.Println("follower AppendEntriesRequest failed")
	}
	action = sm.processEvents(AppendEntriesRequest{3,3,2,3,false,0})//case 2 & 3  statestore + false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{3,3,2,3,false,0}, action)) {
		fmt.Println("follower AppendEntriesRequest failed -2")
	}
	sm.state = follower // was changed by earlier
	action = sm.processEvents(AppendEntriesRequest{3,3,2,2,false,0})//case 2 & 4
	if (!serverAppendRequest(sm, AppendEntriesRequest{3,3,2,2,false,0}, action)) {
		fmt.Println("follower AppendEntriesRequest failed -3")
	}
	sm.state = follower // was changed by earlier
	sm.lastLogIndex = 2
	action = sm.processEvents(AppendEntriesRequest{2,3,2,2,false,0}) //case 6  logstore &resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,2,2,false,0}, action)) {
		fmt.Println("follower AppendEntriesRequest failed -7")
	}
	action = sm.processEvents(AppendEntriesRequest{2,3,1,1,false,0}) //  false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,1,1,false,0}, action)) {
		fmt.Println("follower AppendEntriesRequest failed -4")
	}
	action = sm.processEvents(AppendEntriesRequest{2,3,2,1,false,0}) //false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,2,1,false,0}, action)) {
		fmt.Println("follower AppendEntriesRequest failed -5")
	}
	action = sm.processEvents(AppendEntriesRequest{2,3,4,2,false,0}) //false resp
	if (!serverAppendRequest(sm, AppendEntriesRequest{2,3,4,2,false,0}, action)) {
		fmt.Println("follower AppendEntriesRequest failed -6")
	}


}	

func TestLeader(t *testing.T) {
	p := make([]int,5)
	for i:=0 ; i<5; i++ {
		p[i] = i+1
	}
	
	action := make([]Action,0)
	log := make([]LogEntry,100)
	
	var sm = StateMachine{
		Id: 2, 
		state: leader, 
		leaderId: 2, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 2, 
		lastLogIndex: 0, 
		lastLogTerm: 0, 
		//lastApplied: 0, 
		votedFor: -1, 
		commitIndex: 0, 
		Log: log}

	receivedVotes := make([][]int,10) 

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


	action = sm.processEvents(ClientAppend{})
	if(!testClientAppend(sm,action)) {
		fmt.Println("client append on leader failed")
	}
	action = sm.processEvents(Timeout{})
	if(!leaderTimeout(sm,action)) {
		fmt.Println("timeout on leader failed")
	}


	
	action = sm.processEvents(VoteRequest{1,1,0,0}) //case 2
	if(!leaderVoteRequest(sm,VoteRequest{1,1,0,0},action)) {
		fmt.Println("leader vote request failed")
	}
	action = sm.processEvents(AppendEntriesRequest{1,1,0,0,false,0}) //case 2
	if(!leaderAppendRequest(sm,AppendEntriesRequest{1,1,0,0,false,0},action)) {
		fmt.Println("leader append request failed - 2")
	}

	action = sm.processEvents(VoteRequest{3,1,0,0}) //case 1
	if(!leaderVoteRequest(sm,VoteRequest{3,1,0,0},action)) {
		fmt.Println("leader vote request failed")
	}
	
	sm = StateMachine{
		Id: 2, 
		state: leader, 
		leaderId: 2, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 2, 
		lastLogIndex: 0, 
		lastLogTerm: 0, 
		//lastApplied: 0, 
		votedFor: -1, 
		commitIndex: 0, 
		Log: log}

	action = sm.processEvents(AppendEntriesRequest{3,1,0,0,false,0}) //case 1
	if(!leaderAppendRequest(sm,AppendEntriesRequest{3,1,0,0,false,0},action)) {
		fmt.Println("leader append request failed - 1")
	}

	/*
	sm = StateMachine{
		Id: 2, 
		state: leader, 
		leaderId: 2, 
		peers: p, 
		totalServers: 5, 
		currentTerm: 3, 
		lastLogIndex: 2, 
		lastLogTerm: 3, 
		//lastApplied: 0, 
		votedFor: -1, 
		commitIndex: 1, 
		Log: log}
sm.Log[0] = LogEntry{1,true,0}
sm.Log[1] = LogEntry{2,true,1}
sm.Log[2] = LogEntry{3,false,2}


	action = make([]Action,0)
	nextIndex[1] = 2

	if (!leaderAppendResponse(sm,AppendEntriesResponse{3,1,false,3}, action)) {
		fmt.Println("leader append response failed - 1")
	}
	
	if (!leaderAppendResponse(sm,AppendEntriesResponse{3,1,true,3}, action)) {
		fmt.Println("leader append response failed - 1")
	}
	if (!leaderAppendResponse(sm,AppendEntriesResponse{3,3,true,3}, action)) {
		fmt.Println("leader append response failed - 1")
	}
	if (!leaderAppendResponse(sm,AppendEntriesResponse{3,4,true,3}, action)) {
		fmt.Println("leader append response failed - 1")
	}
	if (!leaderAppendResponse(sm,AppendEntriesResponse{3,5,true,3}, action)) {
		fmt.Println("leader append response failed - 1")
	}

	if (!leaderAppendResponse(sm,AppendEntriesResponse{5,1,true,3}, action)) {
		fmt.Println("leader append response failed - 1")
	}
*/
}	

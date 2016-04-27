package raft
import 	("time"
	 "testing"
	 "strconv"
	 "os"
	 "fmt"
	)
/*func Expect(rafts []*RaftNode, data string, t *testing.T) (bool) {
	for _, node:= range rafts{
		select {
			case com := <-node.CommitChannel():
				if string(com.Data) != data {
					return false
				}
			default:t.Fatal("Expected message on all nodes",data)
				return false
		}
	}
	return true
}*/
func Match(rafts []*RaftNode,ci int) bool {
	for i:=1;i<=NUMRAFTS;i++{
		if rafts[i-1].sm.commitIndex != ci-1 {
			return false
		}
	}
	return true
}
func checkCommit(rafts []*RaftNode,Index int,checkFor ...int) {
	C:=3
	if len(checkFor) > 0 {
    	C = checkFor[0]
  	}
	smid := []int{0,1,2,3,4}

	flag := 0

	for flag < C{
		fmt.Print("")

		for ii := 0; ii < len(smid); ii++ {
			 if rafts[smid[ii]].sm.commitIndex == Index-1{
				smid = append(smid[:ii], smid[ii+1:]...)
				flag++
				ii = 0
				
			}

		}
		time.Sleep(time.Duration(200)*time.Millisecond)
					
	}
	time.Sleep(time.Duration(200)*time.Millisecond)
	
}
func Test_Start(t *testing.T){

	for i:=1;i<=NUMRAFTS;i++{
			os.RemoveAll("state_"+strconv.Itoa(i))
			os.RemoveAll("rflog"+strconv.Itoa(i))
	}

}
func TestRN(t *testing.T) {
	rafts,clust, err := makeRafts()
	if err != nil {
		t.Fatal("makerafts failed")
	}
	time.Sleep(time.Duration(100) * time.Millisecond)
	
	for i:=1;i<=20;i++ {
		leader := LeaderId(rafts)
		rafts[leader-1].Append([]byte(strconv.Itoa(i)))
	}
	checkCommit(rafts,20)
	chk := Match(rafts,20)
	if chk != true {
		t.Fatal("ERROR!! EXPECTED 20 APPENDS")
	} else{
		fmt.Println("SUCCESSFUL - BASIC APPENDS\n")
	}


	leader := LeaderId(rafts)
	if leader != NUMRAFTS {
		rafts[leader].eventCh <- Timeout{}
	} else {
		rafts[leader-2].eventCh <- Timeout{}
	}		
	time.Sleep(time.Duration(100) * time.Millisecond)
	leader = LeaderId(rafts)
	if leader != NUMRAFTS { // sending to non-leader
		rafts[leader-1].Append([]byte("are")) 
	} else {
		rafts[leader-2].Append([]byte("are")) 
	}
	checkCommit(rafts,21)
	chk = Match(rafts,21)
	if chk != true {
		t.Fatal("ERROR !! Expected successful append after append redirection")
	} else {
		fmt.Println("SUCCESSFUL - FOLLOWER APPEND REDIRECTION\n")
	}
	
	
	rafts[1].eventCh <- Timeout{}
	
	time.Sleep(time.Duration(100) * time.Millisecond)
	leader = LeaderId(rafts)
	rafts[leader-1].Append([]byte("you"))
	checkCommit(rafts,22)
	chk = Match(rafts,22)
	if chk != true {
		t.Fatal("ERROR !! Expected successful append after node timeout")
	} else {
		fmt.Println("SUCCESSFUL - LEADER AND FOLLOWER TIMEOUT\n")
	}
	
	
	clust.Partition([]int{1,2,3},[]int{4,5})

	time.Sleep(time.Duration(100)*time.Millisecond)
	leader = LeaderId(rafts[:3])
	rafts[leader-1].Append([]byte("mister"))
	time.Sleep(time.Duration(100) * time.Millisecond)
	checkCommit(rafts,23)
	fmt.Println("SUCCESSFUL - PARTITION LEADER ELECTION AND APPENDS\n")
	
	
	clust.Heal()
	time.Sleep(time.Duration(100)*time.Millisecond)
	leader = LeaderId(rafts)
	
	rafts[leader-1].Append([]byte("YUHOO"))
	time.Sleep(time.Duration(2000) * time.Millisecond)
	checkCommit(rafts,24,5)
	chk = Match(rafts,24)
	if chk != true {
		t.Fatal("ERROR !! Expected successful log updates after partition healing")
	} else {
		fmt.Println("SUCCESSFUL - PARTITION HEALING AND LOG UPDATE\n")
	}
	

	leader = LeaderId(rafts)

	rafts[leader-1].ShutDown()
	time.Sleep(time.Duration(100)*time.Millisecond)
	leader = LeaderId(rafts)
	
	time.Sleep(time.Duration(100)*time.Millisecond)
	rafts[leader-1].Append([]byte("LALALA"))
	checkCommit(rafts,25)
	fmt.Println("SUCCESSFUL - LEADER SHUTDOWN\n")
	
}

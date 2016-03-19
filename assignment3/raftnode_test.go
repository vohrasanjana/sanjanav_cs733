package main
import 	(//"github.com/cs733-iitb/cluster"
	 //"github.com/cs733-iitb/cluster/mock"
	 "time"
	 "testing"
	 "fmt"
	)


func TestRN(t *testing.T) {
	rafts, err := makeRafts()
	if err != nil {
		fmt.Println("makerafts failed")
	}
	time.Sleep(time.Duration(10) * time.Second)
	
	for i :=1; i<=3 ;i++ {
		fmt.Println(rafts[i-1].sm.state)
	}
	time.Sleep(time.Duration(5) * time.Second)
	
	//rafts[0].ClientInput <- ClientAppend{}
	leader := LeaderId(rafts)
	rafts[leader-1].Append([]byte("hi"))
	//for i:=1;i<=3;i++ {

		go func(){
			for{
				select {
					case com := <-rafts[1].CommitChannel():
						fmt.Println(rafts[1].id," committed at index ",com.Index, " with data ",string(com.Data))
					case com := <-rafts[2].CommitChannel():
						fmt.Println(rafts[2].id," committed at index ",com.Index, " with data ",string(com.Data))
					case com := <-rafts[0].CommitChannel():
						fmt.Println(rafts[0].id," committed at index ",com.Index, " with data ",string(com.Data))
				}
			}
		}()
	//}
	time.Sleep(time.Duration(15) * time.Second)
	leader = LeaderId(rafts)
	fmt.Println("========================================================")
	for i:= 1;i<=3;i++ {
		rafts[i-1].sm.PrintLog()
	}
	rafts[leader-1].Append([]byte("how"))
	time.Sleep(time.Duration(2) * time.Millisecond)
	fmt.Println("========================================================")
	for i:= 1;i<=3;i++ {
		rafts[i-1].sm.PrintLog()
	}
	time.Sleep(time.Duration(5) * time.Second)
	
	rafts[1].eventCh <- Timeout{}
	
	time.Sleep(time.Duration(20) * time.Second)
	leader = LeaderId(rafts)
	rafts[leader-1].Append([]byte("are"))
	time.Sleep(time.Duration(5) * time.Second)
	fmt.Println("========================================================")
	for i:= 1;i<=3;i++ {
		rafts[i-1].sm.PrintLog()
	}
	
}

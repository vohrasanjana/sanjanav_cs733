package main

import (
	"net"
	"strings" 
	"strconv"
	"io/ioutil"
	"os"
	"time"
	"math"
	"sync"
)

func check(e error) {
  if e != nil {
    panic(e)
  }
}

type fileinfo struct {
  version int
  exptime int
  numbts int
  inittime time.Time
} 

var fm map[string]fileinfo
var mutex = &sync.Mutex{}

func parseCommand(conn net.Conn, message string) {
    //fmt.Println("In pC")
    var newmessage string

    message = strings.TrimSuffix(message,"\r\n")
    message = strings.TrimPrefix(message,"\r\n")
    //fmt.Println(message)
    command := strings.SplitN(message,"\r\n",2)[0]
    
    words := strings.Split(command," ")
    first := words[0]
    //fmt.Println(first)
    switch first {
      case "write" : 
      { //fmt.Print("In write")
        filename := string(words[1])
        //fmt.Println(filename)
        numbytes, err:= strconv.Atoi(words[2])
        check(err)
        //fmt.Println(numbytes)
        var expt int 
        if len(words) == 4 {
    
        expt_1 := strings.Split(words[3],"\r\n")[0]
        expt, err = strconv.Atoi(expt_1)
        check(err)
        //fmt.Println(expt)
        } else {
        expt = int(math.Inf(1)) 
        }
         
        contenttest := strings.SplitN(message,"\r\n",2)
        content := contenttest[1]
        //fmt.Println("checking",content)
        //fmt.Println("content ----",content[:numbytes])
        contentbyte := []byte(content[:numbytes])
        
        //newcontent := content[numbytes:]
        //fmt.Println(newcontent)
        //fmt.Println(content[numbytes+2:])
        //fmt.Println("YESSS")
        mutex.Lock()
        temp, ok := fm[filename]
        mutex.Unlock()
        if ok != true {
          err = ioutil.WriteFile(filename,contentbyte,0644)
          check(err)
          temp := fileinfo{1,expt,numbytes,time.Now()}
	  mutex.Lock()
          fm[filename] = temp
          mutex.Unlock()
          //fmt.Println("new file is here")
 
          newmessage += "OK "
          newmessage += strconv.Itoa(temp.version)
          newmessage += "\r\n"
            


        } else {
          elapsed := int((time.Since(temp.inittime)).Seconds())
          //fmt.Println(elapsed)
          //fmt.Println(temp.exptime)
          
          if (elapsed <= temp.exptime) {
            err = ioutil.WriteFile(filename,contentbyte,0644)
            check(err)
            temp1 := fileinfo{temp.version + 1,expt,numbytes,time.Now()}
            mutex.Lock()
            delete(fm,filename)
            fm[filename] = temp1
            mutex.Unlock()
            //fmt.Println("version changed", temp1.version)
            newmessage += "OK "
            newmessage += strconv.Itoa(temp1.version)
            newmessage += "\r\n"
            
            

          } else {
            newmessage += "ERR_FILE_EXPIRED\r\n"
          }
        }
      
               
        //fmt.Println("Done write",newmessage)
    // send new string back to client
        conn.Write([]byte(newmessage))
        //fmt.Println("are u here?")
        parseCommand(conn,content[numbytes:])
      } 
      case "read" : {
      
        //var newmessage string
      //f2 := words[0] == "read"
      //if f2 == true {
        filename := strings.Split(words[1],"\r\n")[0]
        //fmt.Println(filename)
        mutex.Lock()
        temp, ok := fm[filename]
        mutex.Unlock()
	if ok == true {
          elapsed := int((time.Since(temp.inittime)).Seconds())
          if (elapsed <= temp.exptime) {

            content, err := ioutil.ReadFile(filename)
            check(err)
            newmessage = "CONTENTS " + strconv.Itoa(temp.version) + " " + strconv.Itoa(temp.numbts) + " " + strconv.Itoa(temp.exptime) + "\r\n" + string(content[:]) + "\r\n"
            //fmt.Println(newmessage)
          } else {
            newmessage = "ERR_FILE_EXPIRED\r\n"
          }
        } else {
          newmessage = "ERR_FILE_NOT_FOUND\r\n"
        }

        conn.Write([]byte(newmessage))
        newcontent := strings.SplitN(message,"\r\n",2)[1]
        parseCommand(conn,newcontent)
      }
      case "delete" : {
        
        var newmessage string
        //f3 := words[0] == "delete"
        //if f3 == true {
        filename := strings.Split(words[1],"\r\n")[0]
        //fmt.Println(filename)
	mutex.Lock()
        _, ok := fm[filename]
	mutex.Unlock()
        if ok == true {
          err := os.Remove(filename)
          check(err)
	  mutex.Lock()
          delete(fm,filename)
	  mutex.Unlock()
          //fmt.Println("deleted",filename)
          newmessage = "OK\r\n"
        
        } else { 
          //fmt.Println("delete -- not found")
          newmessage = "ERR_FILE_NOT_FOUND\r\n"
        }
        conn.Write([]byte(newmessage + "\n"))
        newcontent := strings.SplitN(message,"\r\n",2)[1]
        parseCommand(conn,newcontent)
      
      }
      case "cas" : {
        
        //var newmessage string
        filename := string(words[1])
        //fmt.Println(filename)
        vers, err := strconv.Atoi(words[2])
        check(err) 
        numbytes, err:= strconv.Atoi(words[3])
        check(err)
        //fmt.Println(numbytes) 
        var expt int
        if len(words) == 4 {
    
          expt, err = strconv.Atoi(strings.Split(words[4],"\r\n")[0])
          check(err)
        //fmt.Println(expt)
        } else {
        expt = int(math.Inf(1)) 
        }
        
        contenttest := strings.SplitN(message,"\r\n",2)
        content := contenttest[1]
        //fmt.Println(content)
        contentbyte := []byte(content[:numbytes])
	mutex.Lock()
        temp, ok := fm[filename]
	mutex.Unlock()
        if ok == true {
          elapsed := int((time.Since(temp.inittime)).Seconds())
          //fmt.Println(elapsed)
          //fmt.Println(temp.exptime)
          
          if (elapsed <= temp.exptime) {
            if (vers == temp.version) {
              err = ioutil.WriteFile(filename,contentbyte,0644)
              check(err)
              temp1 := fileinfo{vers+1,expt,numbytes,time.Now()}
	      mutex.Lock()
              delete(fm,filename)
              fm[filename] = temp1
              mutex.Unlock()
		//fmt.Println("cas done with version - ", temp1.version)
            
              newmessage += "OK "
              newmessage += strconv.Itoa(temp1.version)
              newmessage += "\r\n"
            } else {
              newmessage += "ERR_VERSION "+strconv.Itoa(temp.version)+"\r\n"
            }
          } else {
            newmessage += "ERR_FILE_EXPIRED\r\n"
          }
        }
        conn.Write([]byte(newmessage))
        parseCommand(conn,strings.TrimSuffix(content[numbytes:],"\r\n"))
      }
    }
}
func handleClient(conn net.Conn) {


  for {
    //message, _ := bufio.NewReader(conn).ReadString('\n')
    msg := make([]byte,1024) 
    conn.Read(msg)
    message := string(msg)
    if message != "" {
    //fmt.Print("Message Received:", string(message))
    parseCommand(conn,message)
    } else {
    break 
    }
  }
}  

func serverMain() {

  //fmt.Println("Launching server...")

  
  ln, _ := net.Listen("tcp", ":8080")
  
  fm = make(map[string]fileinfo)
  
  for {
  conn, _ := ln.Accept()
  go handleClient(conn)
  }
} 
func main() {
  serverMain()
}


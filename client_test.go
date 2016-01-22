package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"
	"testing"
)


// Simple serial check of getting and setting
func TestTCPSimple(t *testing.T) {
	go serverMain()
        time.Sleep(1 * time.Second) // one second is enough time for the server to start
	name := "f1"
	name1 := "f2"
        contents := "asdfghjkl;"
        contents1 := "qwrnuiop"
	contents2 := "asdcvfgbhn"
	contents3 := "abcdefgh"
	exptime := 1000
	
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)

	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	//overwrite f1
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents1), exptime, contents1)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	expect(t,arr[1], "2")
	
	//read
	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents1)))	
	scanner.Scan()
	expect(t,contents1, scanner.Text())
	//scanner.Scan()
	//expect(t,"uiop\r", scanner.Text())
	
	ver := "2"
	
	//cas
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n",name,ver,len(contents2),exptime,contents2)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	//version1, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	
        expect(t,arr[1],"3")
	if err != nil {
		t.Error("Non-numeric version found")
	}


	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name1, len(contents3), exptime, contents3)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	resp = scanner.Text()
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}


        // Overwrite on file AND multiple commands at once
        fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\nwrite %v %v\r\n%v\r\n", name, len(contents1), exptime, contents1,name1,len(contents1),contents1)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "ERR_FILE_EXPIRED")
	//version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
		

	//delete f2

	fmt.Fprintf(conn, "delete %v\r\n", name1) // try a read now
	scanner.Scan()

	arr = strings.Split(scanner.Text(), " ")
	expect(t,arr[0],"OK")

	
}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}

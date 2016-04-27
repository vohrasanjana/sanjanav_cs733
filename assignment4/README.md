# A Distributed File System Using RAFT
Project for the course CS-733 : Engineering a Cloud, Spring 2015

RAFT is a consensus algorithm for managing replicated instructions over a cluster of servers in a consistent state.This project is a Go implementation of a Distributed File System which uses the Raft consensus protocol for replication and forms a consistent state of log with majority servers agreeing on the actions.

The project basically involves two modules File Server (fs), and Raft Node (raft); and a Client Handler (server.go). The fs and raft run in parallel as Go-routines and communicate among themselves via the client handler.

## File Server - fs
fs is a simple network file server. Each file has a version number, and the server keeps the latest version. The server handles four commands - read, write, compare-and-swap and delete. The files may also have an optional expiry time. A subsequent `cas` or `write` cancels an earlier expiry time, and imposes the new time.
This package has been taken from the implementation at github.com/cs733-iitb/cs733/assignment1/ The required changes have been done for integrating with the system

## Command Specification

The format for each of the four commands is shown below,  

| Command  | Success Response | Error Response
|----------|-----|----------|
|read _filename_ \r\n| CONTENTS _version_ _numbytes_ _exptime remaining_\r\n</br>_content bytes_\r\n </br>| ERR_FILE_NOT_FOUND
|write _filename_ _numbytes_ [_exptime_]\r\n</br>_content bytes_\r\n| OK _version_\r\n| |
|cas _filename_ _version_ _numbytes_ [_exptime_]\r\n</br>_content bytes_\r\n| OK _version_\r\n | ERR\_VERSION _newversion_
|delete _filename_ \r\n| OK\r\n | ERR_FILE_NOT_FOUND

In addition the to the semantic error responses in the table above, all commands can get two additional errors. `ERR_CMD_ERR` is returned on a malformed command, `ERR_INTERNAL` on internal errors.

For `write` and `cas` and in the response to the `read` command, the content bytes is on a separate line. The length is given by _numbytes_ in the first line.

## Raft Package

This module consists of a cluster of five Raft Nodes (raftnode.go), each of which embeds a state machine (statemachine.go), which in turn encodes the consensus algorithm. The details of the algorithm can be found [here](https://ramcloud.stanford.edu/raft.pdf).

In this project, I have implemented an object oriented approach for the raft algorithm. Messages are exchanged between the raft nodes in the form of structures via channels. The algorithm uses a leveldb Log for replication. 
The raft nodes start in the follower mode, and elect a leader by voting. All instruction except read from the file server are sent to the raft leader by the client handler for replication. The raft nodes achieve a consistent replication and send a commit reply to the handler, after which the instruction is processed by the file server.
The latest state of the nodes is saved in a state file. This and the log file can be used for recovery of a node.

## Install

```
`go get github.com/vohrasanjana/sanjanav_cs733/assignment4/...`

```

## References


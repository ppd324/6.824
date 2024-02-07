package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type WorkerRigisterArgs struct {
	Id string
}

type WorkerRegisterReply struct {
	Status string
}

type GetTaskArgs struct {
	Id     string
	Status WorkerStatus
}

type GetTaskReply struct {
	TaskType    string
	MapFilename string
	ReduceFiles []string
	ReduceName  string
	ReduceIndex int
	NReduce     int
}

type ReportTaskStatusAgs struct {
	TaskType    string
	Filename    string
	IsFinished  bool
	TempFile    [][]string
	Status      WorkerStatus
	ReduceIndex int
}

type ReportTaskStatusResply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.

func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

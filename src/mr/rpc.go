package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"

	"github.com/google/uuid"
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

type RequestTaskArgs struct {
	WorkerId uuid.UUID
}

type RequestTaskReply struct {
	TaskType string
	TaskId   int
	Filename string
	NReduce  int
	MapIds   []int
}

type ReportMapTaskArgs struct {
	WorkerId  uuid.UUID
	TaskId    int
	ReduceIds []int
}

type ReportMapTaskReply struct {
}

type ReportReduceTaskArgs struct {
	WorkerId uuid.UUID
	TaskId   int
}

type ReportReduceTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

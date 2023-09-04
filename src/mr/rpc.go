package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

<<<<<<< HEAD
import "os"
import "strconv"
=======
import (
	"os"
	"strconv"
)
>>>>>>> lab1-map-reduce

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

<<<<<<< HEAD
=======
type AskForNewJobArgs struct {
	WorkerNum int
}

type AskForNewJobReply struct {
	FileName        string // this file hasn't been started yet for mapper
	MapperId        int
	ReducerId       int
	NumReduce       int
	AllJobsFinished bool
}

type MapperFinishedArgs struct {
	MapperId int
}

type MapperFinishedReply struct {
	Acknowledged bool
}

type ReduceFinishedArgs struct {
	ReducerId int
}

type ReduceFinishedReply struct {
	Acknowledged bool
}
>>>>>>> lab1-map-reduce

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

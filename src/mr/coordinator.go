package mr

<<<<<<< HEAD
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.

}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
=======
import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	mu              sync.Mutex
	Files           []string // if a mapper has started on this file already
	MapperStarted   map[int]bool
	MapperFinished  map[int]bool
	MapperIdx       int // both init to be 0
	ReducerStarted  map[int]bool
	ReducerFinished map[int]bool
	ReducerIdx      int // increment to 1, until to NumReduce
	NumReduce       int
}

const ToleranceTimeForCrashTest = 10

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
>>>>>>> lab1-map-reduce
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

<<<<<<< HEAD

//
// start a thread that listens for RPCs from worker.go
//
=======
func (c *Coordinator) AssignNewJobForWorker(args *AskForNewJobArgs, reply *AskForNewJobReply) error {
	c.mu.Lock()
	if !c.CheckIfAllMapperStarted() {
		c.AssignNewFileForMapper(reply)
		fmt.Printf("[Coordinator] assigned file %v for mapper id = %v\n", reply.FileName, reply.MapperId)
		go c.WaitAndCheckIfMapWorkerCrashed(reply.MapperId)
		c.mu.Unlock()
	} else if !c.CheckIfAllMapperFinished() { // some mapper jobs haven't finished yet, can't start reducer job
		fmt.Printf("[Coordinator] not all mapper jobs have finished.\n")
		c.mu.Unlock()
	} else if !c.CheckIfAllReducerStarted() {
		c.AssignNewFileForReducer(reply)
		fmt.Printf("[Coordinator] assigned job for reducer id = %v\n", reply.ReducerId)
		go c.WaitAndCheckIfReduceWorkerCrashed(reply.ReducerId)
		c.mu.Unlock()
	} else if !c.CheckIfAllReducerFinished() {
		fmt.Printf("[Coordinator] not all reducer jobs have finished\n")
		c.mu.Unlock()
	} else {
		reply.AllJobsFinished = true
		c.mu.Unlock()
	}
	return nil
}

func (c *Coordinator) AssignNewFileForMapper(reply *AskForNewJobReply) {
	for fileId, fileName := range c.Files {
		mapperId := fileId + 1
		if !c.MapperStarted[mapperId] {
			reply.FileName = fileName
			reply.MapperId = mapperId
			reply.NumReduce = c.NumReduce
			c.MapperStarted[mapperId] = true
			break
		}
	}
}

func (c *Coordinator) AssignNewFileForReducer(reply *AskForNewJobReply) {
	for reducerId, alreadyStarted := range c.ReducerStarted {
		if !alreadyStarted {
			reply.ReducerId = reducerId
			reply.NumReduce = c.NumReduce
			c.ReducerStarted[reducerId] = true
			c.ReducerIdx += 1
			break
		}
	}
}

func (c *Coordinator) UpdateMapperIdToFinished(args *MapperFinishedArgs, reply *MapperFinishedReply) error {
	c.mu.Lock()
	c.MapperFinished[args.MapperId] = true
	reply.Acknowledged = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) UpdateReducerIdToFinished(args *ReduceFinishedArgs, reply *ReduceFinishedReply) error {
	c.mu.Lock()
	c.ReducerFinished[args.ReducerId] = true
	reply.Acknowledged = true
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) CheckIfAllMapperStarted() bool {
	count := 0
	for _, startedStatus := range c.MapperStarted {
		if startedStatus {
			count += 1
		}
	}
	fmt.Printf("[Coordinator] started mapper job count = %v; files count = %v \n", count, len(c.Files))
	if count == len(c.Files) {
		return true
	} else {
		return false
	}
}

func (c *Coordinator) WaitAndCheckIfMapWorkerCrashed(mapperId int) bool {
	time.Sleep(ToleranceTimeForCrashTest * time.Second)
	c.mu.Lock()
	if c.MapperFinished[mapperId] == false {
		fmt.Printf("[Coordinator] mapperId %v crashed !\n", mapperId)
		c.MapperStarted[mapperId] = false
		c.mu.Unlock()
		return true
	} else {
		c.mu.Unlock()
		return false
	}
}

func (c *Coordinator) WaitAndCheckIfReduceWorkerCrashed(reduceId int) bool {
	time.Sleep(ToleranceTimeForCrashTest * time.Second)
	c.mu.Lock()
	if c.ReducerFinished[reduceId] == false {
		fmt.Printf("[Coordinator] reducerId %v crashed !\n", reduceId)
		c.ReducerStarted[reduceId] = false
		c.mu.Unlock()
		return true
	} else {
		c.mu.Unlock()
		return false
	}
}

func (c *Coordinator) CheckIfAllMapperFinished() bool {
	count := 0
	for _, finishedStatus := range c.MapperFinished {
		if finishedStatus {
			count += 1
		}
	}
	fmt.Printf("[Coordinator] finished mapper job count = %v\n", count)
	if count == len(c.Files) {
		return true
	} else {
		return false
	}
}

func (c *Coordinator) CheckIfAllReducerStarted() bool {
	count := 0
	for _, startedStatus := range c.ReducerStarted {
		if startedStatus {
			count += 1
		}
	}
	fmt.Printf("[Coordinator] started reducer job count = %v; files count = %v \n", count, c.NumReduce)
	if count == c.NumReduce {
		return true
	} else {
		return false
	}
}

func (c *Coordinator) CheckIfAllReducerFinished() bool {
	count := 0
	for _, finishedStatus := range c.ReducerFinished {
		if finishedStatus {
			count += 1
		}
	}
	fmt.Printf("[Coordinator] finished reducer job count = %v\n", count)
	if count == c.NumReduce {
		return true
	} else {
		return false
	}
}

// start a thread that listens for RPCs from worker.go
>>>>>>> lab1-map-reduce
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
<<<<<<< HEAD
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

=======
	listener, err := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(listener, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false
	c.mu.Lock()
	if c.CheckIfAllReducerFinished() {
		ret = true
	}
	c.mu.Unlock()
	return ret
}

// =
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:           make([]string, 0),
		MapperStarted:   make(map[int]bool),
		MapperFinished:  make(map[int]bool),
		MapperIdx:       1,
		ReducerStarted:  make(map[int]bool),
		ReducerFinished: make(map[int]bool),
		ReducerIdx:      1,
		NumReduce:       nReduce}

	// Your code here.
	// initialize all the state variables
	for i, file := range files {
		c.Files = append(c.Files, file)
		c.MapperStarted[i+1] = false
		c.MapperFinished[i+1] = false
	}

	for reducerId := 1; reducerId <= c.NumReduce; reducerId++ {
		c.ReducerStarted[reducerId] = false
		c.MapperFinished[reducerId] = false
	}
>>>>>>> lab1-map-reduce

	c.server()
	return &c
}

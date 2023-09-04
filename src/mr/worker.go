package mr

<<<<<<< HEAD
import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


//
// Map functions return a slice of KeyValue.
//
=======
import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
>>>>>>> lab1-map-reduce
type KeyValue struct {
	Key   string
	Value string
}

<<<<<<< HEAD
//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
=======
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
>>>>>>> lab1-map-reduce
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

<<<<<<< HEAD

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
=======
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	for { // be cautious, this is a forever loop
		time.Sleep(1 * time.Second)
		reply := AskForNewJob() // either mapper or reducer job
		if reply.MapperId != 0 && reply.ReducerId == 0 {
			doMapperFunc(reply, mapf)
			TellCoordinatorMapperHasFinished(reply.MapperId)
		} else if reply.ReducerId != 0 && reply.MapperId == 0 {
			doReducerFunc(reply, reducef)
			TellCoordinatorReducerHasFinished(reply.ReducerId)
		} else if reply.ReducerId == 0 && reply.MapperId == 0 {
		} else if reply.ReducerId != 0 && reply.MapperId != 0 {
			fmt.Printf("Wrong reply from coordinator: mapperId = %v, reducerId = %v (can't be both >= 0)\n", reply.MapperId, reply.ReducerId)
			return
		} else if reply.AllJobsFinished {
			fmt.Printf("[Worker] All tasks finished\n")
			return
		}
	}
}

func doMapperFunc(reply *AskForNewJobReply, mapf func(string, string) []KeyValue) {
	fmt.Printf("[Worker] mapper file = %v, mapper index = %v \n", reply.FileName, reply.MapperId)
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content)) // array of KeyValue
	fmt.Printf("[Worker] mapper done, start writing to intermediate\n")

	for reduceId := 0; reduceId < reply.NumReduce; reduceId++ {
		tempFileName, err := ioutil.TempFile("./", "temp")
		if err != nil {
			fmt.Printf("error in creating temp file for mapper job on file %v, mapperId %v\n", reply.FileName, reply.MapperId)
			return
		}
		intermediateName := fmt.Sprintf("mr-%d-%d", reply.MapperId, reduceId+1) // reduceId + 1 to make it 1-indexed
		// fmt.Printf("tempfile name: %v\n", tempFileName.Name())
		tempFile, _ := os.Create(tempFileName.Name())
		enc := json.NewEncoder(tempFile)
		for _, kv := range kva {
			if ihash(kv.Key)%reply.NumReduce == reduceId {
				err := enc.Encode(&kv)
				if err != nil {
					break
				}
			}
		}
		tempFile.Close()
		os.Rename(tempFileName.Name(), intermediateName)
	}
}

func doReducerFunc(reply *AskForNewJobReply, reducef func(string, []string) string) {
	fmt.Printf("reducerId = %v \n", reply.ReducerId)
	pattern := fmt.Sprintf("mr-*-%d", reply.ReducerId)
	files, err := filepath.Glob(pattern)
	if err != nil {
		fmt.Println("Error: can't find files of pattern %v \n", pattern)
		return
	}
	kva := []KeyValue{}
	for _, fileName := range files {
		file, err := os.Open(fileName)
		if err != nil {
			fmt.Println("Error: can't find file %v \n", file)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	sort.Sort(ByKey(kva))
	// fmt.Printf("first key, %v, first value, %v\n", kva[0].Key, kva[0].Value)
	oname := fmt.Sprintf("mr-out-%d", reply.ReducerId)

	tempFileName, err := ioutil.TempFile("./", "temp")
	if err != nil {
		fmt.Printf("[Worker] error in creating temp file for mapper job on reducerId %v\n", reply.ReducerId)
		return
	}

	ofile, _ := os.Create(tempFileName.Name())
	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0 // key = the
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{} // the-1, the-1, the-1
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value) // the-1 * 3
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	ofile.Close()
	os.Rename(tempFileName.Name(), oname)
	fmt.Printf("[Worker] finished reducer job for reducerId %v; output in %v \n", reply.ReducerId, oname)
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
>>>>>>> lab1-map-reduce
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

<<<<<<< HEAD
	// declare a reply structure.
=======
	// declare reply structure.
>>>>>>> lab1-map-reduce
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

<<<<<<< HEAD
//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
=======
// ask for a new file to start a mapper job or for a new reducer job
func AskForNewJob() *AskForNewJobReply {
	args := AskForNewJobArgs{WorkerNum: 1}
	reply := AskForNewJobReply{}
	ok := call("Coordinator.AssignNewJobForWorker", &args, &reply)
	if ok {
		fmt.Printf("[Worker] reply.MapperId: %v, reply.ReducerId: %v\n", reply.MapperId, reply.ReducerId)
	} else {
		fmt.Printf("[Worker] call failed!\n")
	}
	return &reply
}

func TellCoordinatorMapperHasFinished(mapperId int) {
	args := MapperFinishedArgs{MapperId: mapperId}
	reply := MapperFinishedReply{}
	ok := call("Coordinator.UpdateMapperIdToFinished", &args, &reply)
	if ok {
		fmt.Printf("Coordinator acknowledged the finished of MapperId %v\n", mapperId)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func TellCoordinatorReducerHasFinished(reducerId int) {
	args := ReduceFinishedArgs{ReducerId: reducerId}
	reply := ReduceFinishedReply{}
	ok := call("Coordinator.UpdateReducerIdToFinished", &args, &reply)
	if ok {
		fmt.Printf("Coordinator acknowledged the finished of ReducerId %v\n", reducerId)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	client, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer client.Close()

	err = client.Call(rpcname, args, reply)
>>>>>>> lab1-map-reduce
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

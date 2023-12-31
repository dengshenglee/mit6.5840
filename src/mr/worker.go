package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

/*
* finalizeReduceFile atomically renames temporary reduce file to a completed reduce task file
 */
func finalizeReduceFile(tmpFile string, taskN int) {
	finalFile := fmt.Sprintf("mr-out-%d", taskN)
	os.Rename(tmpFile, finalFile)
}

/*
* get name of the intermediate file for a map and reduce task number
 */
func getIntermediateFile(mapTaskN int, redTaskN int) string {
	return fmt.Sprintf("mr-%d-%d", mapTaskN, redTaskN)
}

/*
* finalizeIntermediateFile atomically renames temporary intermediate file to a completed intermediate file
 */
func finalizeIntermediateFile(tmpFile string, mapTaskN int, redTaskN int) {
	finalFile := getIntermediateFile(mapTaskN, redTaskN)
	os.Rename(tmpFile, finalFile)
}

/*
* implementation of map task
 */
func performMap(filename string, taskNum int, nReduceTasks int, mapf func(string, string) []KeyValue) {
	//read contents to map
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v in performMap", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}

	file.Close()
	// map the contents
	kva := mapf(filename, string(content))

	// create temporary files and encoders for each file
	tmpFiles := []*os.File{}
	tmpFilenames := []string{}
	encoders := []*json.Encoder{}
	for r := 0; r < nReduceTasks; r++ {
		tmpFile, err := ioutil.TempFile("", "")
		if err != nil {
			log.Fatalf("cannot open tmpfile\n")
		}
		tmpFiles = append(tmpFiles, tmpFile)
		tmpFilename := tmpFile.Name()
		tmpFilenames = append(tmpFilenames, tmpFilename)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}

	// write output keys to appropriate (temporary) intermediate files
	// using the provided hash function
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduceTasks
		encoders[r].Encode(&kv)
	}
	for _, f := range tmpFiles {
		f.Close()
	}

	// atomically rename files to final intermediate files
	for r := 0; r < nReduceTasks; r++ {
		finalizeIntermediateFile(tmpFilenames[r], taskNum, r)
	}
}

/*
* implementation of Reduce task
 */
func performReduce(taskNum int, NMapTasks int, reducef func(string, []string) string) {
	//get all intermediate file corresponding to this reduce task, and collect the corresponding key-value pairs
	kva := []KeyValue{}
	for m := 0; m < NMapTasks; m++ {
		iFilename := getIntermediateFile(m, taskNum)
		file, err := os.Open(iFilename)
		if err != nil {
			log.Fatalf("cannot open %v in performReduce", iFilename)
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

	// sort the keys
	sort.Sort(ByKey(kva))

	// get temporary reduce file to write Values
	tmpFile, err := ioutil.TempFile("", "")
	if err != nil {
		log.Fatalf("cannot open TempFile")
	}
	tmpFilename := tmpFile.Name()

	// apply reduce function once to all values of the same key
	key_begin := 0
	for key_begin < len(kva) {
		key_end := key_begin + 1
		// find all the values with the same keys -- they are grouped
		// together for the keys are sorted
		for key_end < len(kva) && kva[key_end].Key == kva[key_begin].Key {
			key_end++
		}
		values := []string{}
		for k := key_begin; k < key_end; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[key_begin].Key, values)

		//Write output to reduce task tmp file
		fmt.Fprintf(tmpFile, "%v %v\n", kva[key_begin].Key, output)

		// go to the next key
		key_begin = key_end
	}

	finalizeReduceFile(tmpFilename, taskNum)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		// call the coordinator to get a task
		call("Coordinator.HandleGetTask", &args, &reply)

		log.Printf("Got task %d of type %d, with map-file %s", reply.TaskNum, reply.TaskType, reply.MapFile)

		switch reply.TaskType {
		case Map:
			performMap(reply.MapFile, reply.TaskNum, reply.NReduceTasks, mapf)
		case Reduce:
			performReduce(reply.TaskNum, reply.NMapTasks, reducef)
		case Done:
			os.Exit(0)
		default:
			fmt.Errorf("Bad task type? %d", reply.TaskType)
		}

		//tell the coordinator that we are done
		finargs := FinishedTaskArgs{
			TaskType: reply.TaskType,
			TaskNum:  reply.TaskNum,
		}
		finreply := FinishedTaskReply{}
		call("Coordinator.HandleFinishedTask", &finargs, &finreply)
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
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

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

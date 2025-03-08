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
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		args := WorkerArgs{}
		reply := WorkerReply{}
		ok := call("Coordinator.GetTask", &args, &reply)
		if !ok || reply.TaskType == TaskExit {
			break
		} else if reply.TaskType == TaskMap {
			intermediate := []KeyValue{}
			filename := reply.MapFilename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)

			buckets := make([][]KeyValue, reply.NReduce)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range intermediate {
				bucketId := ihash(kv.Key) % reply.NReduce
				buckets[bucketId] = append(buckets[bucketId], kv)
			}
			for i, bucket := range buckets {
				ofilename := fmt.Sprintf("mr-%v-%v", reply.MapTaskId, i)
				ofile, _ := ioutil.TempFile("", ofilename+"*")
				enc := json.NewEncoder(ofile)
				for _, kv := range bucket {
					e := enc.Encode(&kv)
					if e != nil {
						log.Fatalf("cannot encode %v", kv)
					}
				}
				os.Rename(ofile.Name(), ofilename)
				ofile.Close()
			}

			finishMapArgs := WorkerArgs{MapTaskId: reply.MapTaskId, ReduceTaskId: 0}
			finishMapReply := WorkerReply{}
			call("Coordinator.FinishMapTask", &finishMapArgs, &finishMapReply)
		} else if reply.TaskType == TaskReduce {
			intermediate := []KeyValue{}
			for i := 0; i < reply.NMap; i++ {
				ifilename := fmt.Sprintf("mr-%v-%v", i, reply.ReduceTaskId)
				file, err := os.Open(ifilename)
				if err != nil {
					log.Fatalf("cannot open %v", ifilename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			ofilename := fmt.Sprintf("mr-out-%v", reply.ReduceTaskId)
			ofile, _ := ioutil.TempFile("", ofilename+"*")
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			os.Rename(ofile.Name(), ofilename)
			ofile.Close()

			finishReduceArgs := WorkerArgs{MapTaskId: 0, ReduceTaskId: reply.ReduceTaskId}
			finishReduceReply := WorkerReply{}
			call("Coordinator.FinishReduceTask", &finishReduceArgs, &finishReduceReply)
		} else if reply.TaskType == TaskWait {
			time.Sleep(time.Second)
		} else {
			log.Fatal("Unknown task type")
		}
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

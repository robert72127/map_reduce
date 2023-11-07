package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
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


	//ask coordinator for task
	for {

		assign_args := AssignArgs{}

		assign_reply := AssignReply{}

		ok := call("Coordinator.StartJob", &assign_args, &assign_reply)
		if !ok {
			os.Exit(0)
		}

		switch assign_reply.Task {
		case "map":
			file, err := os.Open(assign_reply.FileName)
			if err != nil {
				log.Fatalf("cannot open %v", assign_reply.FileName)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", assign_reply.FileName)
			}
			file.Close()

			kva := mapf(assign_reply.FileName, string(content))

			// create mapping from keys to files
			file_map := make(map[int]*os.File)

			for _, kv := range kva {
				reduce_index := ihash((kv.Key)) % assign_reply.Task_count
				file := file_map[reduce_index]
				if file == nil {
					file_map[reduce_index], err = os.CreateTemp("./", "mr-"+strconv.Itoa(assign_reply.Task_nr)+strconv.Itoa(reduce_index))
					if err != nil {
						log.Fatalf("cannot create file")
					}
				}
				enc := json.NewEncoder(file_map[reduce_index])
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write output: %v to file  %v", kv, file_map[reduce_index])
				}
			}

			// rename file after we are done with writing
			for reduce_index, f_pointer := range file_map {
				err := os.Rename(f_pointer.Name(), "./"+"mr-"+strconv.Itoa(assign_reply.Task_nr)+strconv.Itoa(reduce_index))
				if err != nil {
					log.Fatalf("cannot rename file:  %v", f_pointer.Name())
				}
				f_pointer.Close()
			}

			//acknowledge coordinator about finishing the job
			finish_args := FinishArgs{}
			finish_args.FileName = assign_reply.FileName
			finish_args.Task = assign_reply.Task
			finish_args.Task_nr = assign_reply.Task_nr
			finish_reply := FinishReply{}

			ok := call("Coordinator.FinishJob", &finish_args, &finish_reply)
			if !ok {
				fmt.Printf("call failed!\n")
			}

		case "reduce": //perform reduce operation
			
			// find all files in current directory that match mr_*_assign_reply.FileName
			all_files, err := os.ReadDir("./")
			matching_files := make([]string, 0)
			if err != nil {
				log.Fatalf("Could not read current directory")
			}
			for _, s := range all_files {
				str := s.Type().String()
				if strings.HasPrefix(str, "mr") && strings.HasSuffix(str, assign_reply.FileName) {
					matching_files = append(matching_files, str)
				}
			}


			// append files & sort
			intermediate := []KeyValue{}
			for _, fname := range matching_files {

				file, err := os.Open(fname)
				if err != nil {
					log.Fatalf("cannot open %v", fname)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}

			sort.Sort(ByKey(intermediate))

			//perform reduce & write result to file
			write_file, err := os.CreateTemp("./", "mr-out-"+strconv.Itoa(assign_reply.Task_nr))
			if err != nil {
				log.Fatalf("cannot create file:  %v", write_file)
			}

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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(write_file, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			err = os.Rename(write_file.Name(), "./"+"mr-out-"+strconv.Itoa(assign_reply.Task_nr))
			if err != nil {
				log.Fatalf("cannot rename file:  %v", write_file.Name())
			}
			write_file.Close()

			//acknowledge that you finished the job
			finish_args := FinishArgs{}
			finish_args.FileName = assign_reply.FileName
			finish_args.Task = assign_reply.Task
			finish_args.Task_nr = assign_reply.Task_nr
			finish_reply := FinishReply{}
			ok := call("Coordinator.FinishJob", &finish_args, &finish_reply)
			if !ok {
				fmt.Printf("call failed!\n")
			}
		case "wait":
			time.Sleep(5 * time.Second)
			continue
		case "exit":
			os.Exit(0)
		}
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

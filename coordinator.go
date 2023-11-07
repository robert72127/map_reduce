package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	Map_files      map[string]int // for each file name we keep state 0 - not assigned, 1 - assigned, 2 - finished
	Reduce_files   map[int]int    // for each file name we keep state 0 - not assigned, 1 - assigned, 2 - finished
	mu             sync.Mutex
	Nreduce        int // nr of reduce jobs
	Task_index     int // which task of N worker will process
	MapFinished    bool
	ReduceFinished bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) StartJob(args *AssignArgs, reply *AssignReply) error {
	// assign job nr
	// keep track of time if worker won't finish assume its dead and
	// assign job to other one

	if !c.MapFinished {
		i := 0
		for fname, finished := range c.Map_files {
			if finished == 0 { //assign to this worker

				c.mu.Lock()

				c.Map_files[fname] = 1 //mark as assigned

				c.mu.Unlock()

				reply.FileName = fname
				reply.Task = "map"
				reply.Task_nr = i
				reply.Task_count = c.Nreduce

				go func() { //wait ten second and check if job is finished
					time.Sleep(10 * time.Second)
					c.mu.Lock()
					if c.Map_files[fname] != 2 {
						c.Map_files[fname] = 0
					}
					c.mu.Unlock()
				}()

				return nil
			}

			i++
		}
		// not all jobs  finished but all are assigned, wait
		reply.Task = "wait"
		return nil

	} else if !c.ReduceFinished {
		i := 0
		for findex, finished := range c.Reduce_files {
			if finished == 0 {
				c.mu.Lock()
				c.Reduce_files[findex] = 1
				c.mu.Unlock()

				reply.FileName = strconv.Itoa(findex)
				reply.Task = "reduce"
				reply.Task_nr = i
				//reply.Task_count = c.Nreduce

				go func() { //wait ten second and check if job is finished
					time.Sleep(10 * time.Second)
					c.mu.Lock()
					if c.Reduce_files[findex] != 2 {
						c.Reduce_files[findex] = 0
					}
					c.mu.Unlock()
				}()

				return nil
			}
			i++
		}
		// not all jobs  finished but all are assigned, wait
		reply.Task = "wait"
		return nil
	} else { //return finished indicating that worker can exit
		reply.Task = "exit"
		return nil
	}
}

func (c *Coordinator) checkFinished(task string) {
	if task == "map" {
		for _, finished := range c.Map_files {
			if finished != 2 {
				return
			}
		}
		c.MapFinished = true
	} else if task == "reduce" {
		for _, finished := range c.Reduce_files {
			if finished != 2 {
				return
			}
		}
		c.ReduceFinished = true
	}
}

// track finished jobs
func (c *Coordinator) FinishJob(args *FinishArgs, reply *FinishReply) error {
	// assign given file as processed if map
	// if reduce assign reduce as finished
	// if last reduce job set ReduceFinished
	if args.Task == "map" {
		c.Map_files[args.FileName] = 2

	} else if args.Task == "reduce" {
		c.Reduce_files[args.Task_nr] = 2
	}
	go c.checkFinished(args.Task)

	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	return c.ReduceFinished

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	//initiate struct
	for _, fname := range files {
		c.Map_files[fname] = 0
	}
	for i := 0; i < nReduce; i++ {
		c.Reduce_files[i] = 0
	}
	c.Nreduce = nReduce
	c.Task_index = 0
	c.MapFinished = false
	c.ReduceFinished = false

	c.server()

	return &c
}

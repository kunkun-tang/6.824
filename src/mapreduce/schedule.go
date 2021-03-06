package mapreduce

import (
	"fmt"
)

//
// schedule() starts and waits for all tasks in the given phase (Map
// or Reduce). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	var rpc_addr string = <- registerChan
	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)
	switch phase {
	case mapPhase:
		for taskId := 0; taskId < ntasks; taskId ++ {
			myTaskArgs := DoTaskArgs{
				JobName:jobName,
				Phase:phase,
				TaskNumber:taskId,
				File:mapFiles[taskId],
				NumOtherPhase:n_other,
			}
			//if ok := call(rpc_addr, "Worker.DoTask", myTaskArgs, new(struct{})); ok == true {
			//	continue
			//} else {
			//	rpc_addr = <- registerChan
			//	call(rpc_addr, "Worker.DoTask", myTaskArgs, new(struct{}))
			//}
			for {
				ok := call(rpc_addr, "Worker.DoTask", myTaskArgs, new(struct{}))
				if ok == true {
					break
				} else {
					rpc_addr = <- registerChan
				}
			}
			debug("Map Error")
		}
	case reducePhase:
		for taskId := 0; taskId < ntasks; taskId ++ {
			//rpc_addr := <- registerChan
			myTaskArgs := DoTaskArgs{
				JobName:jobName,
				Phase:phase,
				TaskNumber:taskId,
				NumOtherPhase:n_other,
			}
			for {
				ok := call(rpc_addr, "Worker.DoTask", myTaskArgs, new(struct{}))
				if ok == true {
					break
				} else {
					rpc_addr = <- registerChan
				}
			}
			debug("Reduce Error")
		}
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

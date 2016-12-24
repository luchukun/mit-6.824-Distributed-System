package mapreduce

import (
	"fmt"
	"sync"
)

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	var waitAllComplete sync.WaitGroup
	for i := 0; i < ntasks; i++ {
		waitAllComplete.Add(1)
		fmt.Printf("DEBUG: current taskNum: %v, nios: %v, phase: %v\n", i, nios, phase)
		go func(taskNum int,nios int, phase jobPhase) {
			//create the task args used in the rpc
			defer waitAllComplete.Done()
			var taskArgs DoTaskArgs
			taskArgs.File = func() string{
				if phase == mapPhase {
					return mr.files[taskNum]
				} else {
					return ""
				}
			}()
			taskArgs.JobName = mr.jobName
			taskArgs.TaskNumber = taskNum
			taskArgs.NumOtherPhase = nios
			taskArgs.Phase = phase
			for {
				worker := <- mr.registerChannel
				ok := call(worker,"Worker.DoTask",&taskArgs,new(struct{}))
				if ok {
					//success, break and put the worker into channel
					go func(){
						mr.registerChannel <- worker
					}()
					break
				}
				//failure,just retry
			}
		}(i,nios,phase)
	}
	waitAllComplete.Wait()
	fmt.Printf("Schedule: %v phase done\n", phase)
}

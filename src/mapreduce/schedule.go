package mapreduce

import (
	"fmt"
	"sync"
	"runtime"
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

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//

	workerChan := make(chan string, runtime.NumCPU())

	go func() {
		worker := <-registerChan
		workerChan <- worker
	}()
	var group sync.WaitGroup
	group.Add(ntasks)
	for i := 0; i < ntasks; i++ {
		worker := <-workerChan
		switch phase {
		case mapPhase:
			go func(jobName string, phase jobPhase, file string, taskNumber int) {
				call(worker, "Worker.DoTask", &DoTaskArgs{
					JobName: jobName,
					File: file,
					Phase: phase,
					TaskNumber: taskNumber,
					NumOtherPhase: n_other}, nil)
				workerChan <- worker
				group.Done()
			}(jobName, phase, mapFiles[i], i)
		case reducePhase:
			go func(jobName string, phase jobPhase, taskNumber int) {
				call(worker, "Worker.DoTask", &DoTaskArgs{
					JobName: jobName,
					Phase: phase,
					TaskNumber: taskNumber,
					NumOtherPhase: n_other}, nil)
				workerChan <- worker
				group.Done()
			}(jobName, phase, i)
		}
	}
	group.Wait()
	close(workerChan)
	fmt.Printf("Schedule: %v phase done\n", phase)
}

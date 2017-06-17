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

	workerChan := make(chan string, runtime.NumCPU())
	taskChan := make(chan *DoTaskArgs)
	doneChan := make(chan bool)
	finish := make(chan bool)

	go func() {
		for {
			worker := <-registerChan
			workerChan <- worker
		}
	}()
	var group sync.WaitGroup
	group.Add(ntasks)
	go runTask(workerChan, taskChan, doneChan, finish)
	go func () {
		for range doneChan {
			group.Done()
		}
	}()
	for i := 0; i < ntasks; i++ {
		var taskArgs *DoTaskArgs
		switch phase {
		case mapPhase:
			taskArgs = &DoTaskArgs{
				JobName: jobName,
				File: mapFiles[i],
				Phase: phase,
				TaskNumber: i,
				NumOtherPhase: n_other}
		case reducePhase:
			taskArgs = &DoTaskArgs{
				JobName: jobName,
				Phase: phase,
				TaskNumber: i,
				NumOtherPhase: n_other}
		}
		taskChan <- taskArgs
	}
	group.Wait()
	finish <- true
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func runTask(workerChan chan string, taskChan chan *DoTaskArgs, doneChan chan bool, finishChan chan bool) {
	loop:
	for {
		select {
		case task := <-taskChan:
			go func (task *DoTaskArgs) {
				worker := <-workerChan
				ok := call(worker, "Worker.DoTask", task, nil)
				if ok {
					workerChan <- worker
					doneChan <- true
				} else {
					taskChan <- task
				}
			}(task)
		case <-finishChan:
			break loop
		}
	}
}
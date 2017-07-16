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

	workerChan := make(chan string, runtime.NumCPU())
	taskChan := make(chan *DoTaskArgs)
	finish := make(chan bool)

	go func() {
		for {
			worker := <-registerChan
			workerChan <- worker
		}
	}()

	group := new(sync.WaitGroup)
	group.Add(ntasks)

	go runTask(workerChan, taskChan, group, finish)

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
		go func() {
			taskChan <- taskArgs
		}()
	}
	group.Wait()
	finish <- true
	fmt.Printf("Schedule: %v phase done\n", phase)
}

func runTask(workerChan chan string, taskChan chan *DoTaskArgs, group *sync.WaitGroup, finishChan chan bool) {
	g := group
	loop:
	for {
		select {
		case task := <-taskChan:
			go func (task *DoTaskArgs) {
				worker := <-workerChan
				ok := call(worker, "Worker.DoTask", task, nil)
				if ok {
					g.Done()
					workerChan <- worker
				} else {
					fmt.Printf("Schedule: call Worker %s do task %s #%d failed\n", worker, task.Phase, task.TaskNumber)
					taskChan <- task
				}
			}(task)
		case <-finishChan:
			break loop
		}
	}
}
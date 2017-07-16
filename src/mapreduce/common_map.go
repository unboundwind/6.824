package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
	"fmt"
	"os"
	"log"
	"encoding/json"
)

// doMap manages one map task: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	fmt.Printf("Map: %s-map-%d\n", jobName, mapTaskNumber)
	contents, err := ioutil.ReadFile(inFile)
	if err != nil {
		log.Fatalf("DoMap[%d]: read %s\n", mapTaskNumber, err)
		return
	}
	mapResult := mapF(inFile, string(contents))

	files := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)

	for _, kv := range mapResult {
		r := (ihash(kv.Key) % nReduce)
		if files[r] == nil {
			file, err := os.Create(reduceName(jobName, mapTaskNumber, r))
			if err != nil {
				log.Fatalf("DoMap[%d]: create %s\n", mapTaskNumber, err)
			}
			files[r] = file
			encoders[r] = json.NewEncoder(file)
		}

		err := encoders[r].Encode(&kv)
		if err != nil {
			log.Fatalf("DoMap[%d]: encode %s, %#v\n", mapTaskNumber, err, kv)
		}
	}

	for _, file := range files {
		err := file.Close()
		if err != nil {
			log.Fatalf("DoMap[%d]: close %s\n", mapTaskNumber, err)
		}
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	h.Write([]byte(s))
	return int(h.Sum32() & 0x7fffffff)
}

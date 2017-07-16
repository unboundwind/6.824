package mapreduce

import (
	"os"
	"log"
	"encoding/json"
	"fmt"
)

// doReduce manages one reduce task: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	fmt.Printf("Recude: %s-reduce-%d\n", jobName, reduceTaskNumber)
	kvs := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		inFile, err := os.Open(reduceName(jobName, i, reduceTaskNumber))
		if err != nil {
			log.Fatalf("DoReduce[%d]: read %s\n", reduceTaskNumber, err)
			continue
		}
		dec := json.NewDecoder(inFile)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}

	out, err := os.Create(outFile)
	if err != nil {
		log.Fatalf("DoRecude[%d]: create %s\n", reduceTaskNumber, err)
	}
	defer out.Close()

	enc := json.NewEncoder(out)
	for k, v := range kvs {
		err := enc.Encode(KeyValue{Key: k, Value: reduceF(jobName, v)})
		if err != nil {
			log.Fatalf("DoRecude[%d]: encode %s\n", reduceTaskNumber, err)
		}
	}
}

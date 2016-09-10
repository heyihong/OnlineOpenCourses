package mapreduce

import (
	"encoding/json"
	"os"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	kvMap := make(map[string][]string)
	for mapNum := 0; mapNum < nMap; mapNum++ {
		file, _ := os.Open(reduceName(jobName, mapNum, reduceTaskNumber))
		dec := json.NewDecoder(file)
		var kv KeyValue
		for {
			if err := dec.Decode(&kv); err == nil {
				kvMap[kv.Key] = append(kvMap[kv.Key], kv.Value)
			} else {
				break
			}
		}
		file.Close()
	}
	mergeFile, _ := os.OpenFile(mergeName(jobName, reduceTaskNumber), os.O_CREATE|os.O_WRONLY, 0666)
	enc := json.NewEncoder(mergeFile)
	for key, values := range kvMap {
		enc.Encode(KeyValue{key, reduceF(key, values)})
	}
	mergeFile.Close()
}

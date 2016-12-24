package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sort"
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
	//reade contents from intermediate reduce file
	keyValues := make(map[string][]string)
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTaskNumber)
		reduceFile, err := os.Open(fileName)
		if err != nil {
			errInfo := fmt.Sprintf("cannot open the reduce file[%s],error message:", fileName, err)
			log.Fatal(errInfo)
		}
		jsonReader := json.NewDecoder(reduceFile)
		debug("reade key/val pairs from #mapNum intermediate files")
		for {
			var keyValue KeyValue
			err := jsonReader.Decode(&keyValue)
			if err != nil {
				break
			}
			_, ok := keyValues[keyValue.Key]
			if !ok {
				keyValues[keyValue.Key] = make([]string, 0)
			}
			keyValues[keyValue.Key] = append(keyValues[keyValue.Key], keyValue.Value)
		}
		if err := reduceFile.Close(); err != nil {
			errInfo := fmt.Sprintf("cannot close reduce file [%s],error message:", fileName, err)
			log.Fatal(errInfo)
		}
	}
	debug("sort keys using sort.Strings() method")
	var keys []string
	for key, _ := range keyValues {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	mergeFileName := mergeName(jobName, reduceTaskNumber)
	debug("store the out into merge file[%s]", mergeFileName)
	mergeFileInput, err := os.Create(mergeFileName)
	if err != nil {
		errInfo := fmt.Sprintf("cannot create merge file [%s],error message:%s", mergeFileName, err)
		log.Fatal(errInfo)
	}
	jsonMergeFileInput := json.NewEncoder(mergeFileInput)
	for _, key := range keys {
		resultKey := reduceF(key, keyValues[key])
		jsonMergeFileInput.Encode(&KeyValue{key, resultKey})
	}
	if err := mergeFileInput.Close(); err != nil {
		errInfo := fmt.Sprintf("cannot close merge file [%s],error message:%s", mergeFileName, err)
		log.Fatal(errInfo)
	}
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
}

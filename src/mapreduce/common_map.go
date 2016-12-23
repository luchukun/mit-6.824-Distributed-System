package mapreduce

import (
	"hash/fnv"
	"log"
	"os"
	"bufio"
	"fmt"
	"time"
	"encoding/json"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// TODO:
	//Open inFile
	fi, err:= os.OpenFile(inFile,os.O_RDONLY,0666)
	debug("%s:open file[%s]",time.Now().Format(time.RFC850),inFile)
	if err != nil {
		errInfo :=fmt.Sprint("open file named[%s] failure,error message: %s",inFile,err)
		log.Fatal(errInfo)
	}
	defer func() {
		if err := fi.Close();err != nil {
			errInfo := fmt.Sprint("close file named[%s] failure,error message: %s",inFile,err)
			log.Fatal(errInfo)
		}
	}()
	//Reade contents from inFile
	debug("%s:beginning reade contents from file[%s]",time.Now().Format(time.RFC850),inFile)
	fileState , err := fi.Stat()
	if err != nil {
		errInfo := fmt.Sprint("%cannot get file[%s],error message: %s",inFile,err)
		log.Fatal(errInfo)
	}
	size := fileState.Size()
	buffer := make([]byte,size)
	br := bufio.NewReader(fi)
	_,err = br.Read(buffer)
	if err != nil {
		errInfo := fmt.Sprintf("cannot reade contens from file[%s]",inFile)
		log.Fatal(errInfo)
	}
	//map contents to keyvalue pairs
	debug("%s begin mapping contents in file[%s] to key/val pairs",time.Now().Format(time.RFC850))
	keyvalues :=  mapF(inFile,string(buffer))
	debug("%s end mapping contents in file[%s]",time.Now().Format(time.RFC850))
	debug("%s write key/val pairs into reduce files",time.Now().Format(time.RFC850))
	//prcompute hash value of every key
	hashValue := make(map[string]uint32)
	for _,keyValue := range keyvalues {
		if _,ok := hashValue[keyValue.Key];!ok {
			//already compute hash val for this key,let's skip it
			continue
		}
		hashValue[keyValue.Key] = ihash(keyValue.Key)
	}
	for i := 0;i < nReduce;i++ {
		//create #nReduce intermediate files
		fileName := reduceName(jobName,mapTaskNumber,i)
		reduceFile,err := os.Create(fileName)
		if err != nil {
			log.Fatal("cannot create intermediate file[%s]",fileName)
		}
		jsonBufferReader := json.NewEncoder(reduceFile)
		for _,keyValue := range keyvalues {
			//store every key/val pair to corresponding reduce file using hash function
			if hashValue[keyValue.Key] % uint32(nReduce) == uint32(i) {
				if err := jsonBufferReader.Encode(&keyValue);err != nil {
					log.Fatal("ecode err:",err)
				}
			}
		}
		if err := reduceFile.Close();err != nil{
			errInfo := fmt.Sprintf("cannot close reduce file[%s],error message:",fileName,err)
			log.Fatal(errInfo)
		}
	}
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

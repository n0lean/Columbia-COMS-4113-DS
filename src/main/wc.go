package main

import (
	"os"
	"strconv"
)
import "fmt"
import "strings"
import "unicode"
import "mapreduce"
import "container/list"

// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
	wordMap := make(map[string]int)
	words := strings.FieldsFunc(value, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	for _, word := range words {
		wordMap[word]++
	}
	res := new(list.List)
	for k, v := range wordMap {
		element := mapreduce.KeyValue{Key: k, Value: strconv.Itoa(v)}
		res.PushBack(element)
	}
	return res
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
	count := 0
	for element := values.Front(); element != nil; element = element.Next() {
		kv := element.Value.(string)
		c, err := strconv.Atoi(kv)
		if err != nil {
			continue
		}
		count += c
	}
	return strconv.Itoa(count)
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) != 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		if os.Args[3] == "sequential" {
			mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
		} else {
			mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
			// Wait until MR is done
			<-mr.DoneChannel
		}
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
	}
}

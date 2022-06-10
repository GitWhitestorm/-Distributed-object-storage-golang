package main

import (
	"Distributed-object-storage-golang/elasticsearch"
	"log"
)

const MIN_VERSION_COUNT = 5

func main() {
	buckets, err := elasticsearch.SearchVersionStatus(MIN_VERSION_COUNT + 1)
	if err != nil {
		log.Println(err)
		return
	}
	for i := range buckets {
		bucket := buckets[i]
		for v := 0; v < bucket.Doc_count-MIN_VERSION_COUNT; v++ {
			elasticsearch.DelMetadata(bucket.Key, v+int(bucket.Min_version.Value))
		}
	}
}

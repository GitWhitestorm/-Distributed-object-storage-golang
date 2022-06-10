package main

import (
	"Distributed-object-storage-golang/apiServers/objects"
	"Distributed-object-storage-golang/elasticsearch"
	"Distributed-object-storage-golang/utils"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func main() {
	files, _ := filepath.Glob(os.Getenv("STORAGE_ROOT") + "/objects/*")
	for i := range files {
		hash := strings.Split(filepath.Base(files[i]), ".")[0]
		ver
	}
}

func verify(hash string) {
	log.Println("verify", hash)
	size, err := elasticsearch.SearchHashSize(hash)
	if err != nil {
		log.Println(err)
		return
	}
	stream, err := objects.GetStream(hash, size)
	if err != nil {
		log.Println(err)
		return
	}
	d := utils.CalculateHash(stream)
	if d != hash {
		log.Println("object hash mismatch, calculated=%s,request=%s", d, hash)

	}
	stream.Close()
}

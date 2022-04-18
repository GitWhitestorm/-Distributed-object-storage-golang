package main

import (
	"Distributed-object-storage-golang/objects"
	"flag"
	"log"
	"net/http"
	"os"
)

var (
	listen_address string
	storage_root   string
)

// 绑定参数
func flagInit() {
	flag.StringVar(&listen_address, "LISTEN_ADDRESS", ":8881", "端口地址")
	flag.StringVar(&storage_root, "STORAGE_ROOT", "F:/go/src/Distributed-object-storage-golang/storage", "存储地址")
}

func main() {
	flagInit()
	flag.Parse()
	// 设置环境变量
	os.Setenv("LISTEN_ADDRESS", listen_address)
	os.Setenv("STORAGE_ROOT", storage_root)
	http.HandleFunc("/objects/", objects.Handler)
	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}

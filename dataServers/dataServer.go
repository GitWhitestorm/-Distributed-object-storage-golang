package main

import (
	"Distributed-object-storage-golang/dataServers/heartbeat"
	"Distributed-object-storage-golang/dataServers/locate"
	"Distributed-object-storage-golang/dataServers/objects"
	"flag"
	"log"
	"net/http"
	"os"
)

var (
	listen_address  string
	storage_root    string
	rabbitmq_server string
)

// 绑定参数
func flagInit() {
	flag.StringVar(&listen_address, "LISTEN_ADDRESS", ":8881", "端口地址")
	flag.StringVar(&storage_root, "STORAGE_ROOT", "F:/go/src/Distributed-object-storage-golang/dataServers/storage", "存储地址")
	flag.StringVar(&rabbitmq_server, "RABBITMQ_SERVER", "amqp://guest:guest@localhost:5672", "rabbitmq服务地址")
}

func main() {
	flagInit()
	flag.Parse()
	// 设置环境变量
	os.Setenv("LISTEN_ADDRESS", listen_address)
	os.Setenv("STORAGE_ROOT", storage_root)
	os.Setenv("RABBITMQ_SERVER", rabbitmq_server)
	// 往apiServer发送心跳
	go heartbeat.StartHeartbeat()

	// 开启监听服务，如果监听到请求的对象存在自己的机器上时，则往dateServers发送自己的地址
	go locate.StratLocate()
	http.HandleFunc("/objects/", objects.Handler)
	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}

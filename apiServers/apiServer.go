package main

import (
	"Distributed-object-storage-golang/apiServers/heartbeat"
	"Distributed-object-storage-golang/apiServers/locate"
	"Distributed-object-storage-golang/apiServers/objects"
	"flag"
	"log"
	"net/http"
	"os"
)

var (
	listen_address  string
	rabbitmq_server string
)

// 绑定参数
func flagInit() {
	flag.StringVar(&rabbitmq_server, "RABBITMQ_SERVER", "amqp://guest:guest@localhost:5672", "rabbitmq服务地址")
	flag.StringVar(&listen_address, "LISTEN_ADDRESS", ":8880", "端口地址")

}

func main() {
	flag.Parse()
	flagInit()
	// 设置环境变量
	os.Setenv("LISTEN_ADDRESS", listen_address)
	os.Setenv("RABBITMQ_SERVER", rabbitmq_server)
	go heartbeat.ListenHeartbeat()
	// 对象服务
	http.HandleFunc("/objects/", objects.Handler)
	// 定位服务
	http.HandleFunc("/locate/", locate.Handler)

	log.Fatal(http.ListenAndServe(os.Getenv("LISTEN_ADDRESS"), nil))
}

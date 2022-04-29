package locate

import (
	"Distributed-object-storage-golang/rabbitmq"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method

	if m != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	info := Locate(strings.Split(r.URL.EscapedPath(), "/")[2])

	if len(info) == 0 {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	b, _ := json.Marshal(info)
	w.Write(b)
}

// 广播给数据服务节点定位
func Locate(objectName string) string {
	mq := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))

	// 往消息队列中推送
	mq.Publish("dataServers", objectName)

	ch := mq.Consume()

	go func() {
		time.Sleep(time.Second)
		// 关闭消息队列
		mq.Close()
	}()

	msg := <-ch
	// 定位成功

	fmt.Println("api-locate", string(msg.Body))
	s, _ := strconv.Unquote(string(msg.Body))

	return s
}
func Exist(name string) bool {
	return Locate(name) != ""
}

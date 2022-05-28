package locate

import (
	"Distributed-object-storage-golang/rabbitmq"
	"Distributed-object-storage-golang/rs"
	"Distributed-object-storage-golang/types"
	"encoding/json"

	"net/http"
	"os"
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
func Locate(objectName string) (locateInfo map[int]string) {
	mq := rabbitmq.New(os.Getenv("RABBITMQ_SERVER"))

	// 往消息队列中推送
	mq.Publish("dataServers", objectName)

	ch := mq.Consume()

	go func() {
		time.Sleep(time.Second)
		// 关闭消息队列
		mq.Close()
	}()

	locateInfo = make(map[int]string)
	for i := 0; i < rs.ALL_SHARDS; i++ {
		msg := <-ch
		if len(msg.Body) == 0 {
			return
		}
		// 定位成功
		var info types.LocateMessage
		json.Unmarshal(msg.Body,&info)
	
		locateInfo[info.Id] = info.Addr
	}

	return locateInfo
}
func Exist(name string) bool {
	return len(Locate(name)) >= rs.DATA_SHARDS
}

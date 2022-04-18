package objects

import (
	"io"
	"log"
	"net/http"
	"os"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method
	if m == http.MethodPut {
		put(w, r)
		return
	}
	if m == http.MethodGet {
		get(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}

// 请求URL：PUT/objects/<object_name>
// 更新对象 写入 $STORAGE_ROOT/objects/<object_name>
func put(w http.ResponseWriter, r *http.Request) {

	// 创建对象
	file, err := os.Create(os.Getenv("STORAGE_ROOT") + "/objects/" +
		strings.Split(r.URL.EscapedPath(), "/")[2])

	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer file.Close()
	// 复制
	io.Copy(file, r.Body)
}

// 请求URL：GET/objects/<object_name>
// 获取对象 从$STORAGE_ROOT/objects/<object_name>获取
func get(w http.ResponseWriter, r *http.Request) {
	file, err := os.Open(os.Getenv("STORAGE_ROOT") + "/objects/" +
		strings.Split(r.URL.EscapedPath(), "/")[2])
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	io.Copy(w, file)
}

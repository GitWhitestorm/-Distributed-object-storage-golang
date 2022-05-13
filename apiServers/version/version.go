package version

import (
	"Distributed-object-storage-golang/elasticsearch"
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	method := r.Method

	if method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	from := 0
	size := 1000

	name := strings.Split(r.URL.EscapedPath(), "/")[2]

	for {
		metas, err := elasticsearch.SearchAllVersions(name, from, size)

		if err != nil {
			log.Println(err)

			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		for i := range metas {
			b, _ := json.Marshal(metas[i])
			w.Write(b)
			w.Write([]byte("\n"))
		}
		// 判断是否有1000个 没有则退出，有则继续
		if len(metas) != size {
			return
		}
		from += size
	}
}

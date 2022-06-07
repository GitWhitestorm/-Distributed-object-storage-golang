package temp

import (
	"Distributed-object-storage-golang/elasticsearch"
	"Distributed-object-storage-golang/rs"
	"Distributed-object-storage-golang/utils"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
)

func Handler(w http.ResponseWriter, r *http.Request) {
	m := r.Method
	if m == http.MethodHead {
		head(w, r)
		return
	}
	if m == http.MethodPut {
		put(w, r)
		return
	}
	w.WriteHeader(http.StatusMethodNotAllowed)
}
func put(w http.ResponseWriter, r *http.Request) {
	token := strings.Split(r.URL.EscapedPath(), "/")[2]
	stream, err := rs.NewRSResumablePutStreamFromToken(token)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	current := stream.CurrentSize()
	if current == -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	offset := utils.GetOffsetFromHeader(r.Header)
	if current != offset {
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	bytes := make([]byte, rs.BLOCK_SIZE)
	for {
		n, err := io.ReadFull(r.Body, bytes)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		current += int64(n)
		if current > stream.Size {
			stream.Commit(false)
			log.Println("resumable put exceed size")
			w.WriteHeader(http.StatusForbidden)
			return
		}
		if n != rs.BLOCK_SIZE && current != stream.Size {
			return
		}
		stream.Write(bytes[:n])
		if current == stream.Size {
			stream.Flush()
			err := elasticsearch.AddVersion(stream.Name, stream.Hash, stream.Size)
			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
	}
}

func head(w http.ResponseWriter, r *http.Request) {
	token := strings.Split(r.URL.EscapedPath(), "/")[2]
	stream, err := rs.NewRSResumablePutStreamFromToken(token)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusForbidden)
		return
	}
	current := stream.CurrentSize()
	if current != -1 {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	w.Header().Set("content-length", fmt.Sprintf("%d", current))
}

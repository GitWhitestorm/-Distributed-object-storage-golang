package objectstream

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
)

//  封装写入流
type PutStream struct {
	writer *io.PipeWriter
	c      chan error
}

// 以服务节点地址和object对象名为参数
// 创建一个可以write的流对象，也就是PutStream
func NewPutStream(server, object string) *PutStream {

	// 同步内存管道
	// writer写入的，reader可以读取。
	// 阻塞的
	reader, writer := io.Pipe()

	c := make(chan error)

	go func() {
		// 转发请求
		request, _ := http.NewRequest("PUT", "http://"+server+"/objects/"+object, reader)

		client := http.Client{}
		response, err := client.Do(request)

		if err == nil && response.StatusCode != http.StatusOK {
			err = fmt.Errorf("dataServer return http code %d", response.StatusCode)
		}
		c <- err
	}()

	return &PutStream{writer, c}
}

func (w *PutStream) Write(b []byte) (n int, err error) {
	return w.writer.Write(b)
}

func (w *PutStream) Close() error {
	w.writer.Close()
	return <-w.c
}

type GetStream struct {
	reader io.Reader
}

func newGetStream(url string) (*GetStream, error) {
	response, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("dataServer return http code %d", response.StatusCode)
	}

	return &GetStream{response.Body}, nil
}

func NewGetStream(server, objcet string) (io.Reader, error) {
	if server == "" || objcet == "" {
		return nil, fmt.Errorf("invaild server %s object %s", server, objcet)
	}

	return newGetStream("http://" + server + "/objects/" + objcet)
}

func (g *GetStream) Read(b []byte) (n int, err error) {
	return g.reader.Read(b)
}

type TempPutStream struct {
	Server string
	Uuid   string
}

func NewTempPutStream(server, hash string, size int64) (*TempPutStream, error) {
	request, err := http.NewRequest("POST", "http://"+server+"/temp/"+hash, nil)

	if err != nil {
		return nil, err
	}
	request.Header.Set("size", fmt.Sprintf("%d", size))
	client := http.Client{}
	response, err := client.Do(request)

	if err != nil {
		return nil, err
	}
	uuid, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	return &TempPutStream{
		Server: server,
		Uuid:   string(uuid),
	}, nil
}

func (w *TempPutStream) Write(p []byte) (n int, err error) {
	request, err := http.NewRequest("PATCH", "http://"+w.Server+"/temp/"+w.Uuid, strings.NewReader(string(p)))

	if err != nil {
		return 0, err
	}
	client := http.Client{}
	response, err := client.Do(request)
	if err != nil {
		return 0, err
	}
	if response.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("dataServer return http code %d", response.StatusCode)
	}
	return len(p), nil
}

// 是否提交暂存
func (w *TempPutStream) Commit(good bool) {
	method := "DELETE"
	if good {
		method = "PUT"
	}
	request, _ := http.NewRequest(method, "http://"+w.Server+"/temp/"+w.Uuid, nil)
	client := http.Client{}
	client.Do(request)
}

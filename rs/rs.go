package rs

import (
	"Distributed-object-storage-golang/apiServers/objectstream"
	"Distributed-object-storage-golang/utils"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/klauspost/reedsolomon"
)

const (
	PARITY_SHARDS   = 2
	DATA_SHARDS     = 4
	ALL_SHARDS      = DATA_SHARDS + PARITY_SHARDS
	BLOCK_PER_SHARD = 8000
	BLOCK_SIZE      = BLOCK_PER_SHARD * DATA_SHARDS
)

// 继承encoder
type RSPutStream struct {
	*encoder
}

type encoder struct {
	// writes数组
	writes []io.Writer
	// RS码编码器
	enc reedsolomon.Encoder
	// 输入数据的缓存
	cache []byte
}

// dataServers表示服务节点组
// hash和size分别表示需要PUT独享的散列值和大小
func NewRSPutStream(dataServers []string, hash string, size int64) (*RSPutStream, error) {
	if len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataServers number mismatch")
	}
	//计算出每个分片的大小，size/4向上取整
	perShard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	writes := make([]io.Writer, ALL_SHARDS)
	var err error
	for i := range writes {
		writes[i], err = objectstream.NewTempPutStream(dataServers[i], fmt.Sprintf("%s.%d", hash, i), perShard)
		if err != nil {
			return nil, err
		}
	}
	enc := NewEncoder(writes)
	return &RSPutStream{enc}, nil
}
func NewEncoder(writes []io.Writer) *encoder {
	// 生成一个具有DATA_SHARDS数据盘和PARITY_SHARDS校验片的RS码编码器enc
	enc, _ := reedsolomon.New(DATA_SHARDS, PARITY_SHARDS)
	return &encoder{writes, enc, nil}
}

func (e *encoder) Write(p []byte) (n int, err error) {
	length := len(p)
	current := 0
	for length != 0 {
		// 根据
		next := BLOCK_SIZE - len(e.cache)
		if next > length {
			next = length
		}

		e.cache = append(e.cache, p[current:current+next]...)
		// 如果缓存等于BLOCK_SIZE则写进分片并刷新缓存
		// 缓存不满BLOCK_SIZE则等待下次调用
		if len(e.cache) == BLOCK_SIZE {
			e.Flush()
		}

		current += next
		length -= next
	}
	return len(p), nil
}
func (e *encoder) Flush() {
	if len(e.cache) == 0 {
		return
	}
	shards, _ := e.enc.Split(e.cache)
	e.enc.Encode(shards)
	for i := range shards {
		e.writes[i].Write(shards[i])
	}
	e.cache = []byte{}
}
func (s *RSPutStream) Commit(success bool) {
	// 将缓存中最后的数据刷新
	s.Flush()
	for i := range s.writes {
		s.writes[i].(*objectstream.TempPutStream).Commit(success)
	}
}

type RSGetStream struct {
	*decoder
}
type decoder struct {
	// readers和writers数组形成互补的关系
	// 对于某个分片id，要么在reader中存在相应的读取流，要么在writers中存在相应的写入流
	readers []io.Reader
	writers []io.Writer
	// 用于RS解码
	enc reedsolomon.Encoder
	// 表示对象的大小
	size int64
	// 用于缓存读取的数据
	cache     []byte
	cacheSize int
	// 表示已经读取了多少字节
	total int64
}

func NewRsGetStream(locateInfo map[int]string, dataServers []string, hash string, size int64) (*RSGetStream, error) {
	if len(locateInfo)+len(dataServers) != ALL_SHARDS {
		return nil, fmt.Errorf("dataServers number mismatch")
	}
	readers := make([]io.Reader, ALL_SHARDS)
	for i := 0; i < ALL_SHARDS; i++ {
		server := locateInfo[i]
		if server == "" {
			locateInfo[i] = dataServers[0]
			dataServers = dataServers[1:]
			continue
		}
		reader, err := objectstream.NewGetStream(server, fmt.Sprintf("%s.%d", hash, i))
		if err != nil {
			readers[i] = reader
		}
	}
	writers := make([]io.Writer, ALL_SHARDS)
	perShard := (size + DATA_SHARDS - 1) / DATA_SHARDS
	var err error
	// reader可能为nil
	for i := range readers {
		if readers[i] == nil {
			// 创建相应的临时对象写入流用于恢复分片，打开的流被保存在writes数组中
			writers[i], err = objectstream.NewTempPutStream(locateInfo[i], fmt.Sprintf("%s.%d", hash, i), perShard)
			if err != nil {
				return nil, err
			}
		}
	}
	dec := NewDecoder(readers, writers, size)
	return &RSGetStream{dec}, nil
}
func NewDecoder(readers []io.Reader, writers []io.Writer, size int64) *decoder {
	enc, _ := reedsolomon.New(DATA_SHARDS, PARITY_SHARDS)
	return &decoder{readers, writers, enc, size, nil, 0, 0}
}

func (d *decoder) Read(p []byte) (n int, err error) {
	if d.cacheSize == 0 {
		err := d.getData()
		if err != nil {
			return 0, nil
		}
	}
	length := len(p)
	if d.cacheSize < length {
		length = d.cacheSize
	}
	d.cacheSize -= length
	copy(p, d.cache[:length])
	d.cache = d.cache[length:]
	return length, nil
}
func (d *decoder) getData() error {
	// 判断当前已经解码的数据大小是否等于对象原始大小
	if d.total == d.size {
		return io.EOF
	}
	// 创建一个长度为6的数组shards
	shards := make([][]byte, ALL_SHARDS)
	// 创建一个长度为0的整形数组repairdIds
	repairIds := make([]int, 0)
	for i := range shards {
		// 判断该分片是否已经丢失
		if d.readers[i] == nil {
			repairIds = append(repairIds, i)
		} else {
			shards[i] = make([]byte, BLOCK_PER_SHARD)
			n, err := io.ReadFull(d.readers[i], shards[i])
			if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
				shards[i] = nil
			} else if n != BLOCK_PER_SHARD {
				shards[i] = shards[i][:n]
			}
		}
	}
	// 尝试恢复处理来
	err := d.enc.Reconstruct(shards)
	if err != nil {
		return err
	}
	for i := range repairIds {
		id := repairIds[i]
		d.writers[id].Write(shards[id])
	}
	// 遍历4个数据分片
	for i := 0; i < DATA_SHARDS; i++ {
		shardSize := int64(len(shards[i]))
		if d.total+shardSize > d.size {
			shardSize -= d.total + shardSize - d.size
		}
		d.cache = append(d.cache, shards[i][:shardSize]...)
		d.cacheSize += int(shardSize)
		d.total += shardSize
	}
	return nil
}

// 写入恢复分片的临时对象转正
func (s *RSGetStream) Close() {
	for i := range s.writers {
		if s.writers[i] != nil {
			s.writers[i].(*objectstream.TempPutStream).Commit(true)
		}
	}
}

type resumableToken struct {
	Name    string
	Size    int64
	Hash    string
	Servers []string
	Uuids   []string
}

type RSResumablePutStream struct {
	*RSPutStream
	*resumableToken
}

func NewRSResumablePutStream(dataServers []string, name, hash string,
	size int64) (*RSResumablePutStream, error) {
	putStream, err := NewRSPutStream(dataServers, hash, size)
	if err != nil {
		return nil, err
	}
	uuids := make([]string, ALL_SHARDS)
	for i := range uuids {
		uuids[i] = putStream.writes[i].(*objectstream.TempPutStream).Uuid

	}
	token := &resumableToken{name, size, hash, dataServers, uuids}
	return &RSResumablePutStream{putStream, token}, nil

}

// 生成字符串token
func (s *RSResumablePutStream) ToToken() string {
	b, _ := json.Marshal(s)
	return base64.StdEncoding.EncodeToString(b)
}

// offest表示需要跳过多少字节，whence表示起跳点
func (s *RSGetStream) Seek(offset int64, whence int) (int64, error) {
	if whence != io.SeekCurrent {
		panic("only support SeekCurrent")
	}
	if offset < 0 {
		panic("only support forward seek")
	}
	for offset != 0 {
		length := int64(BLOCK_SIZE)
		if offset < length {
			length = offset
		}
		buf := make([]byte, length)
		io.ReadFull(s, buf)
		offset -= length
	}
	return offset, nil
}
func NewRSResumablePutStreamFromToken(token string) (*RSResumablePutStream, error) {
	b, err := base64.StdEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	var t resumableToken
	err = json.Unmarshal(b, &t)
	if err != nil {
		return nil, err
	}
	writers := make([]io.Writer, ALL_SHARDS)
	for i := range writers {
		writers[i] = &objectstream.TempPutStream{Server: t.Servers[i], Uuid: t.Uuids[i]}
	}
	enc := NewEncoder(writers)
	return &RSResumablePutStream{&RSPutStream{enc}, &t}, nil
}
func (s *RSResumablePutStream) CurrentSize() int64 {
	r, err := http.Head(fmt.Sprintf("http://%s/temp/%s", s.Servers[0], s.Uuids[0]))
	if err != nil {
		log.Println(err)
		return -1
	}
	if r.StatusCode != http.StatusOK {
		log.Println(r.StatusCode)
		return -1
	}
	size := utils.GetSizeFromHeader(r.Header) * DATA_SHARDS
	if size > s.Size {
		size = s.Size
	}
	return size
}

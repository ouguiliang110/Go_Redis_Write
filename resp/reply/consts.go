package reply

//这里面主要包括五种写死的回复

//这里面主要保存固定的回复内容
type PongReply struct {
}

//客户端发送ping，其会回复pong，而且是固定写死的
var pongbytes = []byte("+PONG\r\n")

func (r PongReply) ToBytes() []byte {
	return pongbytes
}

//工厂模式那种
func MakePongReply() *PongReply {
	return &PongReply{}
}

//回复OK
type OkReply struct {
}

var okBytes = []byte("+Ok\r\n")

func (r OkReply) ToBytes() []byte {
	return okBytes
}

var theOkReply = new(OkReply)

func MakeOkReply() *OkReply {
	return theOkReply
}

//回复空的字符串
type NullBulkReply struct {
}

var nullBulkBytes = []byte("$-1\r\n") //空回复

func (n NullBulkReply) ToBytes() []byte {
	return nullBulkBytes
}
func MakeNullBulkReply() *NullBulkReply {
	return &NullBulkReply{}
}

//回复空数组
var emptyMultiBulkBytes = []byte("*0\r\n")

// EmptyMultiBulkReply is a empty list
type EmptyMultiBulkReply struct{}

// ToBytes marshal redis.Reply
func (r *EmptyMultiBulkReply) ToBytes() []byte {
	return emptyMultiBulkBytes
}

// 真的是空，什么都没有回应
type NoReply struct{}

var noBytes = []byte("")

// ToBytes marshal redis.Reply
func (r *NoReply) ToBytes() []byte {
	return noBytes
}

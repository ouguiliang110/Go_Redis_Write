package reply

import (
	"bytes"
	"go_redis_write/interface/resp"
	"strconv"
)

var (
	nullBulkReplyBytes = []byte("$-1")
	// CRLF is the line separator of redis serialization protocol 通信协议的结尾
	CRLF = "\r\n"
)

/* ---- Bulk Reply ---- */

//存储单个字符串，且二进制要安全

type BulkReply struct {
	Arg []byte //"moody"  "$5\r\nmoody\r\n"
}

func (b BulkReply) ToBytes() []byte {
	if len(b.Arg) == 0 {
		return nullBulkReplyBytes
	}
	return []byte("$" + strconv.Itoa(len(b.Arg)) + CRLF + string(b.Arg) + CRLF)
}
func MakeBulkReply(arg []byte) *BulkReply {
	return &BulkReply{arg}
}

/* ---- Multi Bulk Reply ---- */

//多个字符串的实现方法

type MultiBulkReply struct {
	Args [][]byte //多个字符串
}

func (r MultiBulkReply) ToBytes() []byte {
	argLen := len(r.Args)
	var buf bytes.Buffer //这个实现的效率比较高
	buf.WriteString("*" + strconv.Itoa(argLen) + CRLF)
	for _, arg := range r.Args {
		if arg == nil {
			buf.WriteString("$-1" + CRLF) //如果是空，则回复多字符数组为空
		} else {
			buf.WriteString("$" + strconv.Itoa(len(arg)) + CRLF + string(arg) + CRLF)
		}
	}

	return buf.Bytes()
}
func MakeMultiBulkReply(args [][]byte) *MultiBulkReply {
	return &MultiBulkReply{
		Args: args,
	}
}

/* ---- Status Reply ---- */

// StatusReply stores a simple status string 状态回复
type StatusReply struct {
	Status string
}

// MakeStatusReply creates StatusReply
func MakeStatusReply(status string) *StatusReply {
	return &StatusReply{
		Status: status,
	}
}

// ToBytes marshal redis.Reply
func (r *StatusReply) ToBytes() []byte {
	return []byte("+" + r.Status + CRLF)
}

/* ---- Int Reply ---- */

// IntReply stores an int64 number
type IntReply struct {
	Code int64
}

// MakeIntReply creates int reply
func MakeIntReply(code int64) *IntReply {
	return &IntReply{
		Code: code,
	}
}

// ToBytes marshal redis.Reply
func (r *IntReply) ToBytes() []byte {
	return []byte(":" + strconv.FormatInt(r.Code, 10) + CRLF)
}

//这个接口主要是承载我们的错误回复
type ErrorReply interface {
	Error() string
	ToBytes() []byte
}

// StandardErrReply represents handler error
type StandardErrReply struct {
	Status string
}

// ToBytes marshal redis.Reply
func (r *StandardErrReply) ToBytes() []byte {
	return []byte("-" + r.Status + CRLF)
}

func (r *StandardErrReply) Error() string {
	return r.Status
}

// MakeErrReply creates StandardErrReply
func MakeErrReply(status string) *StandardErrReply {
	return &StandardErrReply{
		Status: status,
	}
}

// IsErrorReply returns true if the given reply is error 判断是正常回复还是异常回复
func IsErrorReply(reply resp.Reply) bool {
	return reply.ToBytes()[0] == '-'
}

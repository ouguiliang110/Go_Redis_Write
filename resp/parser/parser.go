package parser

import (
	"bufio"
	"errors"
	"go_redis_write/interface/resp"
	"go_redis_write/lib/logger"
	"go_redis_write/resp/reply"
	"io"
	"runtime/debug"
	"strconv"
	"strings"
)

type Payload struct {
	Data resp.Reply
	Err  error
}

type readState struct {
	readingMultiLine  bool
	expectedArgsCount int //正在解析第几个数据块（或者是字符）
	msgType           byte
	args              [][]byte //有多少组数据块
	bulkLen           int64    //预设要读的字符，就是$后面的值
}

func (s *readState) finished() bool { //返回我们这个解析器有没有完成
	return s.expectedArgsCount > 0 && len(s.args) == s.expectedArgsCount
}

// ParseStream reads data from io.Reader (TCP server 那层） and send payloads through channel
func ParseStream(reader io.Reader) <-chan *Payload {
	ch := make(chan *Payload)
	go parse0(reader, ch) //为每个用户生成一个解析器，每个解析器开启了一个协程
	return ch
}

//*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n  SET key value
func parse0(reader io.Reader, ch chan<- *Payload) {
	defer func() {
		if err := recover(); err != nil {
			logger.Error(string(debug.Stack()))
		}
	}()
	bufReader := bufio.NewReader(reader)
	var state readState
	var err error
	var msg []byte
	for {
		// read line
		var ioErr bool
		msg, ioErr, err = readLine(bufReader, &state) //都进来一行数据
		if err != nil {                               //处理错误的一行
			if ioErr { // encounter io err, stop read
				ch <- &Payload{
					Err: err,
				}
				close(ch)
				return
			}
			// protocol err, reset read state
			ch <- &Payload{
				Err: err,
			}
			state = readState{}
			continue
		}

		//*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n  SET key value
		/*
		   1. 首先我们先读*3\r\n，发现有3行，则就进行这三行的遍历，而且修改为多行状态 再81行代码中，后返回
		   2. 则到$3\r\n  这个是发现已经多行状态了，则转到122行代码处理多行
		   3.
		*/

		// parse line
		if !state.readingMultiLine { //单行状态 开头处理处理*3\r\n
			// receive new response
			if msg[0] == '*' {
				// multi bulk reply
				err = parseMultiBulkHeader(msg, &state) //开启处理多行模式
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.expectedArgsCount == 0 { //客户发*，但是发过来只有一行
					ch <- &Payload{
						Data: &reply.EmptyMultiBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else if msg[0] == '$' { // bulk reply, 单行状态的处理//*3\r\n变成了多行模式，但字符串格式
				err = parseBulkHeader(msg, &state)
				if err != nil {
					ch <- &Payload{
						Err: errors.New("protocol error: " + string(msg)),
					}
					state = readState{} // reset state
					continue
				}
				if state.bulkLen == -1 { // null bulk reply
					ch <- &Payload{
						Data: &reply.NullBulkReply{},
					}
					state = readState{} // reset state
					continue
				}
			} else { //单行那种 +-：
				// single line reply
				result, err := parseSingleLineReply(msg)
				ch <- &Payload{ //放回结果
					Data: result,
					Err:  err,
				}
				state = readState{} // reset state
				continue
			}
		} else { //多行状态的接下来处理    比如上面处理的到$3\r\n
			// receive following bulk reply
			err = readBody(msg, &state)
			if err != nil {
				ch <- &Payload{
					Err: errors.New("protocol error: " + string(msg)),
				}
				state = readState{} // reset state
				continue
			}
			// if sending finished
			if state.finished() {
				var result resp.Reply
				if state.msgType == '*' {
					result = reply.MakeMultiBulkReply(state.args)
				} else if state.msgType == '$' {
					result = reply.MakeBulkReply(state.args[0])
				}
				ch <- &Payload{
					Data: result,
					Err:  err,
				}
				state = readState{}
			}
		}
	}
}

//*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n  SET key value
func readLine(bufReader *bufio.Reader, state *readState) ([]byte, bool, error) {
	var msg []byte
	var err error
	if state.bulkLen == 0 { // read normal line
		msg, err = bufReader.ReadBytes('\n')
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || msg[len(msg)-2] != '\r' {
			return nil, false, errors.New("protocol error: " + string(msg)) //协议错误，协议出现问题了
		}
	} else { // read bulk line (binary safe) 只
		msg = make([]byte, state.bulkLen+2)
		_, err = io.ReadFull(bufReader, msg)
		if err != nil {
			return nil, true, err
		}
		if len(msg) == 0 || //先判断有没有内容
			msg[len(msg)-2] != '\r' || //倒数第二个字符是否为'\r'
			msg[len(msg)-1] != '\n' { //倒数第二个字符是否为'\n'
			return nil, false, errors.New("protocol error: " + string(msg))
		}
		state.bulkLen = 0 //
	}
	return msg, false, nil
}

//*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n  SET key value
func parseMultiBulkHeader(msg []byte, state *readState) error { //数组那种类型解析，比如SET key value三种字符串
	var err error
	var expectedLine uint64 //来指示去去除*后的3
	//去除这个数字，其中如果值大于
	expectedLine, err = strconv.ParseUint(string(msg[1:len(msg)-2]), 10, 32)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if expectedLine == 0 { //用户发了个寂寞
		state.expectedArgsCount = 0
		return nil
	} else if expectedLine > 0 {
		// first line of multi bulk reply
		state.msgType = msg[0]        //表示读的是数组
		state.readingMultiLine = true //多行状态
		state.expectedArgsCount = int(expectedLine)
		state.args = make([][]byte, 0, expectedLine)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}
func parseBulkHeader(msg []byte, state *readState) error {
	var err error
	state.bulkLen, err = strconv.ParseInt(string(msg[1:len(msg)-2]), 10, 64)
	if err != nil {
		return errors.New("protocol error: " + string(msg))
	}
	if state.bulkLen == -1 { // null bulk
		return nil
	} else if state.bulkLen > 0 {
		state.msgType = msg[0]
		state.readingMultiLine = true
		state.expectedArgsCount = 1
		state.args = make([][]byte, 0, 1)
		return nil
	} else {
		return errors.New("protocol error: " + string(msg))
	}
}

//+OK\r\n    -err\r\n   这两种类型，主要是处理单类型的数据
func parseSingleLineReply(msg []byte) (resp.Reply, error) {
	str := strings.TrimSuffix(string(msg), "\r\n")
	var result resp.Reply
	switch msg[0] {
	case '+': // status reply
		result = reply.MakeStatusReply(str[1:])
	case '-': // err reply
		result = reply.MakeErrReply(str[1:])
	case ':': // int reply
		val, err := strconv.ParseInt(str[1:], 10, 64)
		if err != nil { //如果是这样出现协议错误
			return nil, errors.New("protocol error: " + string(msg))
		}
		result = reply.MakeIntReply(val)
	default:
		// parse as text protocol
		strs := strings.Split(str, " ")
		args := make([][]byte, len(strs))
		for i, s := range strs {
			args[i] = []byte(s)
		}
		result = reply.MakeMultiBulkReply(args)
	}
	return result, nil
}

// read the non-first lines of multi bulk reply or bulk reply
func readBody(msg []byte, state *readState) error {
	line := msg[0 : len(msg)-2]
	var err error
	if line[0] == '$' {
		// bulk reply
		state.bulkLen, err = strconv.ParseInt(string(line[1:]), 10, 64)
		if err != nil {
			return errors.New("protocol error: " + string(msg))
		}
		if state.bulkLen <= 0 { // null bulk in multi bulks
			state.args = append(state.args, []byte{})
			state.bulkLen = 0
		}
	} else {
		state.args = append(state.args, line)
	}
	return nil
}

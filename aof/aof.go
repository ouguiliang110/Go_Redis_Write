package aof

import (
	"go_redis_write/config"
	databaseface "go_redis_write/interface/database"
	"go_redis_write/lib/logger"
	"go_redis_write/lib/utils"
	"go_redis_write/resp/connection"
	"go_redis_write/resp/parser"
	"go_redis_write/resp/reply"
	"io"
	"os"
	"strconv"
	"sync"
)

// CmdLine is alias for [][]byte, represents a command line
type CmdLine = [][]byte

const (
	aofQueueSize = 1 << 16
)

type payload struct { //一个操作的数据结构
	cmdLine CmdLine
	dbIndex int
}

// AofHandler receive msgs from channel and write to AOF file
type AofHandler struct {
	db          databaseface.Database
	aofChan     chan *payload //AOF缓冲区
	aofFile     *os.File      //文件
	aofFilename string
	//当 aof 任务完成并准备关闭时，aof goroutine 将通过此channel将 msg 发送到 main goroutine
	// aof goroutine will send msg to main goroutine through this channel when aof tasks finished and ready to shutdown
	aofFinished chan struct{}
	//暂停 aof for startfinish aof 重写进度
	// pause aof for start/finish aof rewrite progress
	pausingAof sync.RWMutex
	currentDB  int
}

//创建AOF

// NewAOFHandler creates a new aof.AofHandler
func NewAOFHandler(db databaseface.Database) (*AofHandler, error) {
	handler := &AofHandler{}
	handler.aofFilename = config.Properties.AppendFilename
	handler.db = db
	//LoadAOF
	handler.LoadAof(0)
	aofFile, err := os.OpenFile(handler.aofFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	handler.aofFile = aofFile
	handler.aofChan = make(chan *payload, aofQueueSize)
	handler.aofFinished = make(chan struct{})
	go func() {
		handler.handleAof() //开一个协程不断监听AOF
	}()
	return handler, nil
}

// AddAof send command to aof goroutine through channel
func (handler *AofHandler) AddAof(dbIndex int, cmdLine CmdLine) {
	if config.Properties.AppendOnly && handler.aofChan != nil { //判断开不开启AOF
		handler.aofChan <- &payload{
			cmdLine: cmdLine,
			dbIndex: dbIndex,
		}
	}
}

// handleAof listen aof channel and write into file
func (handler *AofHandler) handleAof() {
	// serialized execution
	handler.currentDB = 0
	for p := range handler.aofChan { //不断监听这个Channel
		handler.pausingAof.RLock()          // prevent other goroutines from pausing aof
		if p.dbIndex != handler.currentDB { //如果db不一样则需要去调整一下
			// select db
			data := reply.MakeMultiBulkReply(utils.ToCmdLine("SELECT", strconv.Itoa(p.dbIndex))).ToBytes()
			_, err := handler.aofFile.Write(data)
			if err != nil {
				logger.Warn(err)
				continue // skip this command
			}
			handler.currentDB = p.dbIndex //改一下当前的DB
		}
		data := reply.MakeMultiBulkReply(p.cmdLine).ToBytes()
		_, err := handler.aofFile.Write(data)
		if err != nil {
			logger.Warn(err)
		}
		handler.pausingAof.RUnlock()
	}
	handler.aofFinished <- struct{}{}
}

// LoadAof read aof file
func (handler *AofHandler) LoadAof(maxBytes int) {
	// delete aofChan to prevent write again
	aofChan := handler.aofChan
	handler.aofChan = nil
	defer func(aofChan chan *payload) {
		handler.aofChan = aofChan
	}(aofChan)

	file, err := os.Open(handler.aofFilename)
	if err != nil {
		if _, ok := err.(*os.PathError); ok {
			return
		}
		logger.Warn(err)
		return
	}
	defer file.Close() //打开就要关闭

	var reader io.Reader
	if maxBytes > 0 {
		reader = io.LimitReader(file, int64(maxBytes))
	} else {
		reader = file
	}
	ch := parser.ParseStream(reader)
	fakeConn := &connection.FakeConn{} // only used for save dbIndex
	for p := range ch {
		if p.Err != nil {
			if p.Err == io.EOF {
				break
			}
			logger.Error("parse error: " + p.Err.Error())
			continue
		}
		if p.Data == nil {
			logger.Error("empty payload")
			continue
		}
		r, ok := p.Data.(*reply.MultiBulkReply)
		if !ok {
			logger.Error("require multi bulk reply")
			continue
		}
		ret := handler.db.Exec(fakeConn, r.Args)
		if reply.IsErrorReply(ret) {
			logger.Error("exec err", err)
		}
	}
}

// Close gracefully stops aof persistence procedure
func (handler *AofHandler) Close() {
	if handler.aofFile != nil {
		close(handler.aofChan)
		<-handler.aofFinished // wait for aof finished
		err := handler.aofFile.Close()
		if err != nil {
			logger.Warn(err)
		}
	}
}

// Package cluster provides a server side cluster which is transparent to client. You can connect to any node in the cluster to access all data in the cluster
package cluster

import (
	"context"
	"fmt"
	pool "github.com/jolestar/go-commons-pool/v2"
	"go_redis_write/config"
	"go_redis_write/database"
	databaseface "go_redis_write/interface/database"
	"go_redis_write/interface/resp"
	"go_redis_write/lib/consistenthash"
	"go_redis_write/lib/logger"
	"go_redis_write/resp/reply"
	"runtime/debug"
	"strings"
)

// ClusterDatabase represents a node of godis cluster
// it holds part of data and coordinates other nodes to finish transactions
type ClusterDatabase struct {
	self string //记录自己的名称和地址，这个相当于的主redis

	nodes          []string
	peerPicker     *consistenthash.NodeMap
	peerConnection map[string]*pool.ObjectPool
	db             databaseface.Database //本地的单机数据库
}

// MakeClusterDatabase creates and starts a node of cluster
func MakeClusterDatabase() *ClusterDatabase {
	cluster := &ClusterDatabase{
		self: config.Properties.Self,

		db:             database.NewStandaloneDatabase(),
		peerPicker:     consistenthash.NewNodeMap(nil),
		peerConnection: make(map[string]*pool.ObjectPool),
	}
	nodes := make([]string, 0, len(config.Properties.Peers)+1)
	for _, peer := range config.Properties.Peers { //遍历配置里面的peer节点，都append到nodes中
		nodes = append(nodes, peer)
	}
	nodes = append(nodes, config.Properties.Self) //在放入self节点
	cluster.peerPicker.AddNode(nodes...)
	ctx := context.Background()
	for _, peer := range config.Properties.Peers { //对兄弟节点创建连接池
		cluster.peerConnection[peer] = pool.NewObjectPoolWithDefaultConfig(ctx, &connectionFactory{
			Peer: peer,
		})
	}
	cluster.nodes = nodes
	return cluster
}

//表示redis的

// CmdFunc represents the handler of a redis command
type CmdFunc func(cluster *ClusterDatabase, c resp.Connection, cmdAndArgs [][]byte) resp.Reply

// Close stops current node of cluster
func (cluster *ClusterDatabase) Close() {
	cluster.db.Close()
}

var router = makeRouter()

// Exec executes command on cluster
func (cluster *ClusterDatabase) Exec(c resp.Connection, cmdLine [][]byte) (result resp.Reply) {
	defer func() {
		if err := recover(); err != nil {
			logger.Warn(fmt.Sprintf("error occurs: %v\n%s", err, string(debug.Stack())))
			result = &reply.UnKnownErrReply{}
		}
	}()
	cmdName := strings.ToLower(string(cmdLine[0]))
	cmdFunc, ok := router[cmdName] //跳转到需要执行的函数
	if !ok {
		return reply.MakeErrReply("ERR unknown command '" + cmdName + "', or not supported in cluster mode")
	}
	result = cmdFunc(cluster, c, cmdLine)
	return
}

// AfterClientClose does some clean after client close connection
func (cluster *ClusterDatabase) AfterClientClose(c resp.Connection) {
	cluster.db.AfterClientClose(c)
}

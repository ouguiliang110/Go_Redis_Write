package cluster

import (
	"context"
	"errors"
	"go_redis_write/interface/resp"
	"go_redis_write/lib/utils"
	"go_redis_write/resp/client"
	"go_redis_write/resp/reply"
	"strconv"
)

//在连接池里获取一个连接
func (cluster *ClusterDatabase) getPeerClient(peer string) (*client.Client, error) {
	factory, ok := cluster.peerConnection[peer] //取到连接池
	if !ok {
		return nil, errors.New("connection factory not found")
	}
	raw, err := factory.BorrowObject(context.Background()) //获取一个连接
	if err != nil {
		return nil, err
	}
	conn, ok := raw.(*client.Client) //类型断言
	if !ok {
		return nil, errors.New("connection factory make wrong type")
	}
	return conn, nil
}

//返还连接给连接池
func (cluster *ClusterDatabase) returnPeerClient(peer string, peerClient *client.Client) error {
	connectionFactory, ok := cluster.peerConnection[peer]
	if !ok {
		return errors.New("connection factory not found")
	}
	return connectionFactory.ReturnObject(context.Background(), peerClient)
}

//客户端发送命令给self节点。self系欸但转发给其对应的一致性哈希的peer节点
// relay relays command to peer
// select db by c.GetDBIndex()
// cannot call Prepare, Commit, execRollback of self node
func (cluster *ClusterDatabase) relay(peer string, c resp.Connection, args [][]byte) resp.Reply {
	if peer == cluster.self { //如果是我们的自己的地址，自己执行就行
		// to self db
		return cluster.db.Exec(c, args)
	}
	peerClient, err := cluster.getPeerClient(peer) //获取一个peer的连接
	if err != nil {
		return reply.MakeErrReply(err.Error())
	}
	defer func() { //用于归还连接
		_ = cluster.returnPeerClient(peer, peerClient)
	}()
	peerClient.Send(utils.ToCmdLine("SELECT", strconv.Itoa(c.GetDBIndex())))
	return peerClient.Send(args)
}

// broadcast broadcasts command to all node in cluster
func (cluster *ClusterDatabase) broadcast(c resp.Connection, args [][]byte) map[string]resp.Reply {
	result := make(map[string]resp.Reply)
	for _, node := range cluster.nodes {
		reply := cluster.relay(node, c, args)
		result[node] = reply
	}
	return result
}

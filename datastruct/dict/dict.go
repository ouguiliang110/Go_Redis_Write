package dict

type Consumer func(key string, val interface{}) bool

type Dict interface {
	Get(key string) (val interface{}, exists bool)
	Len() int                                             //返回有多少数据
	Put(key string, val interface{}) (result int)         //把一个值存进去
	PutIfAbsent(key string, val interface{}) (result int) //如果不存在，再存进去
	PutIfExists(key string, val interface{}) (result int) //
	Remove(key string) (result int)
	ForEach(consumer Consumer)
	Keys() []string
	RandomKeys(limit int) []string         //返回随机的键
	RandomDistinctKeys(limit int) []string //返回不同的键
	Clear()                                //把字典清楚掉
}

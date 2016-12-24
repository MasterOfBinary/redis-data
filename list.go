package redistypes

import (
	"github.com/garyburd/redigo/redis"
	"github.com/golang/groupcache/singleflight"
)

// List provides functionality for Redis lists.
type List interface {
	RPush(arg ...interface{}) (uint64, error)

	RPushX(arg ...interface{}) (uint64, error)

	LPush(arg ...interface{}) (uint64, error)

	LPushX(arg ...interface{}) (uint64, error)

	LRange(start, stop int64) ([]interface{}, error)
}

type redisList struct {
	conn redis.Conn
	name string
	sync singleflight.Group
}

// NewRedisList creates a Redis implementation of List.
func NewRedisList(conn redis.Conn, name string) List {
	return &redisList{
		conn: conn,
		name: name,
	}
}

func (l *redisList) RPush(args ...interface{}) (uint64, error) {
	args = prependInterface(l.name, args...)
	return redis.Uint64(l.conn.Do("RPUSH", args...))
}

func (l *redisList) RPushX(args ...interface{}) (uint64, error) {
	args = prependInterface(l.name, args...)
	return redis.Uint64(l.conn.Do("RPUSHX", args...))
}

func (l *redisList) LPush(args ...interface{}) (uint64, error) {
	args = prependInterface(l.name, args...)
	return redis.Uint64(l.conn.Do("LPUSH", args...))
}

func (l *redisList) LPushX(args ...interface{}) (uint64, error) {
	args = prependInterface(l.name, args...)
	return redis.Uint64(l.conn.Do("LPUSHS", args...))
}

func (l *redisList) LRange(start, stop int64) ([]interface{}, error) {
	return redis.Values(l.sync.Do("LRANGE", func() (interface{}, error) {
		return l.conn.Do("LRANGE", l.name, start, stop)
	}))
}

// Package list contains a Go implementation of the list data structure in Redis.
//
// For more information about how the data structure works, see the Redis documentation.
package list

import (
	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/groupcache/singleflight"
)

// List provides functionality for Redis lists.
type List interface {
	Name() string

	LeftPop() (interface{}, error)

	LeftPush(args ...interface{}) (uint64, error)

	LeftPushX(arg interface{}) (uint64, error)

	Range(start, stop int64) ([]interface{}, error)

	RightPop() (interface{}, error)

	RightPush(args ...interface{}) (uint64, error)

	RightPushX(arg interface{}) (uint64, error)
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

func (r redisList) Name() string {
	return r.name
}

func (r *redisList) LeftPop() (interface{}, error) {
	return r.conn.Do("LPOP", r.name)
}

func (r *redisList) LeftPush(args ...interface{}) (uint64, error) {
	args = internal.PrependInterface(r.name, args...)
	return redis.Uint64(r.conn.Do("LPUSH", args...))
}

func (r *redisList) LeftPushX(arg interface{}) (uint64, error) {
	return redis.Uint64(r.conn.Do("LPUSHX", r.name, arg))
}

func (r *redisList) Range(start, stop int64) ([]interface{}, error) {
	return redis.Values(r.sync.Do("LRANGE", func() (interface{}, error) {
		return r.conn.Do("LRANGE", r.name, start, stop)
	}))
}

func (r *redisList) RightPop() (interface{}, error) {
	return r.conn.Do("RPOP", r.name)
}

func (r *redisList) RightPush(args ...interface{}) (uint64, error) {
	args = internal.PrependInterface(r.name, args...)
	return redis.Uint64(r.conn.Do("RPUSH", args...))
}

func (r *redisList) RightPushX(arg interface{}) (uint64, error) {
	return redis.Uint64(r.conn.Do("RPUSHX", r.name, arg))
}
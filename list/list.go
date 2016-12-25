// Package list contains a Go implementation of the list data structure in Redis. For more information about
// how the data structure works, see the Redis documentation.
package list

import (
	"errors"
	"time"

	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/garyburd/redigo/redis"
	"github.com/golang/groupcache/singleflight"
)

// List is a Redis implementation of a linked list.
type List interface {
	// Name returns the name of the List.
	Name() string

	// BlockingLeftPop implements the Redis command BLPOP. It works like LPOP but it
	// blocks until an element exists in the list or timeout is reached. If the timeout
	// is reached, nil is returned. A timeout of 0 can be used to block indefinitely.
	//
	// Since Redis specifies timeout to be in seconds, millisecond-level precision is
	// not possible. If the timeout is not a multiple of one second, an error will be
	// returned.
	//
	// See https://redis.io/commands/blpop.
	BlockingLeftPop(timeout time.Duration) (interface{}, error)

	// BlockingRightPop implements the Redis command BRPOP. It works like RPOP but it
	// blocks until an element exists in the list or timeout is reached. If the timeout
	// is reached, nil is returned. A timeout of 0 can be used to block indefinitely.
	//
	// Since Redis specifies timeout to be in seconds, millisecond-level precision is
	// not possible. If the timeout is not a multiple of one second, an error will be
	// returned.
	//
	// See https://redis.io/commands/brpop.
	BlockingRightPop(timeout time.Duration) (interface{}, error)

	// LeftPop implements the Redis command LPOP. It pops the leftmost value from the
	// list and returns it. If no such value exists, it returns nil.
	//
	// See https://redis.io/commands/lpop.
	LeftPop() (interface{}, error)

	// LeftPush implements the Redis command LPUSH. It pushes one or more values onto the left of the list.
	// It returns an error or the total number of values in the list.
	//
	// See https://redis.io/commands/lpush.
	LeftPush(args ...interface{}) (uint64, error)

	// LeftPushX implements the Redis command LPUSHX. It pushes one value onto the left of a list that
	// already exists. It returns an error or the total number of values in the list. If the list doesn't
	// already exist, it is not created and 0 is returned.
	//
	// See https://redis.io/commands/lpushx.
	LeftPushX(arg interface{}) (uint64, error)

	// Length implements the Redis command LLEN. It returns the length of the list. If the
	// list does not already exist, it returns 0.
	//
	// See https://redis.io/commands/llen.
	Length() (uint64, error)

	// Range implements the Redis command LRANGE. It returns a range of values in the list, starting at
	// index start and ending at index stop. If end is negative, it returns all values from start to the
	// end of the list.
	//
	// See https://redis.io/commands/lrange.
	Range(start, stop int64) ([]interface{}, error)

	// RightPop implements the Redis command RPOP. It pops the rightmost value from the list and returns it.
	// If no such value exists, it returns nil.
	//
	// See https://redis.io/commands/rpop.
	RightPop() (interface{}, error)

	// RightPush implements the Redis command RPUSH. It pushes one or more values onto the right of the list.
	// It returns an error or the total number of values in the list.
	//
	// See https://redis.io/commands/rpush.
	RightPush(args ...interface{}) (uint64, error)

	// RightPushX implements the Redis command RPUSHX. It pushes one value onto the right of a list that
	// already exists. It returns an error or the total number of values in the list. If the list doesn't
	// already exist, it is not created and 0 is returned.
	//
	// See https://redis.io/commands/rpushx.
	RightPushX(arg interface{}) (uint64, error)
}

type redisList struct {
	conn redis.Conn
	name string
	sync singleflight.Group
}

// NewRedisList creates a Redis implementation of List given redigo connection conn and name. The
// Redis key used to identify the List will be name.
func NewRedisList(conn redis.Conn, name string) List {
	return &redisList{
		conn: conn,
		name: name,
	}
}

func (r redisList) Name() string {
	return r.name
}

func (r *redisList) BlockingLeftPop(timeout time.Duration) (interface{}, error) {
	seconds := int64(timeout.Seconds())
	if timeout.Nanoseconds()-seconds*time.Second.Nanoseconds() != 0 {
		return nil, errors.New("Duration is not a multiple of one second")
	}

	values, err := redis.Values(r.conn.Do("BLPOP", r.name, seconds))
	if err != nil {
		return nil, err
	} else if len(values) != 2 {
		return nil, errors.New("Unexpected response length")
	}
	return values[1], err
}

func (r *redisList) BlockingRightPop(timeout time.Duration) (interface{}, error) {
	seconds := int64(timeout.Seconds())
	if timeout.Nanoseconds()-seconds*time.Second.Nanoseconds() != 0 {
		return nil, errors.New("Duration is not a multiple of one second")
	}

	values, err := redis.Values(r.conn.Do("BRPOP", r.name, seconds))
	if err != nil {
		return nil, err
	} else if len(values) != 2 {
		return nil, errors.New("Unexpected response length")
	}
	return values[1], err
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

func (r *redisList) Length() (uint64, error) {
	return redis.Uint64(r.conn.Do("LLEN", r.name))
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

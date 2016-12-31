// Package list contains a Go implementation of the list data structure in Redis. For more information about
// how the data structure works, see the Redis documentation.
package list

import (
	"errors"
	"time"

	"github.com/MasterOfBinary/redistypes"
	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/garyburd/redigo/redis"
)

type Adjacency string

const (
	Before Adjacency = "BEFORE"
	After            = "AFTER"
)

// List is a Redis implementation of a linked list.
type List interface {
	// Base returns the base Type.
	Base() redistypes.Type

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

	// BlockingRightPopLeftPush implements the Redis command BRPOPLPUSH. It works like
	// RightPopLeftPush except it blocks until timeout is reached. A timeout of 0 can
	// be used to block indefinitely.
	//
	// Since Redis specifies timeout to be in seconds, millisecond-level precision is
	// not possible. If the timeout is not a multiple of one second, an error will be
	// returned.
	//
	// See https://redis.io/commands/brpoplpush.
	BlockingRightPopLeftPush(destination List, timeout time.Duration) (interface{}, error)

	// Index implements the Redis command LINDEX. It returns the value at index in
	// the list. The index is 0-based, with the first index 0. Negative numbers
	// denote indices starting at the end of the list, as described by the documentation.
	//
	// See https://redis.io/commands/lindex.
	Index(index int64) (interface{}, error)

	// Insert implements the Redis command LINSERT. It inserts a value either before
	// or after the pivot, depending on adj. It returns the new length of the list,
	// or -1 if the pivot value wasn't found.
	//
	// See https://redis.io/commands/linsert.
	Insert(adj Adjacency, pivot interface{}, value interface{}) (int64, error)

	// LeftPop implements the Redis command LPOP. It pops the leftmost value from the
	// list and returns it. If no such value exists, it returns nil.
	//
	// See https://redis.io/commands/lpop.
	LeftPop() (interface{}, error)

	// LeftPush implements the Redis command LPUSH. It pushes one or more values onto
	// the left of the list. It returns an error or the total number of values in the
	// list.
	//
	// See https://redis.io/commands/lpush.
	LeftPush(args ...interface{}) (uint64, error)

	// LeftPushX implements the Redis command LPUSHX. It pushes one value onto the left
	// of a list that already exists. It returns an error or the total number of values
	// in the list. If the list doesn't already exist, it is not created and 0 is
	// returned.
	//
	// See https://redis.io/commands/lpushx.
	LeftPushX(arg interface{}) (uint64, error)

	// Length implements the Redis command LLEN. It returns the length of the list. If the
	// list does not already exist, it returns 0.
	//
	// See https://redis.io/commands/llen.
	Length() (uint64, error)

	// Range implements the Redis command LRANGE. It returns a range of values in the
	// list, starting at index start and ending at index stop. If end is negative, it
	// returns all values from start to the end of the list.
	//
	// See https://redis.io/commands/lrange.
	Range(start, stop int64) ([]interface{}, error)

	// Remove implements the Redis command LREM. It removes a number of occurrences of
	// value from the list. If count == 0, it removes all occurrences. If count > 0,
	// it removes the first count occurrences, starting from the beginning of the list.
	// If count < 0, it removes the first -count occurrences, starting from the end of
	// the list.
	//
	// Remove returns the number of values removed, or an error.
	//
	// See https://redis.io/commands/lrem.
	Remove(count int64, value interface{}) (uint64, error)

	// RightPop implements the Redis command RPOP. It pops the rightmost value from the
	// list and returns it. If no such value exists, it returns nil.
	//
	// See https://redis.io/commands/rpop.
	RightPop() (interface{}, error)

	// RightPopLeftPush implements the Redis command RPOPLPUSH. It pops the value on the
	// right of the list and pushes it on the left of destination.
	//
	// See https://redis.io/commands/rpoplpush.
	RightPopLeftPush(destination List) (interface{}, error)

	// RightPush implements the Redis command RPUSH. It pushes one or more values onto
	// the right of the list. It returns an error or the total number of values in the list.
	//
	// See https://redis.io/commands/rpush.
	RightPush(args ...interface{}) (uint64, error)

	// RightPushX implements the Redis command RPUSHX. It pushes one value onto the
	// right of a list that already exists. It returns an error or the total number of
	// values in the list. If the list doesn't already exist, it is not created and 0 is
	// returned.
	//
	// See https://redis.io/commands/rpushx.
	RightPushX(arg interface{}) (uint64, error)

	// Set implements the Redis command LSET. It sets the value at the specified index
	// to value. For more information about the index, see Index.
	//
	// See https://redis.io/commands/lset.
	Set(index int64, value interface{}) error
}

type redisList struct {
	conn redis.Conn
	base redistypes.Type
}

// NewRedisList creates a Redis implementation of List given redigo connection conn and name. The
// Redis key used to identify the List will be name.
func NewRedisList(conn redis.Conn, name string) List {
	return &redisList{
		conn: conn,
		base: redistypes.NewRedisType(conn, name),
	}
}

func (r redisList) Base() redistypes.Type {
	return r.base
}

func (r *redisList) BlockingLeftPop(timeout time.Duration) (interface{}, error) {
	seconds := int64(timeout.Seconds())
	if timeout.Nanoseconds()-seconds*time.Second.Nanoseconds() != 0 {
		return nil, errors.New("Duration is not a multiple of one second")
	}

	values, err := redis.Values(r.conn.Do("BLPOP", r.Base().Name(), seconds))
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

	values, err := redis.Values(r.conn.Do("BRPOP", r.Base().Name(), seconds))
	if err != nil {
		return nil, err
	} else if len(values) != 2 {
		return nil, errors.New("Unexpected response length")
	}
	return values[1], err
}

func (r *redisList) BlockingRightPopLeftPush(destination List, timeout time.Duration) (interface{}, error) {
	seconds := int64(timeout.Seconds())
	if timeout.Nanoseconds()-seconds*time.Second.Nanoseconds() != 0 {
		return nil, errors.New("Duration is not a multiple of one second")
	}

	value, err := r.conn.Do("BRPOPLPUSH", r.Base().Name(), destination.Base().Name(), seconds)
	if err != nil {
		return nil, err
	}

	return value, err
}

func (r *redisList) Index(index int64) (interface{}, error) {
	return r.conn.Do("LINDEX", r.Base().Name(), index)
}

func (r *redisList) Insert(adj Adjacency, pivot interface{}, value interface{}) (int64, error) {
	return redis.Int64(r.conn.Do("LINSERT", r.Base().Name(), string(adj), pivot, value))
}

func (r *redisList) LeftPop() (interface{}, error) {
	return r.conn.Do("LPOP", r.Base().Name())
}

func (r *redisList) LeftPush(args ...interface{}) (uint64, error) {
	args = internal.PrependInterface(r.Base().Name(), args...)
	return redis.Uint64(r.conn.Do("LPUSH", args...))
}

func (r *redisList) LeftPushX(arg interface{}) (uint64, error) {
	return redis.Uint64(r.conn.Do("LPUSHX", r.Base().Name(), arg))
}

func (r *redisList) Length() (uint64, error) {
	return redis.Uint64(r.conn.Do("LLEN", r.Base().Name()))
}

func (r *redisList) Range(start, stop int64) ([]interface{}, error) {
	return redis.Values(r.conn.Do("LRANGE", r.Base().Name(), start, stop))
}

func (r *redisList) Remove(count int64, value interface{}) (uint64, error) {
	return redis.Uint64(r.conn.Do("LREM", r.Base().Name(), count, value))
}

func (r *redisList) RightPopLeftPush(destination List) (interface{}, error) {
	return r.conn.Do("RPOPLPUSH", r.Base().Name(), destination.Base().Name())
}

func (r *redisList) RightPop() (interface{}, error) {
	return r.conn.Do("RPOP", r.Base().Name())
}

func (r *redisList) RightPush(args ...interface{}) (uint64, error) {
	args = internal.PrependInterface(r.Base().Name(), args...)
	return redis.Uint64(r.conn.Do("RPUSH", args...))
}

func (r *redisList) RightPushX(arg interface{}) (uint64, error) {
	return redis.Uint64(r.conn.Do("RPUSHX", r.Base().Name(), arg))
}

func (r *redisList) Set(index int64, value interface{}) error {
	_, err := r.conn.Do("LSET", r.Base().Name(), index, value)
	return err
}

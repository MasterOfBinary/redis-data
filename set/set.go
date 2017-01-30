package set

import (
	"github.com/MasterOfBinary/redistypes"
	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/garyburd/redigo/redis"
)

// Set is a Redis implementation of a set.
type Set interface {
	// Base returns the base Type.
	Base() redistypes.Type

	// Add implements the Redis command SADD. It adds one or more values to a set
	// and returns the number of values added to the set, not including the ones
	// that already existed.
	//
	// See https://redis.io/commands/sadd.
	Add(values ...interface{}) (uint64, error)
}

type redisSet struct {
	conn redis.Conn
	base redistypes.Type
}

// NewRedisSet creates a Redis implementation of Set given redigo connection conn and name. The
// Redis key used to identify the Set will be name.
func NewRedisSet(conn redis.Conn, name string) Set {
	return &redisSet{
		conn: conn,
		base: redistypes.NewRedisType(conn, name),
	}
}

func (r redisSet) Base() redistypes.Type {
	return r.base
}

func (r *redisSet) Add(values ...interface{}) (uint64, error) {
	values = internal.PrependInterface(r.Base().Name(), values...)
	return redis.Uint64(r.conn.Do("SADD", values...))
}

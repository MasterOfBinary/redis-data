// Package redistypes provides Redis data types in Go. It a very thin layer on top of https://github.com/garyburd/redigo.
package redistypes

import (
	"errors"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Type is an interface containing methods that every Redis type supports. These
// methods operate on keys in Redis with name equal to Name(), and they do not
// depend on the type of value stored in the key.
type Type interface {
	// Name returns the name of the key in Redis.
	Name() string

	// Delete implements the Redis command DEL. If a key exists in Redis, it is
	// deleted and true is returned. If it does not exist, false is returned.
	//
	// See https://redis.io/commands/del.
	Delete() (bool, error)

	// Exists implements the Redis command EXISTS. It determines if the key exists in
	// Redis. Exists returns true if it exists or false if it does not.
	//
	// See https://redis.io/commands/exists.
	Exists() (bool, error)

	// Expire implements the Redis commands EXPIRE and PEXPIRE. It sets a timeout on the
	// key, after which the key is deleted automatically. To remove the timeout, call the
	// Persist() method. For more information about timeouts, see the documentation.
	//
	// Expire uses the Redis command EXPIRE if timeout is a multiple of one second, or
	// PEXPIRE if timeout has millisecond precision. If it has more precision than
	// millisecond precision, an error is returned to avoid ambiguity in rounding.
	// Expire returns true if the key exists and the timeout was set, or false otherwise.
	//
	// See https://redis.io/commands/expire.
	Expire(timeout time.Duration) (bool, error)

	// Persist implements the Redis command PERSIST. It causes a volatile key to persist.
	//
	// See https://redis.io/commands/persist.
	Persist() (bool, error)

	// Rename renames the key to newkey, both in the Type and in Redis. If
	// newkey already exists in Redis, it is overwritten.
	//
	// See https://redis.io/commands/rename.
	Rename(newkey string) error

	// RenameNX renames the key to newkey, both in the Type and in Redis. If
	// the key was renamed successfully, true is returned. If newkey already exists
	// in Redis, false is returned.
	//
	// See https://redis.io/commands/renamenx.
	RenameNX(newkey string) (bool, error)
}

type redisType struct {
	conn redis.Conn
	name string
}

func NewRedisType(conn redis.Conn, name string) Type {
	return &redisType{
		conn: conn,
		name: name,
	}
}

func (r redisType) Name() string {
	return r.name
}

func (r *redisType) Delete() (bool, error) {
	return redis.Bool(r.conn.Do("DEL", r.name))
}

func (r *redisType) Exists() (bool, error) {
	return redis.Bool(r.conn.Do("EXISTS", r.name))
}

func (r *redisType) Expire(timeout time.Duration) (bool, error) {
	seconds := int64(timeout.Seconds())
	ms := int64(timeout.Nanoseconds() / 1000000)
	if timeout.Nanoseconds()-ms*time.Millisecond.Nanoseconds() != 0 {
		return false, errors.New("Duration is not a multiple of one second")
	}

	if seconds*1000 != ms {
		return redis.Bool(r.conn.Do("PEXPIRE", r.name, ms))
	}

	return redis.Bool(r.conn.Do("EXPIRE", r.name, seconds))
}

func (r *redisType) Persist() (bool, error) {
	return redis.Bool(r.conn.Do("PERSIST", r.name))
}

func (r *redisType) Rename(newkey string) error {
	_, err := r.conn.Do("RENAME", r.name, newkey)
	if err != nil {
		r.name = newkey
	}
	return err
}

func (r *redisType) RenameNX(newkey string) (bool, error) {
	success, err := redis.Bool(r.conn.Do("RENAMENX", r.name))
	if success {
		r.name = newkey
	}
	return success, err
}

package set_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/MasterOfBinary/redistypes/internal/test"
	"github.com/MasterOfBinary/redistypes/set"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var conn redis.Conn

func TestRedisSet_Add(t *testing.T) {
	s := set.NewRedisSet(conn, test.RandomKey())
	defer s.Base().Delete()

	t.Run("non-existing key", func(t *testing.T) {
		value, err := s.Add(1, 2)
		assert.Nil(t, err)
		assert.EqualValues(t, 2, value)
	})

	t.Run("non-existing value", func(t *testing.T) {
		_, _ = s.Base().Delete()
		_, _ = s.Add(1, 2, 3)
		value, err := s.Add(5)
		assert.Nil(t, err)
		assert.EqualValues(t, 1, value)
	})

	t.Run("existing value", func(t *testing.T) {
		_, _ = s.Base().Delete()
		_, _ = s.Add(1, 2, 3)
		value, err := s.Add(2)
		assert.Nil(t, err)
		assert.EqualValues(t, 0, value)
	})
}

func TestRedisSet_Card(t *testing.T) {
	s := set.NewRedisSet(conn, test.RandomKey())
	defer s.Base().Delete()

	t.Run("non-existing key", func(t *testing.T) {
		value, err := s.Card()
		assert.Nil(t, err)
		assert.EqualValues(t, 0, value)
	})

	t.Run("existing key", func(t *testing.T) {
		_, _ = s.Base().Delete()
		_, _ = s.Add(1, 2, 3)
		value, err := s.Card()
		assert.Nil(t, err)
		assert.EqualValues(t, 3, value)
	})
}

func TestMain(m *testing.M) {
	netConn, err := net.Dial("tcp", internal.GetHostAndPort())
	if err != nil {
		fmt.Printf("Error opening net connection, err: %v", err)
		os.Exit(1)
	}

	conn = redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	os.Exit(m.Run())
}

package redistypes_test

import (
<<<<<<< HEAD
	"testing"
	"net"
	"github.com/MasterOfBinary/redistypes/internal"
	"fmt"
	"os"
	"github.com/garyburd/redigo/redis"
	"time"
)

var conn redis.Conn
=======
	"fmt"
	"net"
	"os"
	"testing"
	"time"
>>>>>>> origin/master

	"github.com/MasterOfBinary/redistypes"
	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/MasterOfBinary/redistypes/internal/test"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var conn redis.Conn

func TestRedisType_Name(t *testing.T) {
	name := test.RandomKey()
	r := redistypes.NewRedisType(conn, name)
	assert.Equal(t, name, r.Name())
}

func TestRedisType_Delete(t *testing.T) {
	r := redistypes.NewRedisType(conn, test.RandomKey())
	defer r.Delete()

	t.Run("non-existing key", func(t *testing.T) {
		success, err := r.Delete()
		assert.Nil(t, err)
		assert.False(t, success)
	})

	t.Run("existing key", func(t *testing.T) {
		_, _ = conn.Do("SET", r.Name(), 1)
		success, err := r.Delete()
		assert.Nil(t, err)
		assert.True(t, success)

		exists, err := r.Exists()
		assert.Nil(t, err)
		assert.False(t, exists)
	})
}

func TestRedisType_Exists(t *testing.T) {
	r := redistypes.NewRedisType(conn, test.RandomKey())
	defer r.Delete()

	t.Run("non-existing key", func(t *testing.T) {
		exists, err := r.Exists()
		assert.Nil(t, err)
		assert.False(t, exists)
	})

	t.Run("existing key", func(t *testing.T) {
		_, _ = conn.Do("SET", r.Name(), 1)
		exists, err := r.Delete()
		assert.Nil(t, err)
		assert.True(t, exists)
	})
}

func TestRedisType_Expire(t *testing.T) {
	r := redistypes.NewRedisType(conn, test.RandomKey())
	defer r.Delete()

	t.Run("non-existing key", func(t *testing.T) {
		success, err := r.Expire(time.Second)
		assert.Nil(t, err)
		assert.False(t, success)
	})

	t.Run("timeout tests", func(t *testing.T) {
		scenarios := []struct {
			name     string
			duration time.Duration
			wantErr  bool
		}{
			{
				name:     "1s",
				duration: time.Second,
				wantErr:  false,
			},
			{
				name:     "10s",
				duration: 10 * time.Second,
				wantErr:  false,
			},
			{
				name:     "1s5ms",
				duration: 1*time.Second + 5*time.Millisecond,
				wantErr:  true,
			},
			{
				name:     "5ns",
				duration: 5 * time.Nanosecond,
				wantErr:  true,
			},
			{
				name:     "1s950ms",
				duration: 1*time.Second + 950*time.Millisecond,
				wantErr:  true,
			},
			{
				name:     "1s950ns",
				duration: 1*time.Second + 950*time.Nanosecond,
				wantErr:  true,
			},
		}

		for _, scenario := range scenarios {
			t.Run(scenario.name, func(t *testing.T) {
				_, _ = conn.Do("SET", r.Name(), 1)

				success, err := r.Expire(scenario.duration)
				if scenario.wantErr {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
					assert.True(t, success)
				}
			})
		}
	})

	t.Run("expiring test", func(t *testing.T) {
		_, _ = conn.Do("SET", r.Name(), 1)
		success, err := r.Expire(time.Second)
		assert.Nil(t, err)
		assert.True(t, success)

		value, err := conn.Do("GET", r.Name())
		assert.Nil(t, err)
		test.AssertEqual(t, 1, value)

		time.Sleep(time.Millisecond * 200)

		value, err = conn.Do("GET", r.Name())
		assert.Nil(t, err)
		test.AssertEqual(t, 1, value)

		time.Sleep(time.Second)

		value, err = conn.Do("GET", r.Name())
		assert.Nil(t, err)
		assert.Nil(t, value)
	})
}

func TestRedisType_PExpire(t *testing.T) {
	r := redistypes.NewRedisType(conn, test.RandomKey())
	defer r.Delete()

	t.Run("non-existing key", func(t *testing.T) {
		success, err := r.PExpire(time.Second)
		assert.Nil(t, err)
		assert.False(t, success)
	})

	t.Run("timeout tests", func(t *testing.T) {
		scenarios := []struct {
			name     string
			duration time.Duration
			wantErr  bool
		}{
			{
				name:     "1s",
				duration: time.Second,
				wantErr:  false,
			},
			{
				name:     "10s",
				duration: 10 * time.Second,
				wantErr:  false,
			},
			{
				name:     "1s5ms",
				duration: 1*time.Second + 5*time.Millisecond,
				wantErr:  false,
			},
			{
				name:     "5ns",
				duration: 5 * time.Nanosecond,
				wantErr:  true,
			},
			{
				name:     "1s950ms",
				duration: 1*time.Second + 950*time.Millisecond,
				wantErr:  false,
			},
			{
				name:     "1s950ns",
				duration: 1*time.Second + 950*time.Nanosecond,
				wantErr:  true,
			},
		}

		for _, scenario := range scenarios {
			t.Run(scenario.name, func(t *testing.T) {
				_, _ = conn.Do("SET", r.Name(), 1)

				success, err := r.PExpire(scenario.duration)
				if scenario.wantErr {
					assert.NotNil(t, err)
				} else {
					assert.Nil(t, err)
					assert.True(t, success)
				}
			})
		}
	})

	t.Run("expiring test", func(t *testing.T) {
		_, _ = conn.Do("SET", r.Name(), 1)
		success, err := r.PExpire(500 * time.Millisecond)
		assert.Nil(t, err)
		assert.True(t, success)

		value, err := conn.Do("GET", r.Name())
		assert.Nil(t, err)
		test.AssertEqual(t, 1, value)

		time.Sleep(200 * time.Millisecond)

		value, err = conn.Do("GET", r.Name())
		assert.Nil(t, err)
		test.AssertEqual(t, 1, value)

		time.Sleep(400 * time.Millisecond)

		value, err = conn.Do("GET", r.Name())
		assert.Nil(t, err)
		assert.Nil(t, value)
	})
}

func TestRedisType_Persist(t *testing.T) {
	r := redistypes.NewRedisType(conn, test.RandomKey())
	defer r.Delete()

	t.Run("non-existing key", func(t *testing.T) {
		exists, err := r.Exists()
		assert.Nil(t, err)
		assert.False(t, exists)
	})

	t.Run("expiring test", func(t *testing.T) {
		_, _ = conn.Do("SET", r.Name(), 1)
		success, err := r.Expire(time.Second)
		assert.Nil(t, err)
		assert.True(t, success)

		value, err := conn.Do("GET", r.Name())
		assert.Nil(t, err)
		test.AssertEqual(t, 1, value)

		time.Sleep(time.Millisecond * 200)

		value, err = conn.Do("GET", r.Name())
		assert.Nil(t, err)
		test.AssertEqual(t, 1, value)

		success, err = r.Persist()
		assert.Nil(t, err)
		assert.True(t, success)

		time.Sleep(time.Second)

		value, err = conn.Do("GET", r.Name())
		assert.Nil(t, err)
		test.AssertEqual(t, 1, value)
	})
}

func TestRedisType_Rename(t *testing.T) {
	t.Run("non-existing key", func(t *testing.T) {
		oldname := test.RandomKey()
		newname := test.RandomKey()

		r := redistypes.NewRedisType(conn, oldname)
		defer r.Delete()

		err := r.Rename(newname)
		assert.NotNil(t, err)
	})

	t.Run("non-existing new key", func(t *testing.T) {
		oldname := test.RandomKey()
		newname := test.RandomKey()

		r := redistypes.NewRedisType(conn, oldname)
		defer r.Delete()

		_, _ = conn.Do("SET", oldname, 1)

		err := r.Rename(newname)
		assert.Nil(t, err)

		value, err := conn.Do("GET", oldname)
		assert.Nil(t, err)
		assert.Nil(t, value)

		value, _ = conn.Do("GET", newname)
		test.AssertEqual(t, 1, value)
	})

	t.Run("existing new key", func(t *testing.T) {
		oldname := test.RandomKey()
		newname := test.RandomKey()

		r := redistypes.NewRedisType(conn, oldname)
		defer r.Delete()

		_, _ = conn.Do("SET", oldname, 1)
		_, _ = conn.Do("SET", newname, 2)

		err := r.Rename(newname)
		assert.Nil(t, err)

		value, err := conn.Do("GET", oldname)
		assert.Nil(t, err)
		assert.Nil(t, value)

		value, _ = conn.Do("GET", newname)
		test.AssertEqual(t, 1, value)
	})
}

func TestRedisType_RenameNX(t *testing.T) {
	t.Run("non-existing key", func(t *testing.T) {
		oldname := test.RandomKey()
		newname := test.RandomKey()

		r := redistypes.NewRedisType(conn, oldname)
		defer r.Delete()

		_, err := r.RenameNX(newname)
		assert.NotNil(t, err)
	})

	t.Run("non-existing new key", func(t *testing.T) {
		oldname := test.RandomKey()
		newname := test.RandomKey()

		r := redistypes.NewRedisType(conn, oldname)
		defer r.Delete()

		_, _ = conn.Do("SET", oldname, 1)

		_, err := r.RenameNX(newname)
		assert.Nil(t, err)

		value, err := conn.Do("GET", oldname)
		assert.Nil(t, err)
		assert.Nil(t, value)

		value, _ = conn.Do("GET", newname)
		test.AssertEqual(t, 1, value)
	})

	t.Run("existing new key", func(t *testing.T) {
		oldname := test.RandomKey()
		newname := test.RandomKey()

		r := redistypes.NewRedisType(conn, oldname)
		defer r.Delete()

		_, _ = conn.Do("SET", oldname, 1)
		_, _ = conn.Do("SET", newname, 2)

		success, err := r.RenameNX(newname)
		assert.Nil(t, err)
		assert.False(t, success)

		value, _ := conn.Do("GET", oldname)
		test.AssertEqual(t, 1, value)

		value, _ = conn.Do("GET", newname)
		test.AssertEqual(t, 2, value)
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

func TestMain(m *testing.M) {
	netConn, err := net.Dial("tcp", internal.GetHostAndPort())
	if err != nil {
		fmt.Printf("Error opening net onnection, err: %v", err)
		os.Exit(1)
	}

	conn = redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	os.Exit(m.Run())
}

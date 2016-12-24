package hyperloglog_test

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/MasterOfBinary/redistypes/hyperloglog"
	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func ExampleNewRedisHyperLogLog() {
	netConn, errDial := net.Dial("tcp", internal.GetHostAndPort())
	if errDial != nil {
		fmt.Printf("Unable to dial, err: %v", errDial)
		return
	}

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	hll := hyperloglog.NewRedisHyperLogLog(conn, internal.RandomKey())

	count, errCount := hll.Count()
	if errCount != nil {
		fmt.Printf("Unable to get count, err: %v", errCount)
		return
	}
	fmt.Println("Count:", count)
	// Output: Count: 0
}

func TestRedisHyperLogLog_Add(t *testing.T) {
	netConn, err := net.Dial("tcp", internal.GetHostAndPort())
	assert.Nil(t, err)

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	hll := hyperloglog.NewRedisHyperLogLog(conn, internal.RandomKey())
	count, err := hll.Count()
	assert.Nil(t, err)
	assert.Zero(t, count)

	// Add several unique items
	hll.Add("abc", "def", "ghi")
	count, err = hll.Count()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, count)

	// Add an item that was already added
	hll.Add("abc", "abc")
	count, err = hll.Count()
	assert.Nil(t, err)
	assert.EqualValues(t, 3, count)

}

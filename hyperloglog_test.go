package redistypes_test

import (
	"fmt"
	"net"
	"time"

	"github.com/MasterOfBinary/redistypes"
	"github.com/garyburd/redigo/redis"
)

func ExampleNewRedisHyperLogLog() {
	netConn, _ := net.Dial("tcp", "127.0.0.1:6379")

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	hll := redistypes.NewRedisHyperLogLog(conn, "hll")

	fmt.Println(hll.Name())
	// Output: hll
}

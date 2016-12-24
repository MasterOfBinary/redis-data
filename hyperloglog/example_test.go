package hyperloglog_test

import (
	"fmt"
	"net"
	"time"

	"github.com/MasterOfBinary/redistypes/hyperloglog"
	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/MasterOfBinary/redistypes/internal/test"
	"github.com/garyburd/redigo/redis"
)

func Example() {
	netConn, errDial := net.Dial("tcp", internal.GetHostAndPort())
	if errDial != nil {
		fmt.Printf("Unable to dial, err: %v", errDial)
		return
	}

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	hll := hyperloglog.NewRedisHyperLogLog(conn, test.RandomKey())

	count, errCount := hll.Count()
	if errCount != nil {
		fmt.Printf("Unable to count items in hll, err: %v", errCount)
		return
	}

	fmt.Println("Count is:", count)
	// Output: Count is: 0
}

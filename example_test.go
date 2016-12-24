package redistypes_test

import (
	"fmt"
	"net"
	"time"

	"github.com/MasterOfBinary/redistypes"
	"github.com/garyburd/redigo/redis"
)

func Example() {
	netConn, errDial := net.Dial("tcp", "127.0.0.1:6379")
	if errDial != nil {
		fmt.Errorf("Unable to dial, err: %v", errDial)
		return
	}

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	hll := redistypes.NewRedisHyperLogLog(conn, "hll")

	count, _ := hll.Count()

	fmt.Println("Count is:", count)
	// Output: Count is: 0
}

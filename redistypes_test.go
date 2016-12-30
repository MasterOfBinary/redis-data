package redistypes

import (
	"testing"
	"net"
	"github.com/MasterOfBinary/redistypes/internal"
	"fmt"
	"os"
	"github.com/garyburd/redigo/redis"
	"time"
)

var conn redis.Conn

func TestRedisTypes(t *testing.T) {
	t.Log("Testing")
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

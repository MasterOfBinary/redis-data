package list_test

import (
	"net"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/rafaeljusto/redigomock"
)

func getTestConn() (redis.Conn, error) {
	var conn redis.Conn
	if testing.Short() {
		conn = redigomock.NewConn()
	} else {
		netConn, err := net.Dial("tcp", "127.0.0.1:6379")
		if err != nil {
			return nil, err
		}

		conn = redis.NewConn(netConn, 10*time.Second, 10*time.Second)
	}

	return conn, nil
}

func TestRedisList_RPush(t *testing.T) {
	_, err := getTestConn()
	if err != nil {
		t.Error("Unable to get connection, err:", err)
	}

}

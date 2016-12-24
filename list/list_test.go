package list_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/MasterOfBinary/redistypes/internal/test"
	"github.com/MasterOfBinary/redistypes/list"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var conn redis.Conn

func ExampleNewRedisList() {
	netConn, errDial := net.Dial("tcp", internal.GetHostAndPort())
	if errDial != nil {
		fmt.Printf("Unable to dial, err: %v", errDial)
		return
	}

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	l := list.NewRedisList(conn, test.RandomKey())

	items, errRange := l.LRange(0, -1)
	if errRange != nil {
		fmt.Printf("Unable to get range, err: %v", errRange)
		return
	}
	fmt.Println("Count:", len(items))
	// Output: Count: 0
}

func TestRedisList_RPush(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	scenarios := []struct {
		name    string
		add     []interface{}
		count   int
		wantErr bool
	}{
		{
			name:  "add several items",
			add:   test.StringsToInterfaceSlice("abc", "def", "ghi"),
			count: 3,
		},
		{
			name:  "add integers",
			add:   test.IntsToInterfaceSlice(1, 2),
			count: 5,
		},
		{
			name:  "add a string",
			add:   test.StringsToInterfaceSlice("abc"),
			count: 6,
		},
		{
			name:    "no items to add",
			add:     make([]interface{}, 0),
			count:   6,
			wantErr: true,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, err := l.RPush(scenario.add...)
			if scenario.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				assert.EqualValues(t, scenario.count, count)
			}
		})
	}
}

func TestRedisList_RPushX(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	count, err := l.RPushX("abc")
	assert.Nil(t, err)
	assert.EqualValues(t, 0, count)

	_, err = l.RPush("abc")
	assert.Nil(t, err)

	_, err = l.RPushX("abc")
	assert.Nil(t, err)
}

func TestMain(m *testing.M) {
	netConn, err := net.Dial("tcp", internal.GetHostAndPort())
	if err != nil {
		fmt.Errorf("Error opening net onnection, err: %v", err)
	}

	conn = redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	os.Exit(m.Run())
}

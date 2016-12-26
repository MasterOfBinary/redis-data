package hyperloglog_test

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"

	"github.com/MasterOfBinary/redistypes/hyperloglog"
	"github.com/MasterOfBinary/redistypes/internal"
	"github.com/MasterOfBinary/redistypes/internal/test"
	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

var conn redis.Conn

func ExampleNewRedisHyperLogLog() {
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
		fmt.Printf("Unable to get count, err: %v", errCount)
		return
	}
	fmt.Println("Count:", count)
	// Output: Count: 0
}

func TestRedisHyperLogLog_Add(t *testing.T) {
	hll := hyperloglog.NewRedisHyperLogLog(conn, test.RandomKey())
	defer test.DeleteKey(hll.Base().Name(), conn)

	scenarios := []struct {
		name     string
		add      []interface{}
		modified bool
	}{
		{
			name:     "add several unique items",
			add:      test.StringsToInterfaceSlice("abc", "def", "ghi"),
			modified: true,
		},
		{
			name:     "add an item that was already added",
			add:      test.StringsToInterfaceSlice("abc", "abc"),
			modified: false,
		},
		{
			name:     "add a new item and an existing item",
			add:      test.StringsToInterfaceSlice("abc", "jkl"),
			modified: true,
		},
		{
			name:     "no items to add",
			add:      make([]interface{}, 0),
			modified: false,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			modified, err := hll.Add(scenario.add...)
			assert.Nil(t, err)
			assert.Equal(t, scenario.modified, modified)
		})
	}
}

func TestRedisHyperLogLog_Count(t *testing.T) {
	hll := hyperloglog.NewRedisHyperLogLog(conn, test.RandomKey())
	defer test.DeleteKey(hll.Base().Name(), conn)

	scenarios := []struct {
		name  string
		add   []string
		added int
	}{
		{
			name:  "add several unique items",
			add:   []string{"abc", "def", "ghi"},
			added: 3,
		},
		{
			name:  "add an item that was already added",
			add:   []string{"abc", "abc"},
			added: 0,
		},
		{
			name:  "add a new item and an existing item",
			add:   []string{"abc", "jkl"},
			added: 1,
		},
		{
			name:  "no items to add",
			add:   []string{},
			added: 0,
		},
	}

	want := 0

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			args := test.StringsToInterfaceSlice(scenario.add...)
			_, err := hll.Add(args...)
			assert.Nil(t, err)
			want += scenario.added
			count, err := hll.Count()
			assert.Nil(t, err)
			assert.EqualValues(t, want, count)
		})
	}
}

func TestRedisHyperLogLog_Merge(t *testing.T) {
	scenarios := []struct {
		name  string
		add1  []string
		add2  []string
		count int
	}{
		{
			name:  "no duplicates",
			add1:  []string{"abc", "def", "ghi"},
			add2:  []string{"hij", "jkl"},
			count: 5,
		},
		{
			name:  "all duplicates",
			add1:  []string{"abc", "def"},
			add2:  []string{"abc", "def"},
			count: 2,
		},
		{
			name:  "one duplicate",
			add1:  []string{"abc", "jkl"},
			add2:  []string{"hij", "jkl"},
			count: 3,
		},
		{
			name:  "no items",
			add1:  []string{},
			add2:  []string{},
			count: 0,
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			args1 := test.StringsToInterfaceSlice(scenario.add1...)
			args2 := test.StringsToInterfaceSlice(scenario.add2...)

			hll1 := hyperloglog.NewRedisHyperLogLog(conn, test.RandomKey())
			defer test.DeleteKey(hll1.Base().Name(), conn)
			hll2 := hyperloglog.NewRedisHyperLogLog(conn, test.RandomKey())
			defer test.DeleteKey(hll2.Base().Name(), conn)

			_, err := hll1.Add(args1...)
			assert.Nil(t, err)
			_, err = hll2.Add(args2...)
			assert.Nil(t, err)

			merged, err := hll1.Merge(test.RandomKey(), hll2)
			assert.Nil(t, err)

			count, err := merged.Count()
			assert.Nil(t, err)
			assert.EqualValues(t, scenario.count, count)
		})
	}
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

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

const (
	forwardSlice = false
	reverseSlice = true
)

type scenarioStruct struct {
	name    string
	add     []interface{}
	wantErr bool
}

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

func verifySlice(t *testing.T, l list.List, wantCount int, scenarios []scenarioStruct, reverse bool) {
	got, err := l.LRange(0, -1)
	assert.Nil(t, err)
	assert.EqualValues(t, wantCount, len(got))
	assert.Len(t, got, wantCount)
	i := 0
	for _, scenario := range scenarios {
		if !scenario.wantErr {
			for _, item := range scenario.add {
				index := i
				if reverse == reverseSlice {
					index = len(got) - i - 1
				}
				test.AssertEqual(t, item, got[index])
				i++
			}
		}
	}
}

func TestRedisList_RPush(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	scenarios := []scenarioStruct{
		{
			name: "non-existing key",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name: "existing key",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name: "add integers",
			add:  test.IntsToInterfaceSlice(1, 2),
		},
		{
			name: "add a string",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name:    "no items",
			add:     nil,
			wantErr: true,
		},
	}

	wantCount := 0

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, err := l.RPush(scenario.add...)
			if scenario.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				wantCount += len(scenario.add)
				assert.EqualValues(t, wantCount, count)
			}
		})
	}

	t.Run("verify list data", func(t *testing.T) {
		verifySlice(t, l, wantCount, scenarios, forwardSlice)
	})
}

func TestRedisList_RPushX(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	t.Run("non-existing key", func(t *testing.T) {
		count, err := l.RPushX("abc")
		assert.Nil(t, err)
		assert.EqualValues(t, 0, count)
	})

	_, _ = l.RPush("abc")

	scenarios := []scenarioStruct{
		{
			name: "existing key",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name: "add integer",
			add:  test.IntsToInterfaceSlice(1),
		},
	}

	wantCount := 1

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, err := l.RPushX(scenario.add[0])
			if scenario.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				wantCount += len(scenario.add)
				assert.EqualValues(t, wantCount, count)
			}
		})
	}

	// Pop the one added by RPush
	_, _ = l.LPop()
	wantCount--

	t.Run("verify list data", func(t *testing.T) {
		verifySlice(t, l, wantCount, scenarios, forwardSlice)
	})
}

func TestRedisList_LPush(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	scenarios := []scenarioStruct{
		{
			name: "non-existing key",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name: "existing key",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name: "add integers",
			add:  test.IntsToInterfaceSlice(1, 2),
		},
		{
			name: "add a string",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name:    "no items",
			add:     nil,
			wantErr: true,
		},
	}

	wantCount := 0

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, err := l.LPush(scenario.add...)
			if scenario.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				wantCount += len(scenario.add)
				assert.EqualValues(t, wantCount, count)
			}
		})
	}

	t.Run("verify list data", func(t *testing.T) {
		verifySlice(t, l, wantCount, scenarios, reverseSlice)
	})
}

func TestRedisList_LPushX(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	t.Run("non-existing key", func(t *testing.T) {
		count, err := l.LPushX("abc")
		assert.Nil(t, err)
		assert.EqualValues(t, 0, count)
	})

	_, _ = l.RPush("abc")

	scenarios := []scenarioStruct{
		{
			name: "existing key",
			add:  test.StringsToInterfaceSlice("abc"),
		},
		{
			name: "add integer",
			add:  test.IntsToInterfaceSlice(1),
		},
	}

	wantCount := 1

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			count, err := l.LPushX(scenario.add[0])
			if scenario.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				wantCount += len(scenario.add)
				assert.EqualValues(t, wantCount, count)
			}
		})
	}

	// Pop the one added by RPush
	_, _ = l.RPop()
	wantCount--

	t.Run("verify list data", func(t *testing.T) {
		verifySlice(t, l, wantCount, scenarios, reverseSlice)
	})
}

func TestMain(m *testing.M) {
	netConn, err := net.Dial("tcp", internal.GetHostAndPort())
	if err != nil {
		fmt.Printf("Error opening net connection, err: %v", err)
	}

	conn = redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	os.Exit(m.Run())
}

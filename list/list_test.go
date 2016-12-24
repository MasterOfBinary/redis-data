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
	netConn, _ := net.Dial("tcp", internal.GetHostAndPort())

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	l := list.NewRedisList(conn, test.RandomKey())

	values, _ := l.Range(0, -1)
	fmt.Println("Count:", len(values))
	// Output: Count: 0
}

func ExampleRedisList_Range() {
	netConn, _ := net.Dial("tcp", internal.GetHostAndPort())

	conn := redis.NewConn(netConn, time.Second, time.Second)
	defer conn.Close()

	l := list.NewRedisList(conn, test.RandomKey())

	_, _ = l.RightPush("hello", "world", "how", "are", "you", "today")

	// Get the entire range
	values, _ := redis.Strings(l.Range(0, -1))
	fmt.Println(values)

	// Starting from the middle
	values, _ = redis.Strings(l.Range(2, -1))
	fmt.Println(values)

	// From beginning to middle
	values, _ = redis.Strings(l.Range(0, 1))
	fmt.Println(values)

	// Output:
	// [hello world how are you today]
	// [how are you today]
	// [hello world]
}

func verifySlice(t *testing.T, l list.List, wantCount int, scenarios []scenarioStruct, reverse bool) {
	got, err := l.Range(0, -1)
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

func TestRedisHyperLogLog_Name(t *testing.T) {
	name := test.RandomKey()
	l := list.NewRedisList(conn, name)
	assert.Equal(t, name, l.Name())
}

func TestRedisList_LeftPop(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	t.Run("non-existing key", func(t *testing.T) {
		item, err := l.LeftPop()
		assert.Nil(t, err)
		assert.Nil(t, item)
	})

	t.Run("list with one item", func(t *testing.T) {
		_, _ = l.LeftPush("abc")
		item, err := redis.String(l.LeftPop())
		assert.Nil(t, err)
		assert.EqualValues(t, "abc", item)
	})

	t.Run("list with multiple items", func(t *testing.T) {
		_, _ = l.LeftPush("def", "ghi")
		item, err := redis.String(l.LeftPop())
		assert.Nil(t, err)
		assert.EqualValues(t, "ghi", item)
	})
}

func TestRedisList_LeftPush(t *testing.T) {
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
			count, err := l.LeftPush(scenario.add...)
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

func TestRedisList_LeftPushX(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	t.Run("non-existing key", func(t *testing.T) {
		count, err := l.LeftPushX("abc")
		assert.Nil(t, err)
		assert.EqualValues(t, 0, count)
	})

	_, _ = l.RightPush("abc")

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
			count, err := l.LeftPushX(scenario.add[0])
			if scenario.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				wantCount += len(scenario.add)
				assert.EqualValues(t, wantCount, count)
			}
		})
	}

	// Pop the one added by RightPush
	_, _ = l.RightPop()
	wantCount--

	t.Run("verify list data", func(t *testing.T) {
		verifySlice(t, l, wantCount, scenarios, reverseSlice)
	})
}

func TestRedisList_Range(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	t.Run("non-existing key", func(t *testing.T) {
		items, err := l.Range(0, -1)
		assert.Nil(t, err)
		assert.Empty(t, items)
	})

	added := test.StringsToInterfaceSlice("abc", "def", "ghi")
	added = append(added, 5)
	added = append(added, "jkl")
	added = append(added, 10)

	_, _ = l.RightPush(added...)

	t.Run("full range", func(t *testing.T) {
		items, err := l.Range(0, -1)
		assert.Nil(t, err)
		assert.Len(t, items, len(added))
		for i, item := range items {
			test.AssertEqual(t, added[i], item)
		}
	})

	t.Run("from start to middle", func(t *testing.T) {
		items, err := l.Range(0, 2)
		assert.Nil(t, err)
		assert.Len(t, items, 3)
		for i, item := range items {
			test.AssertEqual(t, added[i], item)
		}
	})

	t.Run("from middle to end", func(t *testing.T) {
		items, err := l.Range(3, -1)
		assert.Nil(t, err)
		assert.Len(t, items, 3)
		for i, item := range items {
			test.AssertEqual(t, added[i+3], item)
		}
	})
}

func TestRedisList_RightPop(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	t.Run("non-existing key", func(t *testing.T) {
		item, err := l.RightPop()
		assert.Nil(t, err)
		assert.Nil(t, item)
	})

	t.Run("list with one item", func(t *testing.T) {
		_, _ = l.RightPush("abc")
		item, err := redis.String(l.RightPop())
		assert.Nil(t, err)
		assert.EqualValues(t, "abc", item)
	})

	t.Run("list with multiple items", func(t *testing.T) {
		_, _ = l.RightPush("def", "ghi")
		item, err := redis.String(l.RightPop())
		assert.Nil(t, err)
		assert.EqualValues(t, "ghi", item)
	})
}

func TestRedisList_RightPush(t *testing.T) {
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
			count, err := l.RightPush(scenario.add...)
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

func TestRedisList_RightPushX(t *testing.T) {
	l := list.NewRedisList(conn, test.RandomKey())

	t.Run("non-existing key", func(t *testing.T) {
		count, err := l.RightPushX("abc")
		assert.Nil(t, err)
		assert.EqualValues(t, 0, count)
	})

	_, _ = l.RightPush("abc")

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
			count, err := l.RightPushX(scenario.add[0])
			if scenario.wantErr {
				assert.NotNil(t, err)
			} else {
				assert.Nil(t, err)
				wantCount += len(scenario.add)
				assert.EqualValues(t, wantCount, count)
			}
		})
	}

	// Pop the one added by RightPush
	_, _ = l.LeftPop()
	wantCount--

	t.Run("verify list data", func(t *testing.T) {
		verifySlice(t, l, wantCount, scenarios, forwardSlice)
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

// Package test contains functions used for testing in redistypes.
package test

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// StringsToInterfaceSlice converts strings to a slice of interfaces containing the strings.
func StringsToInterfaceSlice(strings ...string) []interface{} {
	args := make([]interface{}, len(strings))
	for i, str := range strings {
		args[i] = str
	}
	return args
}

// IntsToInterfaceSlice converts ints to a slice of interfaces containing the ints.
func IntsToInterfaceSlice(ints ...int) []interface{} {
	args := make([]interface{}, len(ints))
	for i, num := range ints {
		args[i] = num
	}
	return args
}

// AssertEqual checks if got, returned from redis, is equal to want, a string or an int. If not equal
// it will cause the test to fail.
func AssertEqual(t *testing.T, want interface{}, got interface{}) {
	switch want.(type) {
	case int:
		gotInt, err := redis.Int(got, nil)
		assert.Nil(t, err)
		assert.EqualValues(t, want, gotInt)
	default:
		gotStr, err := redis.String(got, nil)
		assert.Nil(t, err)
		assert.EqualValues(t, want, gotStr)
	}
}

// RandomKey returns a key of the form test:<number>, where <number> is a random number. It is used for
// testing Redis data types using random keys.
func RandomKey() string {
	return fmt.Sprint("testkey" + strconv.Itoa(rand.Int()))
}

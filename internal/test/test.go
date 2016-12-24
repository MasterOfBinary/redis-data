package test

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

func init() {
	rand.Seed(time.Now().Unix())
}

// StringSliceToInterfaceSlice converts strings to a slice of interfaces containing the strings.
func StringSliceToInterfaceSlice(strings []string) []interface{} {
	args := make([]interface{}, len(strings))
	for i, str := range strings {
		args[i] = str
	}
	return args
}

// RandomKey returns a key of the form test:<number>, where <number> is a random number. It is used for
// testing Redis data types using random keys.
func RandomKey() string {
	return fmt.Sprint("test:" + strconv.Itoa(rand.Int()))
}

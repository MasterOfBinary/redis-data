// Package internal contains internal functions used by redistypes.
package internal

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const defaultHostAndPort = "127.0.0.1:6379"

func init() {
	rand.Seed(time.Now().Unix())
}

// PrependInterface prepends item to args and returns the new interface slice. It does not modify args.
func PrependInterface(item interface{}, args ...interface{}) []interface{} {
	newArgs := make([]interface{}, len(args)+1)
	newArgs[0] = item
	for i, arg := range args {
		newArgs[i+1] = arg
	}
	return newArgs
}

// RandomKey returns a key of the form test:<number>, where <number> is a random number. It is used for
// testing Redis data types using random keys.
func RandomKey() string {
	return fmt.Sprint("test:" + strconv.Itoa(rand.Int()))
}

// GetHostAndPort returns the host and port specified by the operating system's environment variable
// REDIS_HOST_PORT. If no such variable exists, it returns localhost and the default Redis port.
func GetHostAndPort() string {
	hostPort := os.Getenv("REDIS_HOST_PORT")
	if hostPort == "" {
		return defaultHostAndPort
	}
	return hostPort
}

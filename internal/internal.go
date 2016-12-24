// Package internal contains internal functions used by redistypes.
package internal

import "os"

const defaultHostAndPort = "127.0.0.1:6379"

// PrependInterface prepends item to args and returns the new interface slice. It does not modify args.
func PrependInterface(item interface{}, args ...interface{}) []interface{} {
	newArgs := make([]interface{}, len(args)+1)
	newArgs[0] = item
	for i, arg := range args {
		newArgs[i+1] = arg
	}
	return newArgs
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

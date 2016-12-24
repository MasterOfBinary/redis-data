// Package redistypes provides Redis data types in Go. It is built on top of https://github.com/garyburd/redigo.
package redistypes

func prependInterface(item interface{}, args ...interface{}) []interface{} {
	newArgs := make([]interface{}, len(args)+1)
	newArgs[0] = item
	for i, arg := range args {
		newArgs[i+1] = arg
	}
	return newArgs
}

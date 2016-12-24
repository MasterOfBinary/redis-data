// Package internal contains internal functions used by redistypes.
package internal

// PrependInterface prepends item to args and returns the new interface slice. It does not modify args.
func PrependInterface(item interface{}, args ...interface{}) []interface{} {
	newArgs := make([]interface{}, len(args)+1)
	newArgs[0] = item
	for i, arg := range args {
		newArgs[i+1] = arg
	}
	return newArgs
}

// Package broker is the TCP server: listener with a connection cap, one
// goroutine per connection, request dispatch, cap enforcement at the edge,
// and graceful drain. It is the only package that coordinates wire with
// storage.
package broker

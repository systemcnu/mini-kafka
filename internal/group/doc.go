// Package group is the consumer-group coordinator (DD-10..14): membership
// and liveness on control connections, generations with immediate range
// rebalance, level-triggered REJOIN, serve-time fencing with 13-before-12
// precedence, and durable per-group commit files under data/_groups/.
// Errors are sentinels; the broker maps them onto wire codes (this package
// never imports wire).
package group

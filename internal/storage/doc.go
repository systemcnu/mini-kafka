// Package storage owns durability: per-partition append-only logs, the
// group-commit flusher, the atomically-replaced durable frontier, boot-scan
// recovery, and the topics registry. It never imports the wire package;
// broker maps its errors onto protocol codes.
package storage

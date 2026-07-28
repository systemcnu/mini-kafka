// Producer, Consumer, and Admin over one TCP connection each (DD-19
// subset), plus the re-exported error code registry (DD-1). Deadlines are
// deliberately absent at SL0: a fetch may legally park server-side up to
// its maxWait.
package client

import (
	"fmt"
	"net"

	"github.com/systemcnu/mini-kafka/internal/wire"
)

// DefaultAddr mirrors the broker's default loopback listen address (DD-24).
const DefaultAddr = "127.0.0.1:7621"

// Protocol error codes, re-exported so importers never touch internal/wire.
const (
	CodeUnknownTopic  uint16 = 1
	CodeTopicExists   uint16 = 2
	CodeBadPartition  uint16 = 3
	CodeInvalidName   uint16 = 4
	CodeMsgTooLarge   uint16 = 5
	CodeFrameTooLarge uint16 = 6
	CodeMalformed     uint16 = 7
	CodeCapExceeded   uint16 = 8
	CodeFetchTooWide  uint16 = 9
	CodeShuttingDown  uint16 = 10
	CodeWriteFailed   uint16 = 11
)

// Error is a broker-served protocol error.
type Error struct {
	Code uint16
	Msg  string
}

func (e *Error) Error() string { return fmt.Sprintf("broker error %d: %s", e.Code, e.Msg) }

// Record is one fetched record.
type Record struct {
	Offset  uint64
	Payload []byte
}

// TopicInfo is one row of a topic listing.
type TopicInfo struct {
	Name       string
	Partitions uint32
}

// conn is the shared synchronous request/response core: one request in
// flight per connection (DD-15).
type conn struct {
	c net.Conn
}

func dial(addr string) (*conn, error) {
	c, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &conn{c: c}, nil
}

func (cn *conn) roundtrip(reqType byte, body []byte, wantType byte) ([]byte, error) {
	if err := wire.WriteFrame(cn.c, reqType, body); err != nil {
		return nil, err
	}
	respType, respBody, err := wire.ReadFrame(cn.c, wire.MaxResponseFrame)
	if err != nil {
		return nil, err
	}
	if respType == wire.TypeError {
		em, werr := wire.DecodeErrorMsg(respBody)
		if werr != nil {
			return nil, fmt.Errorf("undecodable error frame: %v", werr)
		}
		return nil, &Error{Code: em.Code, Msg: em.Msg}
	}
	if respType != wantType {
		return nil, fmt.Errorf("unexpected response type %d, want %d", respType, wantType)
	}
	return respBody, nil
}

func (cn *conn) close() error { return cn.c.Close() }

// Producer produces synchronously: each Produce blocks until the broker's
// durable ack and returns the assigned offset.
type Producer struct {
	cn *conn
}

// DialProducer connects a Producer to a broker.
func DialProducer(addr string) (*Producer, error) {
	cn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return &Producer{cn: cn}, nil
}

// Produce appends payload to (topic, partition) and returns its offset.
func (p *Producer) Produce(topic string, partition uint32, payload []byte) (uint64, error) {
	body, err := p.cn.roundtrip(wire.TypeProduce,
		wire.Produce{Topic: topic, Partition: partition, Payload: payload}.Encode(),
		wire.TypeProduceResp)
	if err != nil {
		return 0, err
	}
	resp, werr := wire.DecodeProduceResp(body)
	if werr != nil {
		return 0, fmt.Errorf("bad produce response: %v", werr)
	}
	return resp.Offset, nil
}

// Close closes the producer's connection.
func (p *Producer) Close() error { return p.cn.close() }

// Consumer is a raw single-partition fetch loop over one connection.
type Consumer struct {
	cn *conn
}

// DialConsumer connects a Consumer to a broker.
func DialConsumer(addr string) (*Consumer, error) {
	cn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return &Consumer{cn: cn}, nil
}

// Fetch returns records from (topic, partition) starting at offset. It
// blocks up to the broker-side maxWait (0 → broker default 5 s) when no
// durable data is available; an empty result is the at-tail shape.
// maxBytes 0 takes the broker default (1 MiB).
func (c *Consumer) Fetch(topic string, partition uint32, offset uint64, maxWaitMs, maxBytes uint32) ([]Record, error) {
	req := wire.Fetch{
		Topic:     topic,
		Entries:   []wire.FetchEntry{{Partition: partition, Offset: offset}},
		MaxWaitMs: maxWaitMs,
		MaxBytes:  maxBytes,
	}
	body, err := c.cn.roundtrip(wire.TypeFetch, req.Encode(), wire.TypeFetchResp)
	if err != nil {
		return nil, err
	}
	resp, werr := wire.DecodeFetchResp(body)
	if werr != nil {
		return nil, fmt.Errorf("bad fetch response: %v", werr)
	}
	if len(resp.Groups) != 1 || resp.Groups[0].Partition != partition {
		return nil, fmt.Errorf("fetch response has %d groups", len(resp.Groups))
	}
	recs := make([]Record, 0, len(resp.Groups[0].Recs))
	for _, r := range resp.Groups[0].Recs {
		recs = append(recs, Record{Offset: r.Offset, Payload: r.Payload})
	}
	return recs, nil
}

// Close closes the consumer's connection.
func (c *Consumer) Close() error { return c.cn.close() }

// Admin covers topic administration: create and list.
type Admin struct {
	cn *conn
}

// DialAdmin connects an Admin to a broker.
func DialAdmin(addr string) (*Admin, error) {
	cn, err := dial(addr)
	if err != nil {
		return nil, err
	}
	return &Admin{cn: cn}, nil
}

// CreateTopic creates topic with the given partition count (1..16).
func (a *Admin) CreateTopic(topic string, partitions uint32) error {
	_, err := a.cn.roundtrip(wire.TypeCreateTopic,
		wire.CreateTopic{Topic: topic, Partitions: partitions}.Encode(),
		wire.TypeCreateTopicResp)
	return err
}

// Topics lists live topics, sorted by name.
func (a *Admin) Topics() ([]TopicInfo, error) {
	body, err := a.cn.roundtrip(wire.TypeListTopics, nil, wire.TypeListTopicsResp)
	if err != nil {
		return nil, err
	}
	resp, werr := wire.DecodeListTopicsResp(body)
	if werr != nil {
		return nil, fmt.Errorf("bad list response: %v", werr)
	}
	out := make([]TopicInfo, 0, len(resp.Topics))
	for _, t := range resp.Topics {
		out = append(out, TopicInfo{Name: t.Name, Partitions: t.Partitions})
	}
	return out, nil
}

// Close closes the admin's connection.
func (a *Admin) Close() error { return a.cn.close() }

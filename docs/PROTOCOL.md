# mini-kafka wire protocol (version 1)

This document is the complete wire contract of the mini-kafka broker, written
so that a stranger can implement a client from it alone — no reading of the
broker source required. It documents the AS-BUILT protocol: every behavioral
claim below is written against a named source location, and the two registry
tables (§4 message types, §6 error codes) are machine-diffed against
`internal/wire`'s own source by `internal/wire/protocoldoc_test.go` on every
test run, in both directions — a type or code added, removed, renamed, or
renumbered without its doc row is a build failure.

Sections marked **normative** bind any client implementation; notes marked
*informative* describe what the shipped client (`client/`) happens to do and
carry no obligation.

## 1. Overview

- **Version 1 is the only protocol version.** Every frame carries a version
  byte and its only legal value is 1 (`wire.Version`, `internal/wire/frame.go`);
  any other value is rejected as MALFORMED and the connection is closed.
- **Registries are add-only and never renumbered.** The message-type values
  (§4) and error-code values (§6) are pinned; future revisions may append new
  values but existing ones never change meaning (pinned in
  `internal/wire/frame.go` and `internal/wire/errors.go`).
- **Single broker, plain TCP.** There is no cluster, no TLS, and no
  authentication (§13); a client opens an ordinary TCP connection (default
  `127.0.0.1:7621`) and starts writing frames. There is no handshake or hello
  message.
- **Big-endian throughout.** Every multi-byte integer on the wire is
  big-endian (§3).
- One request is in flight per connection at a time: write a request frame,
  then read frames until its answer arrives (§4 read loop, §7 lifecycle).

## 2. Frame envelope

Every message in either direction travels in the same envelope
(`internal/wire/frame.go`, `WriteFrame`/`ReadFrame`):

```text
byte offset   0            4          5          6                4 + len
              +------------+----------+----------+------------------+
              | u32 len    | u8 ver=1 | u8 type  | payload          |
              | big-endian |          |          | (len - 2 bytes)  |
              +------------+----------+----------+------------------+
                           ^~~~~~~~~~ len covers this ~~~~~~~~~~~~~~^
```

- `len` counts everything AFTER itself: the version byte, the type byte, and
  the payload — so `len = 2 + payload length`, and the total on-the-wire
  frame is `4 + len` bytes.
- `ver` must be 1. `type` is a registry value from §4. `payload` is the
  message body, encoded per §5 with the primitives of §3.

**Frame caps (normative).** Caps bound the TOTAL on-the-wire size including
the 4-byte length prefix, and they are asymmetric so a maximum-`maxBytes`
fetch response stays legal (`internal/wire/frame.go`):

- requests (client → broker): `wire.MaxRequestFrame` = 1 MiB + 4 KiB
  (1,052,672 bytes total);
- responses (broker → client): `wire.MaxResponseFrame` = 4 MiB + 64 KiB
  (4,259,840 bytes total). A client's frame reader must accept responses up
  to this cap.

**Envelope errors** (`ReadFrame`, `internal/wire/frame.go`):

- `len < 2` (the body cannot even hold ver + type) → Error frame,
  MALFORMED.
- Oversized frame (`4 + len` over the reader's cap) → Error frame,
  FRAME_TOO_LARGE — checked BEFORE any payload allocation, so a hostile
  length cannot drive memory use.
- `ver != 1` → Error frame, MALFORMED.
- A connection cut mid-frame (header arrived, body did not) is an I/O
  condition, not a protocol error: no Error frame exists for it, the reader
  just sees unexpected EOF.

What the broker does with the connection after each of these is §7.

## 3. Encoding primitives

Message bodies are built from exactly four primitives
(`internal/wire/frame.go`):

| primitive | wire form |
|---|---|
| `u8`, `u16`, `u32`, `u64` | unsigned big-endian integer, 1/2/4/8 bytes |
| `str` | `[u16 n][n bytes]` — length-prefixed bytes, no terminator, no required encoding |
| `blob` | `[u32 n][n bytes]` — length-prefixed bytes |

**Strict decode (normative).** The broker decodes bodies strictly, and a
conforming client should too:

- a body that ends before a declared length is satisfied → MALFORMED
  (`truncated message body`);
- a hostile length can never allocate past the frame it arrived in — every
  length is checked against the bytes actually present, which the frame cap
  already bounds;
- bytes left over after the last field of a body → MALFORMED
  (`trailing bytes after message body`). Do not append padding.

## 4. Message-type registry

The table below is the COMPLETE message set — it is diffed against the
package-wide `Type*` constants of `internal/wire` (all `.go` files, values
contiguous 1..17 plus 255), so it cannot silently gain or lose a row.

**The read loop (normative).** Every request is answered by exactly one
frame: its paired `response` type from the table, OR an Error frame
(type 255) carrying a §6 code. A client must therefore read the type byte
FIRST and branch on 255 before matching the expected response type
(`internal/broker/server.go` error path). Note the one asymmetric pairing:
GroupFetch (17) is answered by FetchResp (4) — no "GroupFetchResp" exists
(`internal/wire/messages.go`).

<!-- registry:types:begin -->
<!-- grammar: between these markers the only legal lines are blanks, this comment, ONE header row, ONE separator row, and data rows whose first cell is a base-10 integer; cells split on unescaped pipes only (a backslash-pipe is a literal pipe); row count must equal the registry cardinality -->

| type | name | direction | response | body summary |
|---|---|---|---|---|
| 1 | Produce | request | ProduceResp | [str topic][u32 partition][blob payload] |
| 2 | ProduceResp | response | — | [u64 offset] — the ack, sent only after the record is durable |
| 3 | Fetch | request | FetchResp | [str topic][u32 n]{[u32 partition][u64 offset]}[u32 maxWaitMs][u32 maxBytes] |
| 4 | FetchResp | response | — | [u32 n]{[u32 partition][u32 nRecs]{[u64 offset][blob payload]}} — zero-record groups are legal |
| 5 | CreateTopic | request | CreateTopicResp | [str topic][u32 partitions] |
| 6 | CreateTopicResp | response | — | empty body |
| 7 | ListTopics | request | ListTopicsResp | empty body — a non-empty body is MALFORMED |
| 8 | ListTopicsResp | response | — | [u32 n]{[str name][u32 partitions]} |
| 9 | JoinGroup | request | JoinGroupResp | [str group][str topic] — one topic per group |
| 10 | JoinGroupResp | response | — | [str memberID][u64 generation][u32 n]{[u32 partition][u64 nextOffset]} — join carries the resume state |
| 11 | Heartbeat | request | HeartbeatResp | [str group][str memberID][u64 generation] |
| 12 | HeartbeatResp | response | — | [u8 flags] — bit0 is the level-triggered REJOIN signal |
| 13 | CommitOffsets | request | CommitOffsetsResp | [str group][str memberID][u64 generation][u32 n]{[u32 partition][u64 next]} — next-to-read positions |
| 14 | CommitOffsetsResp | response | — | empty body — the ack means the commit is durable |
| 15 | LeaveGroup | request | LeaveGroupResp | [str group][str memberID] |
| 16 | LeaveGroupResp | response | — | empty body |
| 17 | GroupFetch | request | FetchResp | [str group][str memberID][u64 generation] plus Fetch's exact entry/maxWait/maxBytes tail — answered by FetchResp, type 4 |
| 255 | Error | response | — | [u16 code][str msg] — code from the §6 registry, msg is diagnostic text |

<!-- registry:types:end -->

## 5. Message reference

**Name rule (normative).** Every topic and group name must match
`^[a-z0-9][a-z0-9._-]{0,127}$` (`internal/wire/names.go`): it starts with a
lowercase letter or digit, uses only lowercase letters, digits, `.`, `_`,
`-`, and is at most 128 bytes. Any request naming a topic or group outside
the rule is answered with INVALID_NAME before anything else happens — the
rule is enforced in the protocol layer, so path traversal is structurally
impossible downstream.

Field tables give each body's fields in wire order. Encodings are §3
primitives; braces `{...}` mark a group repeated by the count immediately
before it. Source of truth: `internal/wire/messages.go`.

### Produce (type 1)

| field | wire form | meaning |
|---|---|---|
| topic | str | target topic, name rule applies |
| partition | u32 | target partition, `0 ≤ partition <` the topic's count |
| payload | blob | the record; at most 1 MiB (§10), rejected BEFORE anything is written |

Answered by ProduceResp only after the record is written, fsynced, and
covered by the durable frontier (§11).

### ProduceResp (type 2)

| field | wire form | meaning |
|---|---|---|
| offset | u64 | the assigned offset — a contiguous ordinal per partition, starting at 0 |

### Fetch (type 3)

| field | wire form | meaning |
|---|---|---|
| topic | str | topic to read, name rule applies |
| nEntries | u32 | number of entries following; 1..16 (§8) |
| entries | {u32 partition, u64 offset} × nEntries | each entry names a partition and the first offset wanted |
| maxWaitMs | u32 | long-poll bound in milliseconds; 0 means the 5 s default, cap 30,000 (§8) |
| maxBytes | u32 | response budget in bytes; 0 means the 1 MiB default, cap 4 MiB (§8) |

Validation order, defaults, the budget accounting formula, and the parking
rules are §8.

### FetchResp (type 4)

| field | wire form | meaning |
|---|---|---|
| nGroups | u32 | number of per-partition groups following |
| groups | {u32 partition, u32 nRecs, {u64 offset, blob payload} × nRecs} × nGroups | records per partition, in the request's entry order |

A group with `nRecs = 0` is legal — it is the empty-at-timeout shape (§8)
and the "budget spent by earlier entries" shape. Offsets within a group
ascend; a response never carries records past the durable frontier (§11).

### CreateTopic (type 5)

| field | wire form | meaning |
|---|---|---|
| topic | str | new topic's name, name rule applies |
| partitions | u32 | partition count, 1..16 (§10) |

### CreateTopicResp (type 6)

Empty body.

### ListTopics (type 7)

Empty body — a ListTopics with a non-empty body is answered MALFORMED
(`internal/broker/handlers.go`).

### ListTopicsResp (type 8)

| field | wire form | meaning |
|---|---|---|
| n | u32 | number of topics |
| topics | {str name, u32 partitions} × n | every existing topic and its partition count |

### JoinGroup (type 9)

| field | wire form | meaning |
|---|---|---|
| group | str | group to join (created on first join), name rule applies |
| topic | str | the topic this group consumes — ONE topic per group |

Joining an existing group while naming a different topic is MALFORMED
(`internal/group/coordinator.go`). Joining an unknown topic is
UNKNOWN_TOPIC before any group state changes. Group-membership behavior —
generations, assignment, re-join semantics — is §9.

### JoinGroupResp (type 10)

| field | wire form | meaning |
|---|---|---|
| memberID | str | broker-minted member identity, unique per broker lifetime |
| generation | u64 | the group generation this join is valid for |
| n | u32 | number of assigned partitions |
| assigned | {u32 partition, u64 nextOffset} × n | owned partitions plus the committed next-to-read offset to resume from |

The join response carries the WHOLE resume state — there is no separate
offset-fetch round (`internal/wire/messages.go`, DD-14).

### Heartbeat (type 11)

| field | wire form | meaning |
|---|---|---|
| group | str | the member's group |
| memberID | str | the member's identity from JoinGroupResp |
| generation | u64 | carried but deliberately NOT fenced (§9) — only an unknown member errors a heartbeat |

### HeartbeatResp (type 12)

| field | wire form | meaning |
|---|---|---|
| flags | u8 | bit0 = REJOIN, a LEVEL: set while the member's joined generation trails the group's; the member must re-JoinGroup (§9) |

### CommitOffsets (type 13)

| field | wire form | meaning |
|---|---|---|
| group | str | the member's group |
| memberID | str | the committing member |
| generation | u64 | fenced at serve time (§9) |
| n | u32 | number of entries |
| entries | {u32 partition, u64 next} × n | per partition, the NEXT offset to read — not the last one processed |

Committed positions are next-to-read. Committing a partition outside the
member's current assignment is STALE_GENERATION (§9).

### CommitOffsetsResp (type 14)

Empty body. The ack is sent only AFTER the commit file's atomic write —
an acked commit survives a crash (§9, §11).

### LeaveGroup (type 15)

| field | wire form | meaning |
|---|---|---|
| group | str | the group to leave |
| memberID | str | the leaving member |

A voluntary leave is a membership event: generation bump and immediate
reassignment for the remaining members (§9).

### LeaveGroupResp (type 16)

Empty body.

### GroupFetch (type 17)

| field | wire form | meaning |
|---|---|---|
| group | str | the member's group — the topic is implied by the group's binding, there is no topic field |
| memberID | str | the fetching member |
| generation | u64 | fenced at serve time, including on wake from a park (§9) |
| nEntries | u32 | as Fetch — 1..16 entries |
| entries | {u32 partition, u64 offset} × nEntries | as Fetch |
| maxWaitMs | u32 | as Fetch |
| maxBytes | u32 | as Fetch |

GroupFetch is answered by FetchResp (type 4) — the response shape is
identical to a plain Fetch's and there is no GroupFetchResp. Fetch
validation and parking (§8) apply unchanged; the group fence (§9) runs
before every serve attempt.

### Error (type 255)

| field | wire form | meaning |
|---|---|---|
| code | u16 | a value from the §6 registry |
| msg | str | human-readable diagnostic; its exact text is NOT part of the contract — branch on `code`, never parse `msg` |

## 6. Error-code registry

The table below is the COMPLETE code set — it is diffed three ways against
`internal/wire/errors.go`'s const block and `wire.AllCodes()`, so it cannot
silently gain or lose a row. (This registry has 13 codes; the original
design prose listed 12 — the divergence was surfaced and accepted at the
SL4 gate, D-SL4-6.)

Whether the connection survives after an Error frame depends on the code's
class — see §7.

<!-- registry:codes:begin -->
<!-- grammar: between these markers the only legal lines are blanks, this comment, ONE header row, ONE separator row, and data rows whose first cell is a base-10 integer; cells split on unescaped pipes only (a backslash-pipe is a literal pipe); row count must equal the registry cardinality -->

| code | name | meaning | when sent |
|---|---|---|---|
| 1 | UNKNOWN_TOPIC | the named topic does not exist | Produce, Fetch, or JoinGroup naming a topic the broker does not have |
| 2 | TOPIC_EXISTS | the topic already exists | CreateTopic on a name already present |
| 3 | BAD_PARTITION | partition index out of range | Produce or Fetch naming a partition at or past the topic's partition count |
| 4 | INVALID_NAME | the name fails the §5 name rule | any request whose topic or group name violates the pinned regex, before anything else happens |
| 5 | MSG_TOO_LARGE | produce payload over the 1 MiB cap | Produce with a payload over broker.MaxPayload, rejected BEFORE anything touches storage |
| 6 | FRAME_TOO_LARGE | the frame exceeds the reader's total-size cap | a request frame over the §10 request cap, detected from the length prefix BEFORE any allocation; the connection is closed after the Error frame (§7) |
| 7 | MALFORMED | the frame or body does not parse, or a structural rule is broken | short frame length, unsupported version, truncated or oversized body fields, trailing bytes, unknown message type, zero fetch entries, ListTopics with a body, JoinGroup naming a different topic than the group's binding, unreadable group commit state |
| 8 | CAP_EXCEEDED | a numeric cap was exceeded | maxWaitMs or maxBytes over their §10 caps, topic or partition-count caps, group or member caps, and — unsolicited (§7) — the connection cap |
| 9 | FETCH_TOO_WIDE | too many fetch entries | Fetch or GroupFetch with more than 16 entries |
| 10 | SHUTTING_DOWN | the broker is draining | every request while a graceful stop is in progress; parked fetches instead return the empty shape (§7) |
| 11 | WRITE_FAILED | persistence failed, nothing was acked | append or fsync failure (the partition then rejects writes until restart), a group commit-file write failure, or an internal storage error — an Error with this code means NO ack was sent |
| 12 | STALE_GENERATION | live member, stale generation | GroupFetch or CommitOffsets carrying an outdated generation, or a commit naming partitions outside the member's assignment (§9) |
| 13 | UNKNOWN_MEMBER | the member identity is gone | group unknown, or memberID not in the live set — a swept or disconnected member's later requests land here (§9) |

<!-- registry:codes:end -->

## 7. Connection lifecycle

*(written in the behavior pass — row 3)*

## 8. Fetch and long-poll

*(written in the behavior pass — row 3)*

## 9. Consumer groups

*(written in the behavior pass — row 3)*

## 10. Limits

Every cap, its owning Go constant, and the code a violation earns. This
table is NOT machine-diffed (ledger row 5) — each row cites its constant by
name so auditing a row is one grep; the values themselves are pinned
behaviorally by per-cap rejection tests.

| Go constant | value | what it caps | on violation |
|---|---|---|---|
| `wire.MaxRequestFrame` | 1 MiB + 4 KiB | total on-the-wire request frame, prefix included | FRAME_TOO_LARGE |
| `wire.MaxResponseFrame` | 4 MiB + 64 KiB | total on-the-wire response frame, prefix included | FRAME_TOO_LARGE |
| `broker.MaxPayload` | 1 MiB | one produce payload | MSG_TOO_LARGE |
| `broker.MaxFetchWaitMs` | 30,000 | fetch maxWaitMs | CAP_EXCEEDED |
| `broker.MaxFetchBytes` | 4 MiB | fetch maxBytes | CAP_EXCEEDED |
| `broker.MaxFetchEntries` | 16 | entries per Fetch/GroupFetch | FETCH_TOO_WIDE |
| `wire.nameRE` (names.go) | 128 bytes | topic/group name length and alphabet | INVALID_NAME |
| `storage.MaxTopics` | 64 | live topics | CAP_EXCEEDED |
| `storage.MaxPartitionsPerTopic` | 16 | partitions per topic at creation | CAP_EXCEEDED |
| `group.MaxGroups` | 64 | live consumer groups | CAP_EXCEEDED |
| `group.MaxMembersPerGroup` | 32 | members per group | CAP_EXCEEDED |
| `broker.DefaultMaxConns` | 256 | concurrent connections | CAP_EXCEEDED, unsolicited (§7) |

Defaults (not caps): fetch `maxWaitMs = 0` means 5,000 ms
(`broker.DefaultFetchWait`); fetch `maxBytes = 0` means 1 MiB
(`broker.DefaultFetchBytes`).

## 11. Durability semantics

*(written in the behavior pass — row 3)*

## 12. Normative client obligations vs informative notes

*(written in the behavior pass — row 3)*

## 13. What this protocol does NOT have

*(written in the behavior pass — row 3)*

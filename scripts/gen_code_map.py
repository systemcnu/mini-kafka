#!/usr/bin/env python3
"""Generate docs/code-map.html — the interactive code map for mini-kafka.

Stdlib-only. Cards, wires, journeys, and the story strip live below as
reviewable data; every symbol anchor and tooltip is re-resolved from source
on each run. A self-check asserts each anchor lands on exactly one
declaration and aborts the write on any miss. Run from anywhere:

    python3 scripts/gen_code_map.py
"""

import datetime
import json
import re
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
OUT = REPO / "docs" / "code-map.html"

# ---------------------------------------------------------------------------
# Map data: cards (roles), wires (numbered happy path + support + repairs),
# journeys, story strip, lifecycle ribbon. Symbols are (file, name, kind);
# kinds: func, method, type, const, var, file.
# ---------------------------------------------------------------------------

CARDS = [
    {
        "id": "mk", "nick": "the counter clerk", "icon": "⌨️", "tint": "cmd",
        "file": "cmd/mk/main.go", "col": 1, "row": 1,
        "purpose": "Four thin subcommands over the public client: create-topic, topics, produce, consume.",
        "groups": [
            ("commands", [
                ("cmd/mk/main.go", "main", "func"),
                ("cmd/mk/main.go", "cmdCreateTopic", "func"),
                ("cmd/mk/main.go", "cmdTopics", "func"),
                ("cmd/mk/main.go", "cmdProduce", "func"),
                ("cmd/mk/main.go", "cmdConsume", "func"),
            ]),
        ],
    },
    {
        "id": "cli", "nick": "the courier", "icon": "\U0001f4e8", "tint": "client",
        "file": "client/client.go", "col": 2, "row": 1,
        "purpose": "The only public package: synchronous Producer, raw Consumer, and Admin — one TCP connection each, one request in flight.",
        "groups": [
            ("produce", [
                ("client/client.go", "Producer", "type"),
                ("client/client.go", "Produce", "method"),
            ]),
            ("consume", [
                ("client/client.go", "Consumer", "type"),
                ("client/client.go", "Fetch", "method"),
            ]),
            ("admin", [
                ("client/client.go", "Admin", "type"),
                ("client/client.go", "CreateTopic", "method"),
                ("client/client.go", "Topics", "method"),
            ]),
            ("shared core", [
                ("client/client.go", "roundtrip", "method"),
                ("client/client.go", "Error", "type"),
                ("client/client.go", "DefaultAddr", "const"),
            ]),
        ],
    },
    {
        "id": "srv", "nick": "the one door", "icon": "\U0001f6aa", "tint": "broker",
        "file": "internal/broker/server.go", "col": 3, "row": 1,
        "purpose": "Accepts every connection under a 256-conn cap, reads one frame at a time, and runs the ordered graceful stop.",
        "groups": [
            ("lifecycle", [
                ("internal/broker/server.go", "New", "func"),
                ("internal/broker/server.go", "Start", "method"),
                ("internal/broker/server.go", "Stop", "method"),
            ]),
            ("per connection", [
                ("internal/broker/server.go", "acceptLoop", "method"),
                ("internal/broker/server.go", "serveConn", "method"),
                ("internal/broker/server.go", "serveRequest", "method"),
            ]),
            ("guard rails", [
                ("internal/broker/server.go", "dropConn", "method"),
                ("internal/broker/server.go", "writeError", "method"),
                ("internal/broker/server.go", "Config", "type"),
            ]),
        ],
    },
    {
        "id": "hnd", "nick": "the front desk", "icon": "\U0001f6c2", "tint": "broker",
        "file": "internal/broker/handlers.go", "col": 4, "row": 1,
        "purpose": "Validates names and caps at the edge, calls storage, encodes the response — the only place wire meets storage.",
        "groups": [
            ("routing", [
                ("internal/broker/handlers.go", "dispatch", "method"),
                ("internal/broker/handlers.go", "storageError", "func"),
            ]),
            ("handlers", [
                ("internal/broker/handlers.go", "handleProduce", "method"),
                ("internal/broker/handlers.go", "handleFetch", "method"),
                ("internal/broker/handlers.go", "handleCreateTopic", "method"),
                ("internal/broker/handlers.go", "handleListTopics", "method"),
            ]),
            ("caps", [
                ("internal/broker/handlers.go", "MaxPayload", "const"),
            ]),
        ],
    },
    {
        "id": "part", "nick": "the ledger keeper", "icon": "\U0001f4d2", "tint": "storage",
        "file": "internal/storage/partition.go", "col": 5, "row": 1,
        "purpose": "One append-only log per partition: group-commit flusher, durable frontier, long-poll parking. The ack invariant lives here.",
        "groups": [
            ("the ledger", [
                ("internal/storage/partition.go", "Partition", "type"),
            ]),
            ("write path", [
                ("internal/storage/partition.go", "Append", "method"),
                ("internal/storage/partition.go", "flusher", "method"),
                ("internal/storage/partition.go", "flush", "method"),
                ("internal/storage/partition.go", "flushRemaining", "method"),
            ]),
            ("read path", [
                ("internal/storage/partition.go", "Fetch", "method"),
                ("internal/storage/partition.go", "readLocked", "method"),
            ]),
            ("on-disk formats", [
                ("internal/storage/partition.go", "encodeRecord", "func"),
                ("internal/storage/partition.go", "encodeFrontier", "func"),
                ("internal/storage/partition.go", "parseFrontier", "func"),
            ]),
            ("stop & degrade", [
                ("internal/storage/partition.go", "Close", "method"),
                ("internal/storage/partition.go", "QueuedWaiters", "method"),
                ("internal/storage/partition.go", "ParkedWaiters", "method"),
                ("internal/storage/partition.go", "ErrWriteRejected", "var"),
            ]),
        ],
    },
    {
        "id": "disk", "nick": "the shelf", "icon": "\U0001f4be", "tint": "disk",
        "file": "data/&lt;topic&gt;/&lt;p&gt;/", "col": 6, "row": 1, "endcap": True,
        "purpose": "What actually survives a crash — three files per partition, nothing else.",
        "lines": [
            ("log", "[u32 len][u32 crc32c][payload]… appended, fsynced"),
            ("frontier", "[u64 length][u32 crc32c] — replaced atomically, never torn"),
            ("meta.json", "written last; its presence IS the topic"),
        ],
        "groups": [],
    },
    {
        "id": "mini", "nick": "the ignition switch", "icon": "\U0001f511", "tint": "cmd",
        "file": "cmd/minikafka/main.go", "col": 1, "row": 2,
        "purpose": "Opens the data dir (boot recovery runs here), starts the listener, and turns SIGINT/SIGTERM into the graceful stop.",
        "groups": [
            ("boot & stop", [
                ("cmd/minikafka/main.go", "main", "func"),
                ("cmd/minikafka/main.go", "cmd/minikafka/main.go", "file"),
            ]),
        ],
    },
    {
        "id": "wire", "nick": "the shared language", "icon": "\U0001f5e3️", "tint": "wire",
        "file": "internal/wire — frame.go · messages.go · errors.go · names.go",
        "col": 2, "row": 2,
        "purpose": "Framing, strict typed message bodies, the pinned error-code registry, and name validation — both sides speak only this.",
        "groups": [
            ("framing", [
                ("internal/wire/frame.go", "WriteFrame", "func"),
                ("internal/wire/frame.go", "ReadFrame", "func"),
                ("internal/wire/frame.go", "Version", "const"),
            ]),
            ("bodies", [
                ("internal/wire/messages.go", "Produce", "type"),
                ("internal/wire/messages.go", "Fetch", "type"),
                ("internal/wire/messages.go", "FetchResp", "type"),
                ("internal/wire/messages.go", "ErrorMsg", "type"),
                ("internal/wire/messages.go", "DecodeProduce", "func"),
                ("internal/wire/messages.go", "DecodeFetch", "func"),
            ]),
            ("registry", [
                ("internal/wire/errors.go", "Code", "type"),
                ("internal/wire/errors.go", "Errf", "func"),
                ("internal/wire/names.go", "ValidateName", "func"),
            ]),
        ],
    },
    {
        "id": "proofs", "nick": "the proving ground", "icon": "\U0001f9ea", "tint": "proofs",
        "file": "*_test.go · scripts/checks.sh", "col": 3, "row": 2,
        "purpose": "The SL0 proof battery: ack-ordering recorder, boot-scan branches, frontier-advance wakes, graceful drain, live caps over the wire.",
        "groups": [
            ("key suites", [
                ("internal/storage/partition_test.go", "internal/storage/partition_test.go", "file"),
                ("internal/storage/recovery_test.go", "internal/storage/recovery_test.go", "file"),
                ("internal/storage/longpoll_test.go", "internal/storage/longpoll_test.go", "file"),
                ("internal/broker/shutdown_test.go", "internal/broker/shutdown_test.go", "file"),
                ("internal/broker/caps_test.go", "internal/broker/caps_test.go", "file"),
                ("internal/broker/broker_test.go", "internal/broker/broker_test.go", "file"),
                ("scripts/checks.sh", "scripts/checks.sh", "file"),
            ]),
        ],
    },
    {
        "id": "store", "nick": "the card catalog", "icon": "\U0001f5c3️", "tint": "storage",
        "file": "internal/storage/store.go", "col": 4, "row": 2,
        "purpose": "The topics registry: atomic topic creation (meta.json last), partition lookup, and the stop-time drain.",
        "groups": [
            ("registry", [
                ("internal/storage/store.go", "Store", "type"),
                ("internal/storage/store.go", "Open", "func"),
                ("internal/storage/store.go", "CreateTopic", "method"),
                ("internal/storage/store.go", "Topics", "method"),
                ("internal/storage/store.go", "Partition", "method"),
            ]),
            ("stop", [
                ("internal/storage/store.go", "Drain", "method"),
                ("internal/storage/store.go", "Close", "method"),
            ]),
        ],
    },
    {
        "id": "rec", "nick": "the boot inspector", "icon": "\U0001f50e", "tint": "storage",
        "file": "internal/storage/recovery.go", "col": 5, "row": 2,
        "purpose": "Walks each log from byte 0 at boot: serve what was acked, truncate what wasn't, refuse loudly if acked bytes are damaged.",
        "groups": [
            ("the scan", [
                ("internal/storage/recovery.go", "recoverPartition", "func"),
                ("internal/storage/recovery.go", "parseRecordAt", "func"),
                ("internal/storage/recovery.go", "scanState", "type"),
            ]),
        ],
    },
    {
        "id": "fsx", "nick": "the ground floor", "icon": "\U0001f9f1", "tint": "storage",
        "file": "internal/storage/fs.go · syncer.go", "col": 6, "row": 2,
        "purpose": "The injectable filesystem and fsync seams — including the never-torn atomicWrite recipe — that fault tests script.",
        "groups": [
            ("fs seam", [
                ("internal/storage/fs.go", "FS", "type"),
                ("internal/storage/fs.go", "File", "type"),
                ("internal/storage/fs.go", "OSFS", "func"),
                ("internal/storage/fs.go", "WriteFileAtomic", "method"),
                ("internal/storage/fs.go", "SyncDir", "method"),
            ]),
            ("fsync seam", [
                ("internal/storage/syncer.go", "Syncer", "type"),
                ("internal/storage/syncer.go", "FileSyncer", "type"),
                ("internal/storage/syncer.go", "Sync", "method"),
            ]),
        ],
    },
]

# Wires. kind: main (numbered accent), support (muted), repair (dashed warn,
# hidden behind the toggle). route: h | v | top | chan | below.
WIRES = [
    {"id": "w1", "from": "mk", "to": "cli", "kind": "main", "num": 1,
     "label": "hand over the message", "route": "h", "lmode": "above"},
    {"id": "w2", "from": "cli", "to": "srv", "kind": "main", "num": 2,
     "label": "one frame over TCP", "route": "h", "lmode": "above"},
    {"id": "w3", "from": "srv", "to": "hnd", "kind": "main", "num": 3,
     "label": "check it, route it", "route": "h", "lmode": "above"},
    {"id": "w4", "from": "hnd", "to": "part", "kind": "main", "num": 4,
     "label": "join the append queue", "route": "h", "lmode": "above"},
    {"id": "w5", "from": "part", "to": "disk", "kind": "main", "num": 5,
     "label": "ink it, fsync it, raise the flag", "route": "h", "lmode": "above"},
    {"id": "w6", "from": "part", "to": "cli", "kind": "main", "num": 6,
     "label": "the ack rides back with the offset", "route": "top",
     "adx": -30, "bdx": 30, "arcy": 16, "lfrac": 0.30},

    {"id": "s1", "from": "mini", "to": "srv", "kind": "support",
     "label": "start · stop", "route": "chan", "exit": "top", "enter": "bottom",
     "cy": 68, "adx": 0, "bdx": -34, "ldx": -100, "ldy": -7},
    {"id": "s2", "from": "srv", "to": "store", "kind": "support",
     "label": "boot: open & recover", "route": "chan", "exit": "bottom", "enter": "top",
     "cy": 30, "adx": 34, "bdx": -30, "ldx": 6, "ldy": -7},
    {"id": "s3", "from": "store", "to": "rec", "kind": "support",
     "label": "boot: scan every partition", "route": "chan", "exit": "top", "enter": "top",
     "cy": 44, "adx": 34, "bdx": -34, "ldx": 0, "ldy": -7},
    {"id": "s4", "from": "cli", "to": "wire", "kind": "support",
     "label": "speaks the wire language", "route": "v", "ldx": 8, "ldy": 0},
    {"id": "s5", "from": "part", "to": "fsx", "kind": "support",
     "label": "every byte through the seams", "route": "chan", "exit": "bottom", "enter": "top",
     "cy": 30, "adx": 40, "bdx": -30, "ldx": 10, "ldy": -7},

    {"id": "r1", "from": "hnd", "to": "cli", "kind": "repair",
     "label": "rejections return as Error frames", "route": "chan",
     "exit": "bottom", "enter": "bottom", "cy": 12, "adx": -30, "bdx": -30,
     "ldx": 0, "ldy": 14},
    {"id": "r2", "from": "part", "to": "hnd", "kind": "repair",
     "label": "write failed → reject until restart", "route": "chan",
     "exit": "bottom", "enter": "bottom", "cy": 12, "adx": 0, "bdx": 30,
     "ldx": 0, "ldy": 14},
    {"id": "r3", "from": "rec", "to": "mini", "kind": "repair",
     "label": "acked damage → refuse to boot", "route": "below",
     "cy": 22, "adx": 0, "bdx": 0, "ldx": 0, "ldy": -7},
]

STORY = [
    {"n": 1, "icon": "\U0001f9d1‍\U0001f4bb", "label": "you type",
     "sub": "mk produce / mk consume at the terminal", "card": "mk"},
    {"n": 2, "icon": "✉️", "label": "sealed & sent",
     "sub": "the client frames it — one request over TCP", "card": "cli"},
    {"n": 3, "icon": "\U0001f6aa", "label": "the one door",
     "sub": "the broker admits, checks, and routes it", "card": "srv"},
    {"n": 4, "icon": "\U0001f58b️", "label": "written in ink",
     "sub": "appended to the partition log and fsynced", "card": "part"},
    {"n": 5, "icon": "\U0001f6a9", "label": "the flag advances",
     "sub": "the durable frontier moves — only then the ack", "card": "disk"},
    {"n": 6, "icon": "\U0001f4ec", "label": "readers catch up",
     "sub": "parked consumers wake — never past the flag", "card": "part"},
]

RIBBON = {
    "title": "A record's life",
    "states": ["queued", "written", "fsynced", "durable", "acked"],
    "transitions": [
        ("internal/storage/partition.go#Append", "Append"),
        ("internal/storage/partition.go#flush", "flush"),
        ("internal/storage/syncer.go#Sync", "Syncer.Sync"),
        ("internal/storage/fs.go#WriteFileAtomic", "WriteFileAtomic"),
    ],
    "note_key": "internal/storage/partition.go#readLocked",
    "note": "“durable” is also the read horizon — readLocked serves up to here and never past it",
}

JOURNEYS = [
    {
        "id": "produce", "title": "A produce, acked in ink",
        "proves": "No producer is acked before its bytes are fsynced and the durable frontier has advanced (DD-4/DD-6).",
        "steps": [
            ("cmd/mk/main.go#cmdProduce", "mk produce dials the shipped client and sends the -m message (or one message per stdin line)."),
            ("client/client.go#Produce", "Produce blocks — the client is synchronous, and this call will not return until the broker's durable ack."),
            ("client/client.go#roundtrip", "roundtrip frames the request and waits for the matching response or a typed Error frame: one request in flight per connection."),
            ("internal/wire/frame.go#WriteFrame", "The envelope goes on: [u32 len][u8 ver][u8 type][payload], big-endian."),
            ("internal/broker/server.go#serveConn", "A goroutine per connection reads exactly one frame at a time — deliberately no read deadline, which would kill parked fetches."),
            ("internal/broker/server.go#serveRequest", "The inflight window opens here, spanning dispatch AND the response write, so a graceful stop never closes a conn under a response in transit."),
            ("internal/broker/handlers.go#handleProduce", "Name and the 1 MiB payload cap are checked BEFORE storage is touched — a rejected produce writes nothing."),
            ("internal/storage/store.go#Partition", "The topics registry resolves (topic, partition) to the live Partition."),
            ("internal/storage/partition.go#Append", "The offset is assigned at append time (so concurrent acks can't misorder) and the producer parks on its waiter's done channel."),
            ("internal/storage/partition.go#flusher", "The group-commit goroutine flushes when the oldest waiter turns 5 ms old — time-only trigger."),
            ("internal/storage/partition.go#flush", "The invariant in one function: write → fsync → frontier atomicWrite → index/notify swap under the write lock → ack."),
            ("internal/storage/fs.go#WriteFileAtomic", "The frontier file is replaced via temp → fsync → rename → fsync(dir): old or new, never torn."),
            ("client/client.go#Produce", "Only now does ProduceResp carry the assigned offset back; mk prints it."),
        ],
    },
    {
        "id": "fetch", "title": "A fetch that waits at the tail",
        "proves": "Readers only ever see fsync-covered records, and a parked fetch cannot miss a wake (DD-5/DD-25).",
        "steps": [
            ("cmd/mk/main.go#cmdConsume", "mk consume -f long-polls; without -f a 1 ms wait makes hitting the tail return at once."),
            ("client/client.go#Fetch", "One (partition, offset) entry per fetch at SL0; an empty result is the legal at-tail shape."),
            ("internal/broker/handlers.go#handleFetch", "Every cap is validated up front — wait, bytes, entry count — and any invalid entry fails the whole frame."),
            ("internal/storage/partition.go#Fetch", "Tail-check and notify-channel capture happen under the read lock, then the fetch parks — the missed-wakeup proof."),
            ("internal/storage/partition.go#readLocked", "Serves only index entries below the durable frontier; always at least one record, so an oversized record can't wedge a consumer."),
            ("internal/storage/partition.go#flush", "A produce lands: the flusher advances the frontier and closes the notify channel under the write lock."),
            ("internal/storage/partition.go#Fetch", "The parked fetch wakes, loops, re-reads under the lock — and now sees durable records."),
            ("cmd/mk/main.go#cmdConsume", "Records ride back in a FetchResp; mk prints offset⇥payload and advances its offset."),
        ],
    },
    {
        "id": "recover", "title": "Kill it, restart it, trust it",
        "proves": "Acked data is never silently lost: damage inside the frontier refuses the boot; an unacked tail is truncated (DD-4/LOG-4).",
        "steps": [
            ("cmd/minikafka/main.go#main", "Boot: opening the data dir runs recovery before the listener ever binds."),
            ("internal/broker/server.go#New", "broker.New opens the store — a refused partition aborts loudly right here and the broker does not start."),
            ("internal/storage/store.go#Open", "Removes aborted creates (topic dirs without meta.json), then boot-scans every partition."),
            ("internal/storage/recovery.go#recoverPartition", "Parses the log from byte 0; the frontier decides which bytes were promised to producers."),
            ("internal/storage/partition.go#parseFrontier", "The frontier file must decode exactly — size and CRC. An unreadable one is refused, never guessed at."),
            ("internal/storage/recovery.go#parseRecordAt", "A record is valid iff its full byte range fits and its CRC matches."),
            ("internal/storage/recovery.go#recoverPartition", "Damage inside [0, frontier): refuse loudly. Invalid records past it: truncate — that data was never acked."),
            ("internal/broker/server.go#Start", "Only a fully recovered store gets a listener."),
        ],
    },
    {
        "id": "stop", "title": "Stopping without dropping",
        "proves": "A graceful stop still acks queued produces and returns parked fetches empty (D-SL0-6).",
        "steps": [
            ("cmd/minikafka/main.go#main", "SIGINT/SIGTERM lands here; srv.Stop() runs the ordered sequence."),
            ("internal/broker/server.go#Stop", "In order: stop accepting → draining on → release parks → drain acks ≤5 s → close storage → close conns."),
            ("internal/storage/partition.go#Fetch", "close(stopping) unparks every fetch; each returns its empty-at-timeout shape instead of an error."),
            ("internal/storage/store.go#Drain", "Waits (bounded) for already-queued produce waiters — a snapshot, never quiescence-chasing."),
            ("internal/storage/partition.go#QueuedWaiters", "Queued plus in-flight-batch waiters; the drain polls this down to zero."),
            ("internal/storage/partition.go#flushRemaining", "At quit the flusher flushes what is already queued, so those producers still get real acks."),
            ("internal/storage/store.go#Close", "Joins every flusher and closes the log files."),
            ("internal/broker/server.go#dropConn", "Every exit path drops the conn from the registry — a missed decrement would wedge the cap after 256 connections ever."),
        ],
    },
    {
        "id": "hostile", "title": "A hostile frame bounces at the edge",
        "proves": "Malformed or oversized input is rejected with a pinned error code before it can touch the disk (D-SL0-8/NFR-2).",
        "steps": [
            ("internal/wire/frame.go#ReadFrame", "The frame length is checked against the cap BEFORE the payload is allocated."),
            ("internal/wire/messages.go#DecodeProduce", "Strict body decode: truncation, hostile lengths, trailing bytes — all MALFORMED."),
            ("internal/wire/names.go#ValidateName", "Names are validated before any filesystem path is formed: '..' and separators are structurally impossible."),
            ("internal/broker/handlers.go#handleProduce", "The 1 MiB payload cap rejects before storage — nothing is written."),
            ("internal/broker/handlers.go#storageError", "Storage sentinels map onto pinned wire codes — storage never imports wire."),
            ("internal/broker/server.go#writeError", "The rejection rides back as a type-255 Error frame; after an oversized frame the conn is closed — the stream is no longer trustworthy."),
        ],
    },
]

# ---------------------------------------------------------------------------
# Anchor resolution + doc-comment extraction (Go + shell), with self-check.
# ---------------------------------------------------------------------------


def decl_patterns(kind, name):
    n = re.escape(name)
    if kind == "func":
        return [rf"^func {n}\("]
    if kind == "method":
        return [rf"^func \([^)]*\) {n}\("]
    if kind == "type":
        return [rf"^type {n} "]
    if kind == "const":
        return [rf"^const {n}\b", rf"^\t{n}\s*=\s", rf"^\t{n}\s+\S+\s*=\s"]
    if kind == "var":
        return [rf"^var {n}\b", rf"^\t{n}\s*=\s"]
    raise ValueError(kind)


def doc_first_line(lines, idx0):
    """First line of the doc-comment block directly above lines[idx0]."""
    i = idx0 - 1
    if i >= 0 and re.match(r"^(const|var|type)\s*\(\s*$", lines[i]):
        i -= 1  # grouped decl: the block comment sits above 'const ('
    end = i
    while i >= 0 and re.match(r"^\s*//", lines[i]):
        i -= 1
    if i == end:
        return None
    return re.sub(r"^\s*//\s?", "", lines[i + 1]).strip()


def file_first_comment(lines):
    i = 1 if lines and lines[0].startswith("#!") else 0
    if i < len(lines) and (lines[i].startswith("//") or lines[i].startswith("#")):
        return re.sub(r"^(//|#)\s?", "", lines[i]).strip()
    return None


def resolve_symbols():
    """Returns (symbols dict key->{file,name,line,doc}, errors list)."""
    wanted = {}
    for card in CARDS:
        for _, syms in card["groups"]:
            for f, name, kind in syms:
                wanted[f"{f}#{name}"] = (f, name, kind)
    cache, symbols, errors = {}, {}, []
    for key, (f, name, kind) in sorted(wanted.items()):
        path = REPO / f
        if f not in cache:
            if not path.exists():
                errors.append(f"{key}: file {f} does not exist")
                continue
            cache[f] = path.read_text().splitlines()
        lines = cache[f]
        if kind == "file":
            doc = file_first_comment(lines)
            symbols[key] = {"file": f, "name": f.rsplit("/", 1)[-1],
                            "line": 1, "doc": doc}
            continue
        hits = []
        for pat in decl_patterns(kind, name):
            rx = re.compile(pat)
            for i, ln in enumerate(lines):
                if rx.search(ln) and i not in [h[0] for h in hits]:
                    hits.append((i, ln))
        if len(hits) != 1:
            errors.append(f"{key} ({kind}): {len(hits)} declaration matches"
                          + "".join(f"\n    line {i+1}: {ln.strip()}" for i, ln in hits))
            continue
        idx0, _ = hits[0]
        symbols[key] = {"file": f, "name": name, "line": idx0 + 1,
                        "doc": doc_first_line(lines, idx0)}
    return symbols, errors


def self_check(symbols, errors):
    card_ids = {c["id"] for c in CARDS}
    chip_keys = set(symbols.keys())
    for w in WIRES:
        for end in (w["from"], w["to"]):
            if end not in card_ids:
                errors.append(f"wire {w['id']}: unknown card '{end}'")
    for s in STORY:
        if s["card"] not in card_ids:
            errors.append(f"story stop {s['n']}: unknown card '{s['card']}'")
    for j in JOURNEYS:
        for key, _ in j["steps"]:
            if key not in chip_keys:
                errors.append(f"journey '{j['id']}': step symbol '{key}' is not a card chip")
    for key, _ in RIBBON["transitions"] + [(RIBBON["note_key"], "")]:
        if key not in chip_keys:
            errors.append(f"ribbon: symbol '{key}' is not a card chip")
    return errors


def git(*args):
    return subprocess.run(["git", "-C", str(REPO), *args],
                          capture_output=True, text=True).stdout.strip()


# ---------------------------------------------------------------------------
# HTML assembly
# ---------------------------------------------------------------------------

def esc(s):
    return (s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def chip(key, label=None):
    return (f'<button class="chip" data-key="{esc(key)}">'
            f'{esc(label or key.split("#", 1)[1])}</button>')


def render_card(c):
    body = []
    body.append(f'<header><span class="tile t-{c["tint"]}">{c["icon"]}</span>'
                f'<h3>{esc(c["nick"])}</h3></header>')
    body.append(f'<p class="purpose">{esc(c["purpose"])}</p>')
    body.append(f'<div class="file">{c["file"]}</div>')
    if c.get("endcap"):
        rows = "".join(f'<div class="fmt"><span class="fname">{esc(n)}</span>'
                       f'<span class="fdesc">{esc(d)}</span></div>'
                       for n, d in c["lines"])
        body.append(f'<div class="formats">{rows}</div>')
    if c["groups"]:
        gs = []
        for glabel, syms in c["groups"]:
            chips = " ".join(chip(f"{f}#{n}",
                                  n.rsplit("/", 1)[-1] if k == "file" else n)
                             for f, n, k in syms)
            gs.append(f'<div class="group"><span class="glabel">{esc(glabel)}</span>'
                      f'<span class="gsyms">{chips}</span></div>')
        body.append(f'<div class="chips">{"".join(gs)}</div>')
    return (f'<article class="card" id="card-{c["id"]}" data-card="{c["id"]}" '
            f'style="grid-column:{c["col"]};grid-row:{c["row"]}">'
            + "".join(body) + "</article>")


def render_story():
    stops = []
    for s in STORY:
        stops.append(
            f'<button class="stop" data-card="{s["card"]}">'
            f'<span class="disc">{s["n"]}</span>'
            f'<span class="sicon">{s["icon"]}</span>'
            f'<span class="slabel">{esc(s["label"])}</span>'
            f'<span class="ssub">{esc(s["sub"])}</span></button>')
    return ('<section id="story" aria-label="the story in six stops">'
            '<div class="line"></div>' + "".join(stops) + "</section>")


def render_ribbon(symbols):
    parts = ['<section id="ribbon"><h2>A record’s life</h2><div class="flow">']
    states, trans = RIBBON["states"], RIBBON["transitions"]
    for i, st in enumerate(states):
        parts.append(f'<span class="pill">{esc(st)}</span>')
        if i < len(trans):
            key, label = trans[i]
            parts.append(f'<span class="tr"><span class="arrow">→</span>'
                         f'{chip(key, label)}</span>')
    parts.append("</div>")
    parts.append(f'<p class="rnote">{chip(RIBBON["note_key"], "readLocked")} '
                 f'{esc(RIBBON["note"])}</p></section>')
    return "".join(parts)


def build_html(symbols, commit, dirty, verified):
    data = {
        "root": str(REPO),
        "commit": commit, "dirty": dirty,
        "symbols": symbols,
        "wires": WIRES,
        "journeys": [
            {"id": j["id"], "title": j["title"], "proves": j["proves"],
             "steps": [{"key": k, "text": t} for k, t in j["steps"]]}
            for j in JOURNEYS
        ],
        "cards": {c["id"]: c["nick"] for c in CARDS},
    }
    data_json = json.dumps(data, ensure_ascii=False).replace("</", "<\\/")

    cards_html = "".join(render_card(c) for c in CARDS)
    story_html = render_story()
    ribbon_html = render_ribbon(symbols)
    journey_opts = "".join(f'<option value="{j["id"]}">{esc(j["title"])}</option>'
                           for j in JOURNEYS)
    gen_date = datetime.date.today().isoformat()
    dirty_mark = " · working tree dirty for mapped files" if dirty else ""

    html = HTML_TEMPLATE
    for token, value in [
        ("%%DATA%%", data_json),
        ("%%CARDS%%", cards_html),
        ("%%STORY%%", story_html),
        ("%%RIBBON%%", ribbon_html),
        ("%%JOURNEY_OPTIONS%%", journey_opts),
        ("%%COMMIT%%", commit + (" (dirty)" if dirty else "")),
        ("%%DIRTY%%", dirty_mark),
        ("%%VERIFIED%%", str(verified)),
        ("%%DATE%%", gen_date),
        ("%%ROOT%%", str(REPO)),
    ]:
        html = html.replace(token, value)
    return html


HTML_TEMPLATE = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link rel="icon" href="data:,">
<title>mini-kafka &mdash; code map</title>
<style>
:root {
  --paper:#faf7f1; --ink:#26221b; --muted:#71685a; --faint:#a1988a;
  --card:#fffdf8; --border:#e5ddcd; --border2:#d8cfbc;
  --accent:#4338ca; --accent-soft:#eef0fd; --on-accent:#fff;
  --wirec:#a89e8d; --warn:#b45309; --good:#15803d;
  --tile-cmd:#e7e2d6; --tile-client:#dbe9f5; --tile-broker:#e3e4f7;
  --tile-storage:#f3e5cd; --tile-wire:#ecdff2; --tile-disk:#e6e2da; --tile-proofs:#dcecd9;
  --ring:#4338ca; --find-dim:.22;
  --serif:Charter,"Bitstream Charter",Georgia,serif;
  --disp:"Avenir Next","Segoe UI Variable","Segoe UI",system-ui,sans-serif;
  --mono:"SF Mono","Cascadia Code",ui-monospace,Menlo,monospace;
}
@media (prefers-color-scheme: dark) {
  :root {
    --paper:#15171c; --ink:#e8e5de; --muted:#9d968a; --faint:#6d675d;
    --card:#1c1f26; --border:#2c3039; --border2:#3a3f4a;
    --accent:#8f97f8; --accent-soft:#272b45; --on-accent:#101223;
    --wirec:#5c6270; --warn:#e0a252; --good:#65c07d;
    --tile-cmd:#2a2d34; --tile-client:#243240; --tile-broker:#2a2c4a;
    --tile-storage:#3a3122; --tile-wire:#342a3d; --tile-disk:#2b2c30; --tile-proofs:#243527;
    --ring:#8f97f8;
  }
}
* { box-sizing:border-box; margin:0; }
html { -webkit-font-smoothing:antialiased; }
body {
  background:var(--paper); color:var(--ink); font-family:var(--disp);
  font-size:14px; line-height:1.45;
  font-variant-numeric:tabular-nums;
}
button { font:inherit; color:inherit; background:none; border:none; cursor:pointer; padding:0; text-align:inherit; }

header.page {
  display:flex; flex-wrap:wrap; align-items:baseline; gap:10px 24px;
  padding:20px 28px 10px;
}
header.page h1 { font-size:21px; font-weight:600; letter-spacing:-.01em; }
header.page h1 .mono { font-family:var(--mono); font-size:19px; }
.standfirst { font-family:var(--serif); font-size:14.5px; color:var(--muted); max-width:560px; }
.editorbox { margin-left:auto; display:flex; align-items:center; gap:8px; font-size:12px; color:var(--muted); }
.editorbox select,.editorbox input {
  font:12px var(--mono); color:var(--ink); background:var(--card);
  border:1px solid var(--border); border-radius:6px; padding:3px 6px;
}
.editorbox input { width:300px; }

#stick { position:sticky; top:0; z-index:40; }
#bar {
  display:flex; align-items:center; gap:10px;
  padding:8px 28px; background:color-mix(in srgb, var(--paper) 92%, transparent);
  backdrop-filter:blur(6px); border-bottom:1px solid var(--border);
  flex-wrap:wrap;
}
#find {
  font:13px var(--mono); color:var(--ink); background:var(--card);
  border:1px solid var(--border); border-radius:8px; padding:5px 10px; width:230px;
}
#find:focus { outline:2px solid var(--accent); outline-offset:1px; }
#hits { font-size:12px; color:var(--muted); min-width:64px; }
#jsel {
  font:13px var(--disp); color:var(--ink); background:var(--card);
  border:1px solid var(--border); border-radius:8px; padding:5px 8px; max-width:280px;
}
.barbtn {
  font-size:12.5px; padding:5px 10px; border:1px solid var(--border);
  border-radius:8px; background:var(--card); color:var(--muted);
}
.barbtn[aria-pressed="true"] { color:var(--accent); border-color:var(--accent); }
.barbtn:disabled { opacity:.4; cursor:default; }
#stepno { font-size:12px; color:var(--muted); min-width:52px; text-align:center; }
.hint { margin-left:auto; font-size:11.5px; color:var(--faint); }

#jpanel { display:none; padding:10px 28px 12px; border-bottom:1px solid var(--border);
  background:color-mix(in srgb, var(--card) 82%, transparent); backdrop-filter:blur(6px); }
body.touring #jpanel { display:block; }
#jproves { font-size:12px; color:var(--muted); margin-bottom:8px; }
#jproves b { color:var(--ink); font-weight:600; }
#jchain { display:flex; gap:6px; overflow-x:auto; padding-bottom:6px; white-space:nowrap; }
.jstep { display:inline-flex; align-items:center; gap:6px; padding:3px 9px;
  border:1px solid var(--border); border-radius:999px; font-size:12px; color:var(--muted); flex:0 0 auto; }
.jstep .n { font-size:10.5px; color:var(--faint); }
.jstep .sym { font-family:var(--mono); color:var(--accent); }
.jstep .mod { font-size:10.5px; color:var(--faint); }
.jstep.cur { border-color:var(--accent); background:var(--accent-soft); color:var(--ink); }
#jnarr { font-family:var(--serif); font-size:14px; margin-top:6px; max-width:860px; }
#jnarr a { color:var(--accent); font-family:var(--mono); font-size:12px; text-decoration:none; margin-left:8px; }

#story { position:relative; display:flex; justify-content:space-between;
  padding:26px 56px 10px; max-width:1280px; margin:0 auto; }
#story .line { position:absolute; left:72px; right:72px; top:43px; height:3px;
  border-radius:2px; background:var(--accent); opacity:.85; }
.stop { position:relative; display:flex; flex-direction:column; align-items:center;
  width:150px; gap:2px; }
.stop .disc { width:30px; height:30px; border-radius:50%; background:var(--accent);
  color:var(--on-accent); display:grid; place-items:center; font-weight:700;
  font-size:14px; border:3px solid var(--paper); box-shadow:0 0 0 1px var(--accent); }
.stop .sicon { font-size:17px; margin-top:5px; }
.stop .slabel { font-weight:600; font-size:13px; }
.stop .ssub { font-family:var(--serif); font-size:11.5px; color:var(--muted); text-align:center; }
.stop:hover .slabel { color:var(--accent); }

#map { position:relative; max-width:1560px; margin:2px auto 0;
  padding:74px 24px 46px; }
#wires { position:absolute; inset:0; width:100%; height:100%; pointer-events:none; z-index:1; }
#map .grid { position:relative; z-index:2; display:grid;
  grid-template-columns:repeat(6, 1fr); column-gap:30px; row-gap:96px; }
.card { background:var(--card); border:1px solid var(--border); border-radius:12px;
  padding:12px 13px 11px; box-shadow:0 1px 3px rgb(0 0 0 / .05); min-width:0; }
.card header { display:flex; align-items:center; gap:8px; margin-bottom:5px; }
.tile { width:28px; height:28px; border-radius:8px; display:grid; place-items:center;
  font-size:15px; flex:0 0 auto; }
.t-cmd{background:var(--tile-cmd)} .t-client{background:var(--tile-client)}
.t-broker{background:var(--tile-broker)} .t-storage{background:var(--tile-storage)}
.t-wire{background:var(--tile-wire)} .t-disk{background:var(--tile-disk)}
.t-proofs{background:var(--tile-proofs)}
.card h3 { font-size:14.5px; font-weight:650; letter-spacing:-.005em; }
.purpose { font-family:var(--serif); font-size:12.5px; color:var(--muted); margin-bottom:6px; }
.file { font-family:var(--mono); font-size:10.5px; color:var(--faint); margin-bottom:7px;
  overflow-wrap:anywhere; }
.group { margin-bottom:4px; font-size:12px; line-height:1.7; }
.glabel { font-size:10px; text-transform:uppercase; letter-spacing:.07em;
  color:var(--faint); margin-right:6px; }
.chip { font-family:var(--mono); font-size:12px; color:var(--accent);
  padding:0 1px; border-radius:4px; }
.chip:hover { text-decoration:underline; }
.gsyms .chip + .chip::before { content:"\00b7\00a0"; color:var(--border2); text-decoration:none; display:inline-block; padding-right:1px; }
.formats { display:grid; gap:5px; margin-top:2px; }
.fmt { font-size:11px; }
.fname { font-family:var(--mono); font-weight:600; display:block; }
.fdesc { font-family:var(--mono); font-size:10.5px; color:var(--muted); }
body.compact .chips, body.compact .purpose, body.compact .formats { display:none; }

.card.ring { outline:2px solid var(--ring); outline-offset:2px; }
.chip.stepped { background:var(--accent); color:var(--on-accent); padding:0 5px; }
.chip.flash { background:var(--accent-soft); }
body.finding .card.dimmed { opacity:var(--find-dim); }
body.finding .chip.dimmed { opacity:.3; }

.wire path { fill:none; }
.wire.main path { stroke:var(--accent); stroke-width:2; }
.wire.support path { stroke:var(--wirec); stroke-width:1.3; }
.wire.repair path { stroke:var(--warn); stroke-width:1.4; stroke-dasharray:5 4; }
.wire.repair { display:none; }
body.repairs .wire.repair, .wire.repair.lit { display:block; }
.wlabel { font:11px var(--disp); fill:var(--muted); }
.wire.main .wlabel { fill:var(--ink); font-weight:500; }
.wire.repair .wlabel { fill:var(--warn); }
.badge circle { fill:var(--accent); }
.badge text { font:700 11px var(--disp); fill:var(--on-accent); text-anchor:middle; }
.leader { stroke:var(--accent); stroke-width:1; opacity:.35; }
body.isolating .wire { opacity:.10; }
body.isolating .wire.lit { opacity:1; }
body.isolating .card { opacity:.45; }
body.isolating .card.lit { opacity:1; }

#ribbon { max-width:1560px; margin:6px auto 0; padding:10px 24px 4px; }
#ribbon h2 { font-size:13px; text-transform:uppercase; letter-spacing:.08em;
  color:var(--faint); font-weight:600; margin-bottom:8px; }
#ribbon .flow { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
.pill { border:1px solid var(--border2); border-radius:999px; padding:3px 12px;
  font-size:12.5px; font-weight:600; background:var(--card); }
.tr { display:inline-flex; align-items:center; gap:5px; font-size:12px; }
.tr .arrow { color:var(--faint); }
.rnote { font-family:var(--serif); font-size:12px; color:var(--muted); margin-top:7px; }

footer { max-width:1560px; margin:8px auto 0; padding:12px 24px 26px;
  font-size:11.5px; color:var(--faint); border-top:1px solid var(--border); }
footer .mono { font-family:var(--mono); }
footer a { color:var(--muted); }

#tip { position:fixed; z-index:60; max-width:400px; background:var(--card);
  border:1px solid var(--border2); border-radius:8px; padding:8px 10px;
  box-shadow:0 4px 14px rgb(0 0 0 / .14); display:none; pointer-events:none; }
#tip .tdoc { font-family:var(--serif); font-size:12.5px; margin-bottom:4px; }
#tip .tdoc.none { color:var(--faint); font-style:italic; }
#tip .tpath { font-family:var(--mono); font-size:11px; color:var(--muted); }
#tip .thint { font-size:10.5px; color:var(--faint); margin-top:3px; }

#toast { position:fixed; bottom:18px; left:50%; transform:translateX(-50%);
  background:var(--ink); color:var(--paper); font-size:12px; padding:6px 14px;
  border-radius:999px; opacity:0; transition:opacity .2s; z-index:70; pointer-events:none; }
#toast.show { opacity:.92; }
</style>
</head>
<body>
<header class="page">
  <h1><span class="mono">mini-kafka</span> &mdash; code map</h1>
  <p class="standfirst">A single-broker, Kafka-style durable log: a message&rsquo;s trip from
  your terminal to fsynced ink on disk &mdash; and never an ack before the ink is dry.</p>
  <div class="editorbox">
    <label>open in <select id="scheme">
      <option value="vscode">VS Code</option>
      <option value="cursor">Cursor</option>
      <option value="copy">copy path</option>
    </select></label>
    <label>root <input id="root" spellcheck="false" value="%%ROOT%%"></label>
  </div>
</header>

<div id="stick">
<nav id="bar">
  <input id="find" type="search" placeholder="find symbol, doc, or path&hellip;" aria-label="find">
  <span id="hits"></span>
  <select id="jsel" aria-label="pick a journey">
    <option value="">journeys: pick a story&hellip;</option>
    %%JOURNEY_OPTIONS%%
  </select>
  <button class="barbtn" id="jprev" disabled>&larr; prev</button>
  <span id="stepno"></span>
  <button class="barbtn" id="jnext" disabled>next &rarr;</button>
  <button class="barbtn" id="repairs" aria-pressed="false">show repairs</button>
  <button class="barbtn" id="compact" aria-pressed="false">compact</button>
  <span class="hint">click a name to open it in your editor &middot; &#8997;-click copies path:line &middot; &larr;/&rarr; step a journey</span>
</nav>

<div id="jpanel">
  <div id="jproves"></div>
  <div id="jchain"></div>
  <p id="jnarr"></p>
</div>
</div>

%%STORY%%

<section id="map">
  <svg id="wires" aria-hidden="true"></svg>
  <div class="grid">
%%CARDS%%
  </div>
</section>

%%RIBBON%%

<footer>
  anchors pinned at commit <span class="mono">%%COMMIT%%</span> &middot;
  <span class="mono">%%VERIFIED%%</span> anchors verified against source &middot;
  generated %%DATE%%%%DIRTY%% &middot;
  refresh with <span class="mono">python3 scripts/gen_code_map.py</span> &middot;
  companion to <a href="../README.md">README</a> and <a href="../DESIGN.md">DESIGN.md</a> (the authorities &mdash; this map only points)
</footer>

<div id="tip" role="tooltip"></div>
<div id="toast"></div>

<script>
const MAP = %%DATA%%;
const $ = (s, el) => (el || document).querySelector(s);
const $$ = (s, el) => Array.from((el || document).querySelectorAll(s));

/* ---------- persisted settings ---------- */
const LS = k => "mini-kafka-map:" + k;
const schemeEl = $("#scheme"), rootEl = $("#root");
schemeEl.value = localStorage.getItem(LS("scheme")) || "vscode";
rootEl.value = localStorage.getItem(LS("root")) || MAP.root;
schemeEl.onchange = () => localStorage.setItem(LS("scheme"), schemeEl.value);
rootEl.onchange = () => localStorage.setItem(LS("root"), rootEl.value);
const compactBtn = $("#compact"), repairsBtn = $("#repairs");
function setCompact(on) {
  document.body.classList.toggle("compact", on);
  compactBtn.setAttribute("aria-pressed", on);
  localStorage.setItem(LS("compact"), on ? "1" : "");
  drawWires();
}
function setRepairs(on) {
  document.body.classList.toggle("repairs", on);
  repairsBtn.setAttribute("aria-pressed", on);
}
compactBtn.onclick = () => setCompact(!document.body.classList.contains("compact"));
repairsBtn.onclick = () => setRepairs(!document.body.classList.contains("repairs"));
if (localStorage.getItem(LS("compact"))) setCompact(true);

/* ---------- chips: tooltips + editor deep links ---------- */
const tip = $("#tip"), toast = $("#toast");
function sym(key) { return MAP.symbols[key]; }
function pathLine(key) { const s = sym(key); return s.file + ":" + s.line; }
function absPathLine(key) {
  return rootEl.value.replace(/\/$/, "") + "/" + pathLine(key);
}
function showToast(msg) {
  toast.textContent = msg; toast.classList.add("show");
  clearTimeout(showToast.t); showToast.t = setTimeout(() => toast.classList.remove("show"), 1600);
}
function copyText(text) {
  if (navigator.clipboard && window.isSecureContext) {
    navigator.clipboard.writeText(text).then(() => showToast("copied " + text));
  } else {
    const ta = document.createElement("textarea");
    ta.value = text; document.body.appendChild(ta); ta.select();
    try { document.execCommand("copy"); showToast("copied " + text); }
    catch (e) { showToast("copy failed — " + text); }
    ta.remove();
  }
}
function openKey(key, alt) {
  const scheme = schemeEl.value;
  if (alt || scheme === "copy") { copyText(absPathLine(key)); return; }
  location.href = scheme + "://file/" + absPathLine(key);
}
document.addEventListener("click", e => {
  const c = e.target.closest(".chip");
  if (!c) return;
  openKey(c.dataset.key, e.altKey);
});
document.addEventListener("mouseover", e => {
  const c = e.target.closest(".chip");
  if (!c) { tip.style.display = "none"; return; }
  const s = sym(c.dataset.key);
  const doc = s.doc
    ? '<div class="tdoc">' + escapeHtml(s.doc) + "</div>"
    : '<div class="tdoc none">no doc-comment</div>';
  tip.innerHTML = doc + '<div class="tpath">' + escapeHtml(pathLine(c.dataset.key)) +
    '</div><div class="thint">click opens in editor · ⌥-click copies path:line</div>';
  tip.style.display = "block";
  const r = c.getBoundingClientRect(), tw = tip.offsetWidth, th = tip.offsetHeight;
  let x = r.left, y = r.bottom + 8;
  if (x + tw > innerWidth - 12) x = innerWidth - tw - 12;
  if (y + th > innerHeight - 12) y = r.top - th - 8;
  tip.style.left = x + "px"; tip.style.top = y + "px";
});
function escapeHtml(s) {
  return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
}

/* ---------- wires ---------- */
const map = $("#map"), svg = $("#wires");
function rectOf(id) {
  const el = $("#card-" + id), m = map.getBoundingClientRect(), r = el.getBoundingClientRect();
  return { x: r.left - m.left, y: r.top - m.top, w: r.width, h: r.height,
           cx: r.left - m.left + r.width / 2, cy: r.top - m.top + r.height / 2,
           right: r.left - m.left + r.width, bottom: r.top - m.top + r.height };
}
function rowBounds(row) {
  const cards = $$(".card").filter(c => c.style.gridRow == row);
  const m = map.getBoundingClientRect();
  let top = 1e9, bottom = 0;
  for (const c of cards) {
    const r = c.getBoundingClientRect();
    top = Math.min(top, r.top - m.top); bottom = Math.max(bottom, r.bottom - m.top);
  }
  return { top, bottom };
}
function elbow(points, rad) {
  let d = "M" + points[0][0] + " " + points[0][1];
  for (let i = 1; i < points.length - 1; i++) {
    const [px, py] = points[i - 1], [x, y] = points[i], [nx, ny] = points[i + 1];
    const din = Math.hypot(x - px, y - py), dout = Math.hypot(nx - x, ny - y);
    const r = Math.min(rad, din / 2, dout / 2);
    const ax = x - Math.sign(x - px) * r, ay = y - Math.sign(y - py) * r;
    const bx = x + Math.sign(nx - x) * r, by = y + Math.sign(ny - y) * r;
    d += " L" + ax + " " + ay + " Q" + x + " " + y + " " + bx + " " + by;
  }
  const last = points[points.length - 1];
  d += " L" + last[0] + " " + last[1];
  return d;
}
function svgEl(tag, attrs) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", tag);
  for (const k in attrs) el.setAttribute(k, attrs[k]);
  return el;
}
function drawWires() {
  svg.innerHTML = "";
  const W = map.clientWidth, H = map.clientHeight;
  svg.setAttribute("viewBox", "0 0 " + W + " " + H);
  const defs = svgEl("defs", {});
  for (const [id, cls] of [["arr-main", "var(--accent)"], ["arr-support", "var(--wirec)"], ["arr-repair", "var(--warn)"]]) {
    const m = svgEl("marker", { id, viewBox: "0 0 10 10", refX: 9, refY: 5,
      markerWidth: 7, markerHeight: 7, orient: "auto-start-reverse" });
    m.appendChild(svgEl("path", { d: "M0 0L10 5L0 10z", fill: cls }));
    defs.appendChild(m);
  }
  svg.appendChild(defs);
  const r1 = rowBounds(1), r2 = rowBounds(2);
  for (const w of MAP.wires) {
    const a = rectOf(w.from), b = rectOf(w.to);
    const g = svgEl("g", { class: "wire " + w.kind, "data-from": w.from, "data-to": w.to, "data-wire": w.id });
    let d = "", badge = null, label = null;
    if (w.route === "h") {
      const dy = w.dy || 0;
      const lr = a.right < b.x;      // left-to-right or right-to-left
      const y = ((a.cy + b.cy) / 2) + dy;
      const x1 = lr ? a.right + 2 : a.x - 2, x2 = lr ? b.x - 4 : b.right + 4;
      d = "M" + x1 + " " + y + " L" + x2 + " " + y;
      const mx = (x1 + x2) / 2;
      badge = [mx, y];
      const rowTop = Math.min(a.y, b.y), rowBot = Math.max(a.bottom, b.bottom);
      if (w.lmode === "above") {
        label = { x: mx, y: rowTop - 24, anchor: "middle" };
        g.appendChild(svgEl("line", { class: "leader", x1: mx, y1: rowTop - 18, x2: mx, y2: y - 12 }));
      } else if (w.lmode === "below") {
        label = { x: mx, y: rowBot + 20, anchor: "middle" };
      } else {
        label = { x: mx, y: y + (w.ldy || -8), anchor: "middle" };
      }
    } else if (w.route === "v") {
      const dx = w.dx || 0, x = a.cx + dx;
      d = "M" + x + " " + (a.bottom + 2) + " L" + x + " " + (b.y - 4);
      label = { x: x + (w.ldx || 8), y: (a.bottom + b.y) / 2 + (w.ldy || 0), anchor: "start" };
    } else if (w.route === "top") {
      const ax = a.cx + (w.adx || 0), bx = b.cx + (w.bdx || 0), ay = w.arcy || 16;
      const pts = [[ax, a.y - 2], [ax, ay], [bx, ay], [bx, b.y - 4]];
      d = elbow(pts, 10);
      const lx = bx + (ax - bx) * (1 - (w.lfrac || .5));
      badge = [(ax + bx) / 2, ay];
      label = { x: lx, y: ay - 9, anchor: "middle" };
    } else if (w.route === "chan" || w.route === "below") {
      const chY = w.route === "below" ? r2.bottom + (w.cy || 20)
                                      : r1.bottom + (w.cy || 30);
      const ax = a.cx + (w.adx || 0), bx = b.cx + (w.bdx || 0);
      const aY = w.exit === "top" ? a.y - 2 : a.bottom + 2;
      const bY = w.enter === "top" ? b.y - 4 : b.bottom + 4;
      const pts = [[ax, aY], [ax, chY], [bx, chY], [bx, bY]];
      d = elbow(pts, 10);
      label = { x: (ax + bx) / 2 + (w.ldx || 0), y: chY + (w.ldy || -7), anchor: "middle" };
    }
    const p = svgEl("path", { d, "marker-end": "url(#arr-" + w.kind + ")" });
    g.appendChild(p);
    if (label) {
      const t = svgEl("text", { class: "wlabel", x: Math.max(46, Math.min(W - 46, label.x)),
        y: label.y, "text-anchor": label.anchor });
      t.textContent = w.label;
      g.appendChild(t);
    }
    if (badge && w.num) {
      const bg = svgEl("g", { class: "badge" });
      bg.appendChild(svgEl("circle", { cx: badge[0], cy: badge[1], r: 9.5 }));
      const tx = svgEl("text", { x: badge[0], y: badge[1] + 3.8 });
      tx.textContent = w.num;
      bg.appendChild(tx);
      g.appendChild(bg);
    }
    svg.appendChild(g);
  }
}
addEventListener("resize", () => { clearTimeout(drawWires.t); drawWires.t = setTimeout(drawWires, 120); });

/* ---------- hover isolation ---------- */
$$(".card").forEach(card => {
  card.addEventListener("mouseenter", () => {
    const id = card.dataset.card;
    document.body.classList.add("isolating");
    card.classList.add("lit");
    $$("#wires .wire").forEach(w => {
      if (w.dataset.from === id || w.dataset.to === id) {
        w.classList.add("lit");
        const other = w.dataset.from === id ? w.dataset.to : w.dataset.from;
        const oc = $("#card-" + other); if (oc) oc.classList.add("lit");
      }
    });
  });
  card.addEventListener("mouseleave", () => {
    document.body.classList.remove("isolating");
    $$(".card.lit").forEach(c => c.classList.remove("lit"));
    $$("#wires .wire.lit").forEach(w => w.classList.remove("lit"));
  });
});

/* ---------- story strip ---------- */
$$("#story .stop").forEach(s => s.addEventListener("click", () => flashCard(s.dataset.card)));
function flashCard(id) {
  const el = $("#card-" + id);
  el.scrollIntoView({ behavior: "smooth", block: "center" });
  el.classList.add("ring");
  setTimeout(() => { if (!tour.active()) el.classList.remove("ring"); }, 1600);
}

/* ---------- find ---------- */
const findEl = $("#find"), hitsEl = $("#hits");
findEl.addEventListener("input", () => {
  const q = findEl.value.trim().toLowerCase();
  if (!q) { clearFind(); return; }
  document.body.classList.add("finding");
  let n = 0;
  $$("#map .chip").forEach(c => {
    const s = sym(c.dataset.key);
    const hay = (s.name + " " + s.file + " " + (s.doc || "")).toLowerCase();
    const hit = hay.includes(q);
    c.classList.toggle("dimmed", !hit);
    if (hit) n++;
  });
  $$(".card").forEach(card => {
    const anyChip = $$(".chip:not(.dimmed)", card).length > 0;
    const own = (card.textContent || "").toLowerCase().includes(q);
    card.classList.toggle("dimmed", !(anyChip || own));
  });
  hitsEl.textContent = n + " hit" + (n === 1 ? "" : "s");
});
function clearFind() {
  document.body.classList.remove("finding");
  findEl.value = ""; hitsEl.textContent = "";
  $$(".dimmed").forEach(el => el.classList.remove("dimmed"));
}
findEl.addEventListener("keydown", e => {
  if (e.key === "Enter") {
    const first = $("#map .chip:not(.dimmed)");
    if (first && document.body.classList.contains("finding")) {
      first.scrollIntoView({ behavior: "smooth", block: "center" });
      first.classList.add("flash"); setTimeout(() => first.classList.remove("flash"), 1500);
    }
  } else if (e.key === "Escape") { clearFind(); findEl.blur(); }
});

/* ---------- journeys ---------- */
const jsel = $("#jsel"), jprev = $("#jprev"), jnext = $("#jnext"),
      stepno = $("#stepno"), jchain = $("#jchain"), jnarr = $("#jnarr"),
      jproves = $("#jproves");
const tour = {
  j: null, i: 0,
  active() { return !!this.j; },
  start(id) {
    this.j = MAP.journeys.find(x => x.id === id) || null;
    this.i = 0;
    if (!this.j) { this.end(); return; }
    document.body.classList.add("touring");
    jproves.innerHTML = "<b>what this story proves:</b> " + escapeHtml(this.j.proves);
    jchain.innerHTML = "";
    this.j.steps.forEach((st, k) => {
      const s = sym(st.key);
      const el = document.createElement("button");
      el.className = "jstep";
      el.innerHTML = '<span class="n">' + (k + 1) + '</span><span class="sym">' +
        escapeHtml(s.name) + '</span><span class="mod">' +
        escapeHtml(s.file.split("/").slice(-1)[0]) + "</span>";
      el.onclick = () => { this.i = k; this.show(); };
      jchain.appendChild(el);
    });
    jprev.disabled = jnext.disabled = false;
    this.show();
  },
  show() {
    const st = this.j.steps[this.i], s = sym(st.key);
    $$(".jstep", jchain).forEach((el, k) => el.classList.toggle("cur", k === this.i));
    const cur = $$(".jstep", jchain)[this.i];
    if (cur) cur.scrollIntoView({ inline: "center", block: "nearest", behavior: "smooth" });
    jnarr.innerHTML = escapeHtml(st.text) +
      '<a href="#" id="jopen">' + escapeHtml(pathLine(st.key)) + "</a>";
    $("#jopen").onclick = e => { e.preventDefault(); openKey(st.key, e.altKey); };
    stepno.textContent = (this.i + 1) + " / " + this.j.steps.length;
    $$(".chip.stepped").forEach(c => c.classList.remove("stepped"));
    $$(".card.ring").forEach(c => c.classList.remove("ring"));
    const chipEl = $('#map .chip[data-key="' + CSS.escape(st.key) + '"]');
    if (chipEl) {
      chipEl.classList.add("stepped");
      const card = chipEl.closest(".card");
      card.classList.add("ring");
      chipEl.scrollIntoView({ behavior: "smooth", block: "center" });
    }
  },
  next() { if (this.j && this.i < this.j.steps.length - 1) { this.i++; this.show(); } },
  prev() { if (this.j && this.i > 0) { this.i--; this.show(); } },
  end() {
    this.j = null;
    document.body.classList.remove("touring");
    jsel.value = ""; stepno.textContent = "";
    jprev.disabled = jnext.disabled = true;
    $$(".chip.stepped").forEach(c => c.classList.remove("stepped"));
    $$(".card.ring").forEach(c => c.classList.remove("ring"));
  },
};
jsel.onchange = () => { tour.start(jsel.value); jsel.blur(); };
jprev.onclick = () => tour.prev();
jnext.onclick = () => tour.next();
document.addEventListener("keydown", e => {
  const t = e.target;
  if (t && t.matches && t.matches("input,select,textarea")) return;
  if (tour.active()) {
    if (e.key === "ArrowRight") { tour.next(); e.preventDefault(); }
    else if (e.key === "ArrowLeft") { tour.prev(); e.preventDefault(); }
    else if (e.key === "Escape") tour.end();
  } else if (e.key === "Escape") clearFind();
});

drawWires();
setTimeout(drawWires, 200);  // fonts settle
</script>
</body>
</html>
"""


def main():
    symbols, errors = resolve_symbols()
    errors = self_check(symbols, errors)
    if errors:
        print("code-map self-check FAILED; not writing output:", file=sys.stderr)
        for e in errors:
            print("  - " + e, file=sys.stderr)
        sys.exit(1)

    commit = git("rev-parse", "--short", "HEAD") or "uncommitted"
    mapped_files = sorted({s["file"] for s in symbols.values()})
    dirty = bool(git("status", "--porcelain", "--", *mapped_files))

    html = build_html(symbols, commit, dirty, len(symbols))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(html)
    print(f"wrote {OUT.relative_to(REPO)}")
    print(f"verified {len(symbols)}/{len(symbols)} anchors at commit {commit}"
          + (" (dirty)" if dirty else ""))


if __name__ == "__main__":
    main()

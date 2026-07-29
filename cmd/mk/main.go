// Command mk is the mini-kafka CLI (D-SL0-7): create-topic, topics,
// produce, consume — thin subcommands over the public client package.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/systemcnu/mini-kafka/client"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	var err error
	switch os.Args[1] {
	case "create-topic":
		err = cmdCreateTopic(os.Args[2:])
	case "topics":
		err = cmdTopics(os.Args[2:])
	case "produce":
		err = cmdProduce(os.Args[2:])
	case "consume":
		err = cmdConsume(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "mk %s: %v\n", os.Args[1], err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: mk <command> [flags]

commands:
  create-topic -t <topic> -p <partitions> [-addr host:port]
  topics       [-addr host:port]
  produce      -t <topic> -p <partition> [-m <msg>] [-addr host:port]
               (without -m, one message per stdin line)
  consume      -t <topic> -p <partition> [-o <offset>] [-f] [-addr host:port]
  consume      -t <topic> -g <group> [-addr host:port]
               (group mode: assignment comes from the group; polls forever,
               prints partition<TAB>offset<TAB>payload, commits per batch)`)
}

func cmdCreateTopic(args []string) error {
	fs := flag.NewFlagSet("create-topic", flag.ExitOnError)
	addr := fs.String("addr", client.DefaultAddr, "broker address")
	topic := fs.String("t", "", "topic name")
	partitions := fs.Uint("p", 1, "partition count (1..16)")
	fs.Parse(args)
	if *topic == "" {
		return fmt.Errorf("-t is required")
	}
	a, err := client.DialAdmin(*addr)
	if err != nil {
		return err
	}
	defer a.Close()
	if err := a.CreateTopic(*topic, uint32(*partitions)); err != nil {
		return err
	}
	fmt.Printf("created topic %s with %d partition(s)\n", *topic, *partitions)
	return nil
}

func cmdTopics(args []string) error {
	fs := flag.NewFlagSet("topics", flag.ExitOnError)
	addr := fs.String("addr", client.DefaultAddr, "broker address")
	fs.Parse(args)
	a, err := client.DialAdmin(*addr)
	if err != nil {
		return err
	}
	defer a.Close()
	topics, err := a.Topics()
	if err != nil {
		return err
	}
	for _, t := range topics {
		fmt.Printf("%s\t%d\n", t.Name, t.Partitions)
	}
	return nil
}

func cmdProduce(args []string) error {
	fs := flag.NewFlagSet("produce", flag.ExitOnError)
	addr := fs.String("addr", client.DefaultAddr, "broker address")
	topic := fs.String("t", "", "topic name")
	partition := fs.Uint("p", 0, "partition")
	msg := fs.String("m", "", "message; omit to read one message per stdin line")
	fs.Parse(args)
	if *topic == "" {
		return fmt.Errorf("-t is required")
	}
	p, err := client.DialProducer(*addr)
	if err != nil {
		return err
	}
	defer p.Close()

	produceOne := func(payload []byte) error {
		off, err := p.Produce(*topic, uint32(*partition), payload)
		if err != nil {
			return err
		}
		fmt.Println(off)
		return nil
	}
	if *msg != "" {
		return produceOne([]byte(*msg))
	}
	sc := bufio.NewScanner(os.Stdin)
	// Payloads may legally reach 1 MiB; the default Scanner cap is 64 KiB.
	sc.Buffer(make([]byte, 0, 64*1024), 1<<20+1024)
	for sc.Scan() {
		if err := produceOne(append([]byte(nil), sc.Bytes()...)); err != nil {
			return err
		}
	}
	return sc.Err()
}

func cmdConsume(args []string) error {
	fs := flag.NewFlagSet("consume", flag.ExitOnError)
	addr := fs.String("addr", client.DefaultAddr, "broker address")
	topic := fs.String("t", "", "topic name")
	partition := fs.Uint("p", 0, "partition")
	offset := fs.Uint64("o", 0, "start offset")
	follow := fs.Bool("f", false, "long-poll for new records instead of exiting at the tail")
	group := fs.String("g", "", "consumer group; assignment comes from the group (excludes -p/-o)")
	fs.Parse(args)
	if *topic == "" {
		return fmt.Errorf("-t is required")
	}
	if *group != "" {
		// -p/-o would be silently meaningless with -g (the group assigns
		// partitions and positions): reject the combination loudly.
		var conflict string
		fs.Visit(func(f *flag.Flag) {
			if f.Name == "p" || f.Name == "o" {
				conflict = f.Name
			}
		})
		if conflict != "" {
			return fmt.Errorf("-%s cannot be combined with -g: assignment comes from the group", conflict)
		}
		return consumeGroup(*addr, *group, *topic)
	}
	c, err := client.DialConsumer(*addr)
	if err != nil {
		return err
	}
	defer c.Close()

	off := *offset
	for {
		// Non-follow uses a 1 ms wait so hitting the tail returns at once;
		// follow long-polls with the broker default-sized wait.
		maxWait := uint32(1)
		if *follow {
			maxWait = 5000
		}
		recs, err := c.Fetch(*topic, uint32(*partition), off, maxWait, 0)
		if err != nil {
			return err
		}
		for _, r := range recs {
			fmt.Printf("%d\t%s\n", r.Offset, r.Payload)
			off = r.Offset + 1
		}
		if len(recs) == 0 && !*follow {
			return nil
		}
	}
}

// consumeGroup is the SLICES demo surface: join the group, poll forever,
// print each record, commit per batch. Fencing errors surface verbatim
// (D-SL2-8/10: no auto-heal before surfacing) — but only UNKNOWN_MEMBER is
// terminal: the member's identity is gone (SD-11's zombie). STALE_GENERATION
// on a commit is a LIVE member racing a routine rebalance; the surfaced line
// is printed and the loop continues — the next Poll re-joins (D-SL2-8) —
// otherwise two members could never coexist through each other's joins.
func consumeGroup(addr, group, topic string) error {
	gc, err := client.JoinGroup(addr, group, topic)
	if err != nil {
		return err
	}
	defer gc.Close()
	for {
		recs, err := gc.Poll(5 * time.Second)
		if err != nil {
			return err
		}
		for _, r := range recs {
			fmt.Printf("%d\t%d\t%s\n", r.Partition, r.Offset, r.Payload)
		}
		if len(recs) > 0 {
			if err := gc.Commit(); err != nil {
				var ce *client.Error
				if errors.As(err, &ce) && ce.Code == client.CodeStaleGeneration {
					fmt.Fprintf(os.Stderr, "mk consume: %v (rejoining)\n", err)
					continue
				}
				return err
			}
		}
	}
}

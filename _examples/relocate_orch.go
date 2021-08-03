package main

// Node - Worker
// Key - Job definition
import (
	"fmt"
	"math/rand"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type Member string

func (m Member) String() string {
	return string(m)
}

// consistent package doesn't provide a default hashing function.
// You should provide a proper one to distribute keys/members uniformly.
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	// you should use a proper hash function for uniformity.
	return xxhash.Sum64(data)
}

func main() {

	// Create a new consistent instance.
	members := []consistent.Member{}
	for i := 0; i < 8; i++ {
		member := Member(fmt.Sprintf("Worker%d", i))
		members = append(members, member)
	}

	cfg := consistent.Config{
		PartitionCount:    1190611,
		ReplicationFactor: 20,
		Load:              1.25,
		Hasher:            hasher{},
	}
	c := consistent.New(members, cfg)

	// Create new job definitions, hash them and find their owner
	for j := 0; j < 10; j++ {
		fmt.Printf("---New job %d----\n", j)
		jd := fmt.Sprintf("job-definition-%d", rand.Intn(10000000))
		key := []byte(jd)
		// calculates partition id for the given key
		// partID := hash(key) % partitionCount
		// the partitions is already distributed among members by Add function.
		for i := 0; i < 1; i++ {
			owner := c.LocateKey(key)
			fmt.Println(owner.String())
			// Prints node2.olricmq.com
		}
	}

	// Store current layout of partitions
	owners := make(map[int]string)
	for partID := 0; partID < cfg.PartitionCount; partID++ {
		owners[partID] = c.GetPartitionOwner(partID).String()
	}

	// Add a new member
	m := Member(fmt.Sprintf("Worker%d", 9))
	c.Add(m)

	// I don't think parition owners matters other than for observability.
	var changed int
	for partID, member := range owners {
		owner := c.GetPartitionOwner(partID)
		if member != owner.String() {
			changed++
			// fmt.Printf("partID: %3d moved to %s from %s\n", partID, owner.String(), member)
		}
	}
	fmt.Printf("\n%d%% of the partitions are relocated\n", (100*changed)/cfg.PartitionCount)

	// Create new job definitions, hash them and find their owner
	for j := 0; j < 10; j++ {
		fmt.Printf("---New job %d----\n", j)
		jd := fmt.Sprintf("job-definition-%d", rand.Intn(10000000))
		key := []byte(jd)
		// calculates partition id for the given key
		// partID := hash(key) % partitionCount
		// the partitions is already distributed among members by Add function.
		for i := 0; i < 1; i++ {
			owner := c.LocateKey(key)
			fmt.Println(owner.String())
			// Prints node2.olricmq.com
		}
	}
}

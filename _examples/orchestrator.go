package main

// Node - queuer
// Key - Job definition
import (
	"fmt"
	"math/rand"

	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash"
)

type myMember string

func (m myMember) String() string {
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
	cfg := consistent.Config{
		PartitionCount:    271, // TODO: what is a good parition count?
		ReplicationFactor: 20,
		Load:              1.25, // TODO: What is a good load?
		Hasher:            hasher{},
	}
	c := consistent.New(nil, cfg)

	// Add some members to the consistent hash table.
	// Add function calculates average load and distributes partitions over members
	queuer1 := myMember("queuer1")
	c.Add(queuer1)

	queuer2 := myMember("queuer2")
	c.Add(queuer2)

	// Create new job definitions, hash them and find their owner
	for j := 0; j < 10; j++ {
		fmt.Printf("---New job %d----\n", j)
		jd := fmt.Sprintf("job-definition-%d", rand.Intn(10000000))
		key := []byte(jd)
		// calculates partition id for the given key
		// partID := hash(key) % partitionCount
		// the partitions is already distributed among members by Add function.
		for i := 0; i < 10; i++ {
			owner := c.LocateKey(key)
			fmt.Println(owner.String())
			// Update job_definition set owner_queuer = 'owner.string()' where owner_queuer != 'owner.String()';
			// Prints node2.olricmq.com
		}
	}

}

// Each key is a job definition
// Each member is a queuer/queuer
// Paritions independent of key or members.
// SELECT * FROM job_definition WHERE owner_queuer = 'queuer1';
// Read from environment avraible and say that num_workers = 10;
// Relocation would change owner_queuer of job definitions where owners changed.

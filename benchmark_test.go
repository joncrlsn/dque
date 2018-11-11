// benchmark_test.go
package dque_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/joncrlsn/dque"
)

// item3 is the thing we'll be storing in the queue
type item3 struct {
	Name     string
	Id       int
	SomeBool bool
}

// item3Builder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func item3Builder() interface{} {
	return &item3{}
}

func BenchmarkEnqueue(b *testing.B) {

	qName := "testBenchEnqueue"

	b.StopTimer()

	// Clean up from a previous run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory", err)
	}

	// Create the queue
	q, err := dque.New(qName, ".", 100, item3Builder)
	if err != nil {
		b.Fatal("Error creating new dque", err)
	}
	b.StartTimer()

	start := time.Now()
	for n := 0; n < b.N; n++ {
		err := q.Enqueue(item3{"Short Name", n, true})
		if err != nil {
			b.Fatal("Error enqueuing to dque", err)
		}
	}

	elapsed := time.Now().Sub(start)
	fmt.Printf("Elapsed time to enqueue %d items: %v\n", b.N, elapsed)

	// Clean up from the run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory for BenchmarkDequeue", err)
	}
}

func BenchmarkDequeue(b *testing.B) {

	qName := "testBenchDequeue"

	b.StopTimer()

	// Clean up from a previous run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory", err)
	}

	// Create the queue
	q, err := dque.New(qName, ".", 100, item3Builder)
	if err != nil {
		b.Fatal("Error creating new dque", err)
	}

	for i := 0; i < 6000; i++ {
		err := q.Enqueue(item3{"Sorta, kind of, a Big Long Name", i, true})
		if err != nil {
			b.Fatal("Error enqueuing to dque", err)
		}
	}
	b.StartTimer()

	start := time.Now()
	for n := 0; n < b.N; n++ {
		_, err := q.Dequeue()
		if err != nil {
			b.Fatal("Error dequeuing from dque", err)
		}
	}

	elapsed := time.Now().Sub(start)
	fmt.Printf("Elapsed time to dequeue %d items: %v\n", b.N, elapsed)

	// Clean up from the run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory for BenchmarkDequeue", err)
	}
}

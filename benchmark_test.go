// benchmark_test.go
package dque_test

import (
	"os"
	"testing"

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

func BenchmarkEnqueue_Safe(b *testing.B) {
	benchmarkEnqueue(b, false /* true=turbo */)
}
func BenchmarkEnqueue_Turbo(b *testing.B) {
	benchmarkEnqueue(b, true /* true=turbo */)
}

func benchmarkEnqueue(b *testing.B, turbo bool) {

	qName := "testBenchEnqueue"

	b.StopTimer()

	// Clean up from a previous run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory:", err)
	}

	// Create the queue
	q, err := dque.New(qName, ".", 100, item3Builder)
	if err != nil {
		b.Fatal("Error creating new dque:", err)
	}
	if turbo {
		q.TurboOn()
	}
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		err := q.Enqueue(item3{"Short Name", n, true})
		if err != nil {
			b.Fatal("Error enqueuing to dque:", err)
		}
	}

	// Clean up from the run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory for BenchmarkDequeue:", err)
	}
}

func BenchmarkDequeue_Safe(b *testing.B) {
	benchmarkDequeue(b, false /* true=turbo */)
}
func BenchmarkDequeue_Turbo(b *testing.B) {
	benchmarkDequeue(b, true /* true=turbo */)
}

func benchmarkDequeue(b *testing.B, turbo bool) {

	qName := "testBenchDequeue"

	b.StopTimer()

	// Clean up from a previous run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory:", err)
	}

	// Create the queue
	q, err := dque.New(qName, ".", 100, item3Builder)
	if err != nil {
		b.Fatal("Error creating new dque", err)
	}
	var iterations int = 5000
	if turbo {
		q.TurboOn()
		iterations = iterations * 10
	}

	for i := 0; i < iterations; i++ {
		err := q.Enqueue(item3{"Sorta, kind of, a Big Long Name", i, true})
		if err != nil {
			b.Fatal("Error enqueuing to dque:", err)
		}
	}
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		_, err := q.Dequeue()
		if err != nil {
			b.Fatal("Error dequeuing from dque:", err)
		}
	}

	// Clean up from the run
	if err := os.RemoveAll(qName); err != nil {
		b.Fatal("Error removing queue directory for BenchmarkDequeue", err)
	}
}

// queue_test.go
package dque_test

import (
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/joncrlsn/dque"
)

// item2 is the thing we'll be storing in the queue
type item2 struct {
	Id int
}

// item2Builder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func item2Builder() interface{} {
	return &item2{}
}

func itemsGen(from, to int) []interface{} {
	result := make([]interface{}, to-from)
	for i := 0; i < to-from; i++ {
		result[i] = &item2{from + i}
	}
	return result
}

// Test prepend at various stages of execution
func TestQueue_PrependLoop(t *testing.T) {
	testQueue_PrependLoop(t, true /* true=turbo */)
	testQueue_PrependLoop(t, false /* true=turbo */)
}

func testQueue_PrependLoop(t *testing.T, turbo bool) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create a new queue with segment size of 3
	q := newQ(t, qName, turbo)

	for i := 0; i < 4; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
	}
	if err := q.Prepend(itemsGen(4, 10)); err != nil {
		t.Fatal("Error prepending", err)
	}
	checkQueue(t, q, []int{4, 5, 6, 7, 8, 9, 0, 1})
	if err := q.Prepend(itemsGen(10, 15)); err != nil {
		t.Fatal("Error prepending", err)
	}
	checkQueue(t, q, []int{10, 11, 12, 13})
	for i := 15; i < 20; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
	}
	checkQueue(t, q, []int{14, 2, 3, 15, 16, 17, 18, 19})
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

func checkQueue(t *testing.T, q *dque.DQue, values []int) {
	for _, i := range values {
		obj, err := q.Dequeue()
		if err != nil {
			t.Fatal("Unable to get element", err)
		}
		i2, ok := obj.(*item2)
		if !ok {
			t.Fatalf("Invalid object type %v", obj)
		}
		if i2.Id != i {
			t.Fatalf("Got wrong element (should be %d): %v", i, i2)
		}
	}
}

// Adds 1 and removes 1 in a loop to ensure that when we've filled
// up the first segment that we delete it and move on to the next segment
func TestQueue_AddRemoveLoop(t *testing.T) {
	testQueue_AddRemoveLoop(t, true /* true=turbo */)
	testQueue_AddRemoveLoop(t, false /* true=turbo */)
}

func testQueue_AddRemoveLoop(t *testing.T, turbo bool) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create a new queue with segment size of 3
	var err error
	q := newQ(t, qName, turbo)

	for i := 0; i < 4; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
		_, err = q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing", err)
		}
	}

	assert(t, 0 == q.Size(), "Size is not 0")

	firstSegNum, lastSegNum := q.SegmentNumbers()

	// Assert that we have just one segment
	assert(t, firstSegNum == lastSegNum, "The first segment must match the last")

	// Assert that the first segment is #2
	assert(t, 2 == firstSegNum, "The first segment is not 2")

	// Now reopen the queue and check our assertions again.
	q.Close()
	q = openQ(t, qName, turbo)

	firstSegNum, lastSegNum = q.SegmentNumbers()

	// Assert that we have just one segment
	assert(t, firstSegNum == lastSegNum, "After opening, the first segment must match the second")

	// Assert that the first segment is #2
	assert(t, 2 == firstSegNum, "After opening, the first segment is not 2")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

// Adds 2 and removes 1 in a loop to ensure that when we've filled
// up the first segment that we delete it and move on to the next segment
func TestQueue_Add2Remove1(t *testing.T) {
	testQueue_Add2Remove1(t, true /* true=turbo */)
	testQueue_Add2Remove1(t, false /* true=turbo */)
}
func testQueue_Add2Remove1(t *testing.T, turbo bool) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create a new queue with segment size of 3
	var err error
	q := newQ(t, qName, turbo)

	// Add 2 and remove one each loop
	for i := 0; i < 4; i = i + 2 {
		var item interface{}
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
		if err := q.Enqueue(&item2{i + 1}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
		item, err = q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing", err)
		}
		assert(t, item != nil, "Item is nil")
	}

	firstSegNum, lastSegNum := q.SegmentNumbers()

	// Assert that we have more than one segment
	assert(t, firstSegNum < lastSegNum, "The first segment cannot match the second")

	// Assert that the first segment is #2
	assert(t, 2 == lastSegNum, "The last segment must be 2")

	// Now reopen the queue and check our assertions again.
	q.Close()
	q = openQ(t, qName, turbo)

	firstSegNum, lastSegNum = q.SegmentNumbers()

	// Assert that we have more than one segment
	assert(t, firstSegNum < lastSegNum, "After opening, the first segment can not match the second")

	// Assert that the first segment is #2
	assert(t, 2 == lastSegNum, "After opening, the last segment must be 2")

	// Test Peek to make sure the size doesn't change
	assert(t, 2 == q.Size(), "Queue size is not 2 before peeking")
	obj, err := q.Peek()
	if err != nil {
		t.Fatal("Error peeking at the queue", err)
	}

	assert(t, 2 == q.Size(), "After peaking, aueue size must still be 2")
	assert(t, obj != nil, "Peeked object must not be nil.")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

// Adds 9 and removes 8
func TestQueue_Add9Remove8(t *testing.T) {
	testQueue_Add9Remove8(t, true /* true = turbo */)
	testQueue_Add9Remove8(t, false /* true = turbo */)
}

func testQueue_Add9Remove8(t *testing.T, turbo bool) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create new queue with segment size 3
	q := newQ(t, qName, turbo)

	// Enqueue 9 items
	for i := 0; i < 9; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
	}

	// Check the Size calculation
	assert(t, 9 == q.Size(), "the size is calculated wrong.  Should be 9")

	firstSegNum, lastSegNum := q.SegmentNumbers()

	// Assert that the first segment is #1
	assert(t, 1 == firstSegNum, "the first segment is not 1")

	// Assert that the last segment is #4
	assert(t, 3 == lastSegNum, "the last segment is not 3")

	// Dequeue 8 items
	for i := 0; i < 8; i++ {
		iface, err := q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing:", err)
		}

		// Check the Size calculation
		assert(t, 8-i == q.Size(), "the size is calculated wrong.")
		item, ok := iface.(item2)
		if ok {
			fmt.Printf("Dequeued %T %t %#v\n", item, ok, item)
			assert(t, i == item.Id, "Unexpected itemId")
		} else {
			item, ok := iface.(*item2)
			assert(t, ok, "Dequeued object is not of type *item2")
			assert(t, i == item.Id, "Unexpected itemId")
		}
	}

	firstSegNum, lastSegNum = q.SegmentNumbers()

	// Assert that we have only one segment
	assert(t, firstSegNum == lastSegNum, "The first segment must match the second")

	// Assert that the first segment is #3
	assert(t, 3 == firstSegNum, "The last segment is not 3")

	// Now reopen the queue and check our assertions again.
	q.Close()
	_ = openQ(t, qName, turbo)

	// Assert that we have more than one segment
	assert(t, firstSegNum == lastSegNum, "After opening, the first segment must match the second")

	// Assert that the last segment is #3
	assert(t, 3 == lastSegNum, "After opening, the last segment is not 3")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}
}

func TestQueue_EmptyDequeue(t *testing.T) {
	testQueue_EmptyDequeue(t, true /* true=turbo */)
	testQueue_EmptyDequeue(t, false /* true=turbo */)
}
func testQueue_EmptyDequeue(t *testing.T, turbo bool) {
	qName := "testEmptyDequeue"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	// Create new queue
	q := newQ(t, qName, turbo)
	assert(t, 0 == q.Size(), "Expected an empty queue")

	// Dequeue an item from the empty queue
	item, err := q.Dequeue()
	assert(t, dque.ErrEmpty == err, "Expected an ErrEmpty error")
	assert(t, item == nil, "Expected nil because queue is empty")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}
}

func TestQueue_NewOrOpen(t *testing.T) {
	testQueue_NewOrOpen(t, true /* true=turbo */)
	testQueue_NewOrOpen(t, false /* true=turbo */)
}

func testQueue_NewOrOpen(t *testing.T, turbo bool) {
	qName := "testNewOrOpen"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	// Create new queue with newOrOpen
	q := newOrOpenQ(t, qName, turbo)
	q.Close()

	// Open the same queue with newOrOpen
	q = newOrOpenQ(t, qName, turbo)
	q.Close()

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}
}

func TestQueue_Turbo(t *testing.T) {
	qName := "testNewOrOpen"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	// Create new queue
	q := newQ(t, qName, false)

	if err := q.TurboOff(); err == nil {
		t.Fatal("Expected an error")
	}

	if err := q.TurboSync(); err == nil {
		t.Fatal("Expected an error")
	}

	if err := q.TurboOn(); err != nil {
		t.Fatal("Error turning on turbo:", err)
	}

	if err := q.TurboOn(); err == nil {
		t.Fatal("Expected an error")
	}

	if err := q.TurboSync(); err != nil {
		t.Fatal("Error running TurboSync:", err)
	}

	// Enqueue 1000 items
	start := time.Now()
	for i := 0; i < 1000; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing:", err)
		}
	}
	elapsedTurbo := time.Since(start)

	assert(t, q.Turbo(), "Expected turbo to be on")

	if err := q.TurboOff(); err != nil {
		t.Fatal("Error turning off turbo:", err)
	}

	// Enqueue 1000 items
	start = time.Now()
	for i := 0; i < 1000; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing:", err)
		}
	}
	elapsedSafe := time.Since(start)

	assert(t, elapsedTurbo < elapsedSafe/2, "Turbo time (%v) must be faster than safe mode (%v)", elapsedTurbo, elapsedSafe)

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}
}

func TestQueue_NewFlock(t *testing.T) {
	qName := "testFlock"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}

	// New and Close a DQue properly should work
	q, err := dque.New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating dque:", err)
	}
	err = q.Close()
	if err != nil {
		t.Fatal("Error closing dque:", err)
	}

	// Double-open should fail
	q, err = dque.Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque:", err)
	}
	_, err = dque.Open(qName, ".", 3, item2Builder)
	if err == nil {
		t.Fatal("No error during double-open dque")
	}
	err = q.Close()
	if err != nil {
		t.Fatal("Error closing dque:", err)
	}

	// Double-close should fail
	q, err = dque.Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque:", err)
	}
	err = q.Close()
	if err != nil {
		t.Fatal("Error closing dque:", err)
	}
	err = q.Close()
	if err == nil {
		t.Fatal("No error during double-closing dque")
	}

	// Cleanup
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_UseAfterClose(t *testing.T) {
	qName := "testUseAfterClose"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory:", err)
	}

	q, err := dque.New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating dque:", err)
	}
	err = q.Enqueue(&item2{0})
	if err != nil {
		t.Fatal("Error enqueing item:", err)
	}
	err = q.Close()
	if err != nil {
		t.Fatal("Error closing dque:", err)
	}

	queueClosedError := "queue is closed"

	err = q.Close()
	assert(t, err.Error() == queueClosedError, "Expected error not found", err)

	err = q.Enqueue(&item2{0})
	assert(t, err.Error() == queueClosedError, "Expected error not found", err)

	_, err = q.Dequeue()
	assert(t, err.Error() == queueClosedError, "Expected error not found", err)

	_, err = q.Peek()
	assert(t, err.Error() == queueClosedError, "Expected error not found", err)

	s := q.Size()
	assert(t, s == 0, "Expected error")

	s = q.SizeUnsafe()
	assert(t, s == 0, "Expected error")

	err = q.TurboOn()
	assert(t, err.Error() == queueClosedError, "Expected error not found", err)

	err = q.TurboOff()
	assert(t, err.Error() == queueClosedError, "Expected error not found", err)

	err = q.TurboSync()
	assert(t, err.Error() == queueClosedError, "Expected error not found", err)

	// Cleanup
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_BlockingBehaviour(t *testing.T) {
	qName := "testBlocking"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, false)

	go func() {
		err := q.Enqueue(&item2{0})
		assert(t, err == nil, "Expected no error")
	}()

	x, err := q.PeekBlock()
	assert(t, err == nil, "Expected no error")
	assert(t, x != nil, "Item is nil")

	x, err = q.DequeueBlock()
	assert(t, err == nil, "Expected no error")
	assert(t, x != nil, "Item is nil")

	x, err = q.Dequeue()
	assert(t, err == dque.ErrEmpty, "Expected error not found")

	timeout := time.After(3 * time.Second)
	done := make(chan bool)
	go func() {
		x, err = q.DequeueBlock()
		assert(t, err == nil, "Expected no error")
		assert(t, x != nil, "Item is nil")
		done <- true
	}()

	go func() {
		time.Sleep(1 * time.Second)
		err := q.Enqueue(&item2{2})
		assert(t, err == nil, "Expected no error")
	}()

	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

	// Cleanup
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_BlockingWithClose(t *testing.T) {
	qName := "testBlockingWithClose"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, false)

	go func() {
		time.Sleep(1 * time.Second)
		err := q.Close()
		assert(t, err == nil, "Expected no error")
	}()

	timeout := time.After(3 * time.Second)
	done := make(chan bool)
	go func() {
		// The queue is empty,
		// so DequeueBlock should really block and wait,
		// until the other goroutine calls Close,
		// and the Close should wake-up this DequeueBlock block,
		// and return an error because the queue is now closed.
		_, err := q.DequeueBlock()
		assert(t, err == dque.ErrQueueClosed, "Expected ErrQueueClosed error")
		done <- true
	}()

	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

	// Cleanup
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func TestQueue_BlockingAggresive(t *testing.T) {
	rand.Seed(0) // ensure we have reproducible sleeps

	qName := "testBlockingAggresive"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}

	q := newQ(t, qName, false)

	numProducers := 5
	numItemsPerProducer := 50
	numConsumers := 25

	done := make(chan bool)
	var wg sync.WaitGroup
	wg.Add(numProducers * numItemsPerProducer)

	go func() {
		wg.Wait()
		q.Close()
		done <- true
	}()

	// producers
	for p := 0; p < numProducers; p++ {
		go func(producer int) {
			for i := 0; i < numItemsPerProducer; i++ {
				s := rand.Intn(150)
				time.Sleep(time.Duration(s) * time.Millisecond)
				err := q.Enqueue(&item2{i})
				assert(t, err == nil, "Expected no error", err)
				fmt.Println("Enqueued item", i, "by producer", producer, "after sleeping", s)
			}
		}(p)
	}

	// consumers
	for c := 0; c < numConsumers; c++ {
		go func(consumer int) {
			for {
				x, err := q.DequeueBlock()
				if err == dque.ErrQueueClosed {
					return
				}
				assert(t, err == nil, "Expected no error")
				fmt.Println("Dequeued item", x, "by consumer", consumer)
				wg.Done()
			}
		}(c)
	}

	timeout := time.After(10 * time.Second)
	select {
	case <-timeout:
		t.Fatal("Test didn't finish in time")
	case <-done:
	}

	// Cleanup
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory:", err)
	}
}

func newOrOpenQ(t *testing.T, qName string, turbo bool) *dque.DQue {
	// Create a new segment with segment size of 3
	q, err := dque.NewOrOpen(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating or opening dque:", err)
	}

	if turbo {
		_ = q.TurboOn()
	}
	return q
}

func newQ(t *testing.T, qName string, turbo bool) *dque.DQue {
	// Create a new segment with segment size of 3
	q, err := dque.New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating new dque:", err)
	}
	if turbo {
		_ = q.TurboOn()
	}
	return q
}

func openQ(t *testing.T, qName string, turbo bool) *dque.DQue {
	// Open an existing segment with segment size of 3
	q, err := dque.Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque:", err)
	}
	if turbo {
		_ = q.TurboOn()
	}
	return q
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

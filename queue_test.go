// queue_test.go
package dque_test

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

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

// Adds 1 and removes 1 in a loop to ensure that when we've filled
// up the first segment that we delete it and move on to the next segment
func TestQueue_AddRemoveLoop(t *testing.T) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create a new queue with segment size of 3
	var err error
	q := newQ(t, qName)

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
	q = openQ(t, qName)

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
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create a new queue with segment size of 3
	var err error
	q := newQ(t, qName)

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
	assert(t, 2 == lastSegNum, "The last segment is not 2")

	// Now reopen the queue and check our assertions again.
	q = openQ(t, qName)

	firstSegNum, lastSegNum = q.SegmentNumbers()

	// Assert that we have more than one segment
	assert(t, firstSegNum < lastSegNum, "After opening, the first segment can not match the second")

	// Assert that the first segment is #2
	assert(t, 2 == lastSegNum, "After opening, the last segment must be 2")

	// Test Peek to make sure the size doesn't change
	assert(t, 2 == q.Size(), "Queue size is not 2 before peeking")
	obj, err := q.Peek()
	assert(t, 2 == q.Size(), "Queue size is not 2 after peeking")
	assert(t, obj != nil, "Object is nil")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

// Adds 7 and removes 6
func TestQueue_Add7Remove6(t *testing.T) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create new queue with segment size 3
	q := newQ(t, qName)

	// Enqueue 7 items
	for i := 0; i < 7; i++ {
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
	}

	// Check the Size calculation
	assert(t, 7 == q.Size(), "the size is calculated wrong.  Should be 7")

	firstSegNum, lastSegNum := q.SegmentNumbers()

	// Assert that the first segment is #3
	assert(t, 1 == firstSegNum, "the first segment is not 1")

	// Assert that the last segment is #3
	assert(t, 3 == lastSegNum, "the last segment is not 3")

	// Dequeue 6 items
	for i := 0; i < 6; i++ {
		iface, err := q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing", err)
		}

		// Check the Size calculation
		assert(t, 6-i == q.Size(), "the size is calculated wrong.")
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
	q = openQ(t, qName)

	// Assert that we have more than one segment
	assert(t, firstSegNum == lastSegNum, "After opening, the first segment must match the second")

	// Assert that the last segment is #3
	assert(t, 3 == lastSegNum, "After opening, the last segment is not 3")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

func TestQueue_EmptyDequeue(t *testing.T) {
	qName := "testEmptyDequeue"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create new queue
	q := newQ(t, qName)
	assert(t, 0 == q.Size(), "Expected an empty queue")

	// Dequeue an item from the empty queue
	item, err := q.Dequeue()
	assert(t, dque.EMPTY == err, "Expected an EMPTY error")
	assert(t, item == nil, "Expected nil because queue is empty")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

func TestQueue_NewOrOpen(t *testing.T) {
	qName := "testNewOrOpen"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create new queue
	newOrOpenQ(t, qName)

	// Open the same queue
	newOrOpenQ(t, qName)

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

func newOrOpenQ(t *testing.T, qName string) *dque.DQue {
	// Create a new segment with segment size of 3
	q, err := dque.NewOrOpen(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating or opening dque", err)
	}
	return q
}

func newQ(t *testing.T, qName string) *dque.DQue {
	// Create a new segment with segment size of 3
	q, err := dque.New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating new dque", err)
	}
	return q
}

func openQ(t *testing.T, qName string) *dque.DQue {
	// Open an existing segment with segment size of 3
	q, err := dque.Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque", err)
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

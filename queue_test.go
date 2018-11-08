// queue_test.go
package dque

import (
	"fmt"
	"os"
	"testing"

	"github.com/stvp/assert"
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
		var item interface{}
		if err := q.Enqueue(&item2{i}); err != nil {
			t.Fatal("Error enqueueing", err)
		}
		item, err = q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing", err)
		}
		fmt.Printf("Dequeued %#v:", item)
	}

	// Assert that we have just one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "The first segment must match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.firstSegment.number, "The first segment is not 2")

	// Now reopen the queue and check our assertions again.
	q = openQ(t, qName)

	// Assert that we have just one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "After opening, the first segment must match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.firstSegment.number, "After opening, the first segment is not 2")

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
		fmt.Println("Dequeued:", item)
	}

	// Assert that we have more than one segment
	assert.NotEqual(t, q.firstSegment, q.lastSegment, "The first segment cannot match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.lastSegment.number, "The last segment is not 2")

	// Now reopen the queue and check our assertions again.
	q = openQ(t, qName)

	// Assert that we have more than one segment
	assert.NotEqual(t, q.firstSegment, q.lastSegment, "After opening, the first segment can not match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.lastSegment.number, "After opening, the last segment is not 2")

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
	assert.Equal(t, 7, q.Size(), "the size is calculated wrong.  Should be 7")

	// Assert that the first segment is #3
	assert.Equal(t, 1, q.firstSegment.number, "the first segment is not 1")

	// Assert that the last segment is #3
	assert.Equal(t, 3, q.lastSegment.number, "the last segment is not 3")

	// Dequeue 6 items
	for i := 0; i < 6; i++ {
		iface, err := q.Dequeue()
		if err != nil {
			t.Fatal("Error dequeueing", err)
		}

		// Check the Size calculation
		assert.Equal(t, 6-i, q.Size(), "the size is calculated wrong.")
		item, ok := iface.(item2)
		if ok {
			fmt.Printf("Dequeued %T %t %#v\n", item, ok, item)
			assert.Equal(t, i, item.Id, "Unexpected itemId")
		} else {
			item, ok := iface.(*item2)
			assert.True(t, ok, "Dequeued object is not of type *item2")
			assert.Equal(t, i, item.Id, "Unexpected itemId")
		}
	}

	// Assert that we have only one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "The first segment must match the second")

	// Assert that the first segment is #3
	assert.Equal(t, 3, q.firstSegment.number, "The last segment is not 3")

	// Now reopen the queue and check our assertions again.
	q = openQ(t, qName)

	// Assert that we have more than one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "After opening, the first segment must match the second")

	// Assert that the last segment is #3
	assert.Equal(t, 3, q.lastSegment.number, "After opening, the last segment is not 3")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

func TestQueue_EmptyDequeue(t *testing.T) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error removing queue directory", err)
	}

	// Create new queue with segment size 3
	q := newQ(t, qName)

	item, err := q.Dequeue()
	assert.Equal(t, EMPTY, err, "Expected a QUEUE_EMPTY error")
	assert.Nil(t, item, "Expected nil because queue is empty")

	if err := os.RemoveAll(qName); err != nil {
		t.Fatal("Error cleaning up the queue directory", err)
	}
}

func newQ(t *testing.T, qName string) *DQue {
	// Create a new segment with segment size of 3
	q, err := New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error creating new dque", err)
	}
	return q
}

func openQ(t *testing.T, qName string) *DQue {
	// Open an existing segment with segment size of 3
	q, err := Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Fatal("Error opening dque", err)
	}
	return q
}

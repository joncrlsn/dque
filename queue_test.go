// queue_test.go
package main

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
func Test_queue_AddRemoveLoop(t *testing.T) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Error("Error removing queue directory", err)
	}

	// Create a new segment with segment size of 3
	q, err := New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Error("Error creating new dque", err)
	}

	for i := 0; i < 4; i++ {
		var item interface{}
		if err := q.Enqueue(item2{i}); err != nil {
			t.Error("Error enqueueing", err)
		}
		item, err = q.Dequeue()
		if err != nil {
			t.Error("Error dequeueing", err)
		}
		fmt.Println("Dequeued:", item)
	}

	// Assert that we have just one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "The first segment must match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.firstSegment.number, "The first segment is not 2")

	// Now reopen the queue and check our assertions.
	q, err = Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Error("Error creating new dque", err)
	}

	// Assert that we have just one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "After opening, the first segment must match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.firstSegment.number, "After opening, the first segment is not 2")
}

// Adds 2 and removes 1 in a loop to ensure that when we've filled
// up the first segment that we delete it and move on to the next segment
func Test_queue_Add2Remove1(t *testing.T) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Error("Error removing queue directory", err)
	}

	// Create a new segment with segment size of 3
	q, err := New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Error("Error creating new dque", err)
	}

	for i := 0; i < 4; i = i + 2 {
		var item interface{}
		if err := q.Enqueue(item2{i}); err != nil {
			t.Error("Error enqueueing", err)
		}
		if err := q.Enqueue(item2{i + 1}); err != nil {
			t.Error("Error enqueueing", err)
		}
		item, err = q.Dequeue()
		if err != nil {
			t.Error("Error dequeueing", err)
		}
		fmt.Println("Dequeued:", item)
	}

	// Assert that we have more than one segment
	assert.NotEqual(t, q.firstSegment, q.lastSegment, "The first segment cannot match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.lastSegment.number, "The last segment is not 2")

	// Now reopen the queue and check our assertions.
	q, err = Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Error("Error creating new dque", err)
	}

	// Assert that we have more than one segment
	assert.NotEqual(t, q.firstSegment, q.lastSegment, "After opening, the first segment cannot match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.lastSegment.number, "After opening, the last segment is not 2")
}

// Adds 4 and removes 3
func Test_queue_Add4Remove3(t *testing.T) {
	qName := "test1"
	if err := os.RemoveAll(qName); err != nil {
		t.Error("Error removing queue directory", err)
	}

	// Create a new segment with segment size of 3
	q, err := New(qName, ".", 3, item2Builder)
	if err != nil {
		t.Error("Error creating new dque", err)
	}

	// Add 4 items
	for i := 0; i < 4; i++ {
		if err := q.Enqueue(item2{i}); err != nil {
			t.Error("Error enqueueing", err)
		}
	}

	for i := 0; i < 3; i++ {
		iface, err := q.Dequeue()
		if err != nil {
			t.Error("Error dequeueing", err)
		}
		item, ok := iface.(item2)
		assert.True(t, ok, "Dequeued object is not of type item2")
		assert.Equal(t, i, item.Id, "Unexpected itemId")
	}

	// Assert that we have more than one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "The first segment must match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.lastSegment.number, "The last segment is not 2")

	// Now reopen the queue and check our assertions.
	q, err = Open(qName, ".", 3, item2Builder)
	if err != nil {
		t.Error("Error creating new dque", err)
	}

	// Assert that we have more than one segment
	assert.Equal(t, q.firstSegment, q.lastSegment, "After opening, the first segment must match the second")

	// Assert that the first segment is #2
	assert.Equal(t, 2, q.lastSegment.number, "After opening, the last segment is not 2")
}

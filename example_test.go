package dque

import (
	"log"
)

// Item is the thing we'll be storing in the queue
type Item struct {
	Name string
	Id   int
}

// ItemBuilder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func ItemBuilder() interface{} {
	return &Item{}
}

func ExampleQueue_main() {
	// Create a new queue with segment size of 50
	q, err := New("item-queue", "./var", 50, ItemBuilder)
	if err != nil {
		log.Fatal("Error creating new dque", err)
	}

	// Add an item to the queue
	if err := q.Enqueue(&Item{"Joe", 1}); err != nil {
		log.Fatal("Error enqueueing item", err)
	}

	// You can reconsitute the queue from disk at any time
	// as long as you never use the old instance
	q, err = Open("item-queue", "./var", 50, ItemBuilder)
	if err != nil {
		log.Fatal("Error opening existing dque", err)
	}

	// Dequeue an item and act on it
	var iface interface{}
	if iface, err = q.Dequeue(); err != nil {
		log.Fatal("Error ")
	}
	item, ok := iface.(*Item)
	if !ok {
		log.Fatal("Dequeued object is not an Item pointer")
	}

	doSomething(item)
}

func doSomething(iface interface{}) {

}

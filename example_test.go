package dque_test

//
// Run with run test -
//

import (
	"log"

	"github.com/joncrlsn/dque"
)

// Item is what we'll be storing in the queue.  It can be any struct
// as long as the fields you want stored are public.
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
	qName := "item-queue"
	qDir := "/tmp"
	segmentSize := 50

	// Create a new queue with segment size of 50
	q, err := dque.NewOrOpen(qName, qDir, segmentSize, ItemBuilder)
	if err != nil {
		log.Fatal("Error creating new dque ", err)
	}

	// Add an item to the queue
	if err := q.Enqueue(&Item{"Joe", 1}); err != nil {
		log.Fatal("Error enqueueing item ", err)
	}

	log.Println("Size should be 1:", q.Size())

	// You can reconsitute the queue from disk at any time
	// as long as you never use the old instance
	q, err = dque.Open(qName, qDir, segmentSize, ItemBuilder)
	if err != nil {
		log.Fatal("Error opening existing dque ", err)
	}

	// Peek at the next item in the queue
	var iface interface{}
	if iface, err = q.Peek(); err != nil {
		if err != dque.EMPTY {
			log.Fatal("Error peeking at item ", err)
		}
	}

	// Dequeue the next item in the queue
	if iface, err = q.Dequeue(); err != nil {
		if err != dque.EMPTY {
			log.Fatal("Error dequeuing item ", err)
		}
	}

	log.Println("Size should be zero:", q.Size())

	// Assert type of the response to an Item pointer so we can work with it
	item, ok := iface.(*Item)
	if !ok {
		log.Fatal("Dequeued object is not an Item pointer")
	}

	doSomething(item)
}

func doSomething(item *Item) {
	log.Println("Dequeued", item)
}

func main() {

}

# dque - simple embedded durable queue for Go

dque is a persistent, scalable, FIFO queue for Go.  Because it frustrated me (Jon Carlson) that the only embedded persistent queues I could find for Go were wrappers around key value stores, I wrote this to show that a simple, fast, persistent FIFO queue can be written.

Thank you to Gabor Cselle who, years ago, inspired me with an example of an [in-memory persistent queue written in Java](http://www.gaborcselle.com/open_source/java/persistent_queue.html).  I was intrigued by the simplicity of his approach, which became the foundation of the "segment" part of this queue which holds the head and the tail of the queue in memory.

The performance is pretty good. On a 3 year old MacBook Pro with SSD, I am able to get around 350 microseconds per enqueue and 400 microseconds per dequeue (for a small struct).

Please note that I don't claim to be very good at maintaining an active project.  I'd like to know about bugs so I can fix them, but if you want to add features or make big changes then please fork this project.  If you do good things with it, I'll add a link on this page to your project.

### implementation
* The queue is held in segments of a configurable size. Each segment corresponds with a file on disk. If there is more than one segment, new items are enqueued to the last segment and dequeued from the first segment.
* Because the encoding/gob package is used to store the struct to disk: 
  * Only structs can be stored in the queue.
  * Only one type of struct can be stored in each queue.
  * Only public fields in a struct will be stored.
  * You must provide a function that returns a pointer to a new struct of the type stored in the queue.  This function is used when loading segments into memory from disk.  If you can think of a better way to handle this, I'd love to hear it.
* Segment implementation:
  * Enqueueing an item adds it both to the end of the last segment file and to the in-memory item slice for that segment.
  * When a segment reaches its maximum size a new segment is created.
  * Dequeueing an item removes it from the beginning of the in-memory slice and appends a "delete" marker to the end of the segment file.  This allows the item to be left in the file until the number of delete markers matches the number of items, at which point the entire file is deleted.
  * When a segment is reconstituted from disk, each "delete" marker found in the file causes a removal of the first element of the in-memory slice.
  * When each item in the segment has been dequeued, the segment file is deleted and the next segment is loaded into memory.

### example
```golang
package main

import (
	"github.com/joncrlsn/dque"
	"log"
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

func main() {
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

	// Dequeue an item and act on it
	var iface interface{}
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
```

### todo
* store the segment size in a file inside the queue. Then it only needs to be specified on dque.New(...)

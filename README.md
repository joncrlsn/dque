# dque - simple embedded durable queue for Go

dque is:
* embedded into your Golang program
* persistent -- survives program restarts
* scalable -- not limited by your RAM, but by your disk space
* FIFO - First in First Out
* synchronized and safe for concurrent usage

I love tools that do one thing well.  This queue implementation should fit into that category.  It frustrated me that the only embedded persistent queues I could find for Go were wrappers around key value stores, so I wrote this to show that it could be done without being dependent on a storage engine that is better suited to other use cases.

Thank you to Gabor Cselle who, years ago, inspired me with an example of an [in-memory persistent queue written in Java](http://www.gaborcselle.com/open_source/java/persistent_queue.html).  I was intrigued by the simplicity of his approach, which became the foundation of the "segment" part of this queue which holds the head and the tail of the queue in memory as well as storing the segment files in between.

The performance is pretty good. On a 3 year old MacBook Pro with SSD, I am able to get around 350 microseconds per enqueue and 400 microseconds per dequeue (for a small struct).

### implementation
* The queue is held in segments of a configurable size. Each segment corresponds with a file on disk. If there is more than one segment, new items are enqueued to the last segment and dequeued from the first segment.
* Because the encoding/gob package is used to store the struct to disk: 
  * Only structs can be stored in the queue.
  * Only one type of struct can be stored in each queue.
  * Only public fields in a struct will be stored.
  * You must provide a function that returns a pointer to a new struct of the type stored in the queue.  This function is used when loading segments into memory from disk.  If you can think of a better way to handle this, I'd love to hear it.
* Queue segment implementation:
  * For nice visuals, see [Gabor Cselle's documentation here](http://www.gaborcselle.com/open_source/java/persistent_queue.html).  Note that Gabor's implementation kept the entire queue in memory as well as disk.
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

	var iface interface{}

	// Peek at the first item in the queue without removing it
	if iface, err = q.Peek(); err != nil {
		if err != dque.EMPTY {
			log.Fatal("Error peeking at item ", err)
		}
	}

	log.Println("Size should still be one:", q.Size())

	// Dequeue an item and act on it
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

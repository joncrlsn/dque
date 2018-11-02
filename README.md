# dque - durable (persistent) queue

dque is an embedded persistent FIFO queue for Go.  It frustrated me that the only persistent queues for Go were just wrappers around LevelDB, so I wrote this.  While I like the approach, I hope that Golang generics will someday make it easier to use.  

I rather hope that someone will fork this and build a real, supported project out of it.  I wrote this as an example of how a persistent FIFO queue should work (IMHO) and I'd love to see someone else take it and make it "fly".  

The current code seems to work, but I'm still writing tests and creating a benchmark for it to see how it performs. 

### Implementation
* The queue is held in segments of a configurable size. Each segment corresponds with a file on disk. If there is more than one segment, new items are enqueued to the last segment and dequeued from the first segment.
* Because the encoding/gob package is used to store the struct to disk: 
  * Only structs can be stored in the queue
  * Only one type of struct can be stored
  * When creating or opening a queue, you must provide a function that returns a pointer to the struct stored in the queue
* Segment implementation:
  * Enqueueing an item adds it both to the end of the last segment file and to the in-memory item slice for that segment.
  * When a segment reaches its maximum size a new segment is created.
  * Dequeueing an item removes it from the in-memory slice and appends a "delete" marker to the end of the segment file.
  * When the segment is reconsituted from disk, each "delete" marker causes a removal of the first element of the in-memory slice.
  * When each item in the segment has been dequeued, the segment file is deleted and the next segment is loaded into memory.

### Example
```golang
// Item is the thing we'll be storing in the queue
type Item struct {
    Name string
    Id int
}

// ItemBuilder creates a new item and returns a pointer to it.
// This is used when we load a segment of the queue from disk.
func ItemBuilder() interface{} {
    return &Item{}
}

func main() {
   	// Create a new queue with segment size of 50
    q, err := New("item-queue", "/var", 50, ItemBuilder)
    if err != nil {
        log.Fatal("Error creating new dque", err)
    }

    // Add an item to the queue
    if err := q.Enqueue(&Item{"Joe",1}); err != nil {
        log.Fatal("Error enqueueing item", err)
    }

    // You can reconsitute the queue from disk at any time
    q, err = Open("item-queue", "/var", 50, ItemBuilder)
    if err != nil {
        log.Fatal("Error opening existing dque", err)
    }

    // Dequeue an item and act on it
    var iface interface{}
    if iface, err = q.Dequeue(); err != nil {
        log.Fatal("Error ")
    }
    item, ok := iface.(*Item)
    if ok {
        doSomething(item)
    else {
        log.Fatal("Dequeued object is not an Item pointer")
    }
}

```

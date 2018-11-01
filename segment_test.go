// segement_test.go
package main

import (
	"fmt"
	"log"
	"os"
	"testing"
)

// item1 is the thing we'll be storing in the queue
type item1 struct {
	Name string
}

// item1Builder creates a new item and returns a pointer to it.
// This is used when we load a queue from disk.
func item1Builder() interface{} {
	return &item1{}
}

// Test_segment verifies the behavior of the queue segment.
// TODO: Make this into a real test that programmatically verifies assumptions
func Test_segment(t *testing.T) {
	testDir := "./test"
	os.RemoveAll(testDir)
	os.Mkdir(testDir, 0755)

	// Create a new segment of the queue
	seg, err := newQueueSegment(testDir, 1, item1Builder)
	if err != nil {
		log.Fatalf("newQueueSegment('%s') failed with '%s'\n", testDir, err.Error())
	}

	// Add some items and remove one
	seg.add(&item1{Name: "item1"})
	seg.add(&item1{Name: "item 2"})
	_, err = seg.remove()
	if err != nil {
		log.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	seg.add(&item1{Name: "item3"})
	_, err = seg.remove()
	if err != nil {
		log.Fatalf("Remove() failed with '%s'\n", err.Error())
	}

	fmt.Println("Recreating the segment from disk")

	seg, err = openQueueSegment(testDir, 1, item1Builder)
	if err != nil {
		log.Fatalf("openQueueSegment('%s') failed with '%s'\n", testDir, err.Error())
	}
	log.Println("Opened segment: ", seg.filePath())

	log.Println("Removing all items:")
	for {
		_, err := seg.remove()
		if err != nil {
			if err == emptySegment {
				break
			}
			log.Fatalf("Remove() failed with '%s'\n", err.Error())
		}
	}
}

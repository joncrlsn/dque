// segement_test.go
package dque

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/stvp/assert"
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
func TestSegment(t *testing.T) {
	testDir := "./TestSegment"
	os.RemoveAll(testDir)
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatalf("Error creating directory from the TestSegment method: %s\n", err)
	}

	// Create a new segment of the queue
	seg, err := newQueueSegment(testDir, 1, item1Builder)
	if err != nil {
		t.Fatalf("newQueueSegment('%s') failed with '%s'\n", testDir, err.Error())
	}

	// Add some items and remove one
	seg.add(&item1{Name: "Number 1"})
	assert.Equal(t, 1, seg.size(), "Expected size of 1")

	seg.add(&item1{Name: "Number 2"})
	assert.Equal(t, 2, seg.size(), "Expected size of 2")
	_, err = seg.remove()
	if err != nil {
		t.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	assert.Equal(t, 1, seg.size(), "Expected size of 1")
	assert.Equal(t, 2, seg.sizeOnDisk(), "Expected sizeOnDisk of 2")
	seg.add(&item1{Name: "item3"})
	assert.Equal(t, 2, seg.size(), "Expected size of 2")
	_, err = seg.remove()
	if err != nil {
		t.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	assert.Equal(t, 1, seg.size(), "Expected size of 1")

	fmt.Println("Recreating the segment from disk")

	seg, err = openQueueSegment(testDir, 1, item1Builder)
	if err != nil {
		t.Fatalf("openQueueSegment('%s') failed with '%s'\n", testDir, err.Error())
	}
	log.Println("Opened segment: ", seg.filePath())
	assert.Equal(t, 1, seg.size(), "Expected size of 1")

	log.Println("Removing all items:")
	for {
		_, err := seg.remove()
		if err != nil {
			if err == emptySegment {
				break
			}
			t.Fatalf("Remove() failed with '%s'\n", err.Error())
		}
	}

	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Error cleaning up directory from the TestSegment method with '%s'\n", err.Error())
	}
}

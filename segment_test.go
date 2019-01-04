// segement_test.go
package dque

//
// White box texting of the aSegment struct and methods.
//

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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

// Test_segment verifies the behavior of one segment.
func TestSegment(t *testing.T) {
	testDir := "./TestSegment"
	os.RemoveAll(testDir)
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatalf("Error creating directory from the TestSegment method: %s\n", err)
	}

	// Create a new segment of the queue
	seg, err := newQueueSegment(testDir, 1, false, item1Builder)
	if err != nil {
		t.Fatalf("newQueueSegment('%s') failed with '%s'\n", testDir, err.Error())
	}

	//
	// Add some items and remove one
	//
	seg.add(&item1{Name: "Number 1"})
	assert(t, 1 == seg.size(), "Expected size of 1")

	seg.add(&item1{Name: "Number 2"})
	assert(t, 2 == seg.size(), "Expected size of 2")
	_, err = seg.remove()
	if err != nil {
		t.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	assert(t, 1 == seg.size(), "Expected size of 1")
	assert(t, 2 == seg.sizeOnDisk(), "Expected sizeOnDisk of 2")
	seg.add(&item1{Name: "item3"})
	assert(t, 2 == seg.size(), "Expected size of 2")
	_, err = seg.remove()
	if err != nil {
		t.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	assert(t, 1 == seg.size(), "Expected size of 1")

	//
	// Recreate the segment from disk and remove the remaining item
	//
	seg, err = openQueueSegment(testDir, 1, false, item1Builder)
	if err != nil {
		t.Fatalf("openQueueSegment('%s') failed with '%s'\n", testDir, err.Error())
	}
	assert(t, 1 == seg.size(), "Expected size of 1")

	_, err = seg.remove()
	if err != nil {
		if err != errEmptySegment {
			t.Fatalf("Remove() failed with '%s'\n", err.Error())
		}
	}
	assert(t, 0 == seg.size(), "Expected size of 0")

	// Cleanup
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Error cleaning up directory from the TestSegment method with '%s'\n", err.Error())
	}
}

// TestSegment_Open verifies the behavior of the openSegment function.
func TestSegment_openQueueSegment_failIfNew(t *testing.T) {
	testDir := "./TestSegment_Open"
	os.RemoveAll(testDir)
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatalf("Error creating directory in the TestSegment_Open method: %s\n", err)
	}

	seg, err := openQueueSegment(testDir, 1, false, item1Builder)
	if err == nil {
		t.Fatalf("openQueueSegment('%s') should have failed because it should be new\n", testDir)
	}
	assert(t, seg == nil, "segment after failure must be nil")

	// Cleanup
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Error cleaning up directory from the TestSegment_Open method with '%s'\n", err.Error())
	}
}

// TestSegment_Turbo verifies the behavior of the turboOn() and turboOff() methods.
func xTestSegment_Turbo(t *testing.T) {
	testDir := "./TestSegment"
	os.RemoveAll(testDir)
	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatalf("Error creating directory in the TestSegment_Turbo method: %s\n", err)
	}

	seg, err := newQueueSegment(testDir, 10, false, item1Builder)
	if err != nil {
		t.Fatalf("newQueueSegment('%s') failed\n", testDir)
	}

	// turbo is off so expect syncCount to change
	seg.add(&item1{Name: "Number 1"})
	assert(t, 1 == seg.size(), "Expected size of 1")
	assert(t, 1 == seg.syncCount, "syncCount must be 1")

	// Turn on turbo and expect sync count to stay the same.
	seg.turboOn()
	seg.add(&item1{Name: "Number 2"})
	assert(t, 2 == seg.size(), "Expected size of 2")
	assert(t, 1 == seg.syncCount, "syncCount must still be 1")

	// Turn off turbo and expect the syncCount to increase when remove is called.
	if err = seg.turboOff(); err != nil {
		t.Fatalf("Unexpecte error turning off turbo('%s')\n", testDir)
	}
	_, err = seg.remove()
	if err != nil {
		t.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	assert(t, 2 == seg.syncCount, "syncCount must be 2 now") // syncCount should have increased by one

	// Cleanup
	if err := os.RemoveAll(testDir); err != nil {
		t.Fatalf("Error cleaning up directory from the TestSegment_Open method with '%s'\n", err.Error())
	}
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

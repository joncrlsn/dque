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
	assert(t, seg.add(&item1{Name: "Number 1"}) == nil, "failed to add item1")
	assert(t, 1 == seg.size(), "Expected size of 1")

	assert(t, seg.add(&item1{Name: "Number 2"}) == nil, "failed to add item2")
	assert(t, 2 == seg.size(), "Expected size of 2")
	_, err = seg.remove()
	if err != nil {
		t.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	assert(t, 1 == seg.size(), "Expected size of 1")
	assert(t, 2 == seg.sizeOnDisk(), "Expected sizeOnDisk of 2")
	assert(t, seg.add(&item1{Name: "item3"}) == nil, "failed to add item3")
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

// TestSegment_ErrCorruptedSegment tests error handling for corrupted data
func TestSegment_ErrCorruptedSegment(t *testing.T) {
	testDir := "./TestSegmentError"
	os.RemoveAll(testDir)
	defer os.RemoveAll((testDir))

	if err := os.Mkdir(testDir, 0755); err != nil {
		t.Fatalf("Error creating directory in the TestSegment_ErrCorruptedSegment method: %s\n", err)
	}

	f, err := os.Create((&qSegment{dirPath: testDir}).filePath())
	if err != nil {
		t.Fatal(err)
	}

	// expect an 8 byte object, but only write 7 bytes
	if _, err := f.Write([]byte{0, 0, 0, 8, 1, 2, 3, 4, 5, 6, 7}); err != nil {
		t.Fatal(err)
	}
	f.Close()

	_, err = openQueueSegment(testDir, 0, false, func() interface{} { return make([]byte, 8) })
	if err == nil {
		t.Fatal("expected ErrCorruptedSegment but got nil")
	}
	// // go >= 1.13:
	// var corruptedError ErrCorruptedSegment
	// if !errors.As(err, &corruptedError) {
	// 	t.Fatalf("expected ErrCorruptedSegment but got %T: %s", err, err)
	// }
	corruptedError, ok := unwrapError(unwrapError(err)).(ErrCorruptedSegment)
	if !ok {
		t.Fatalf("expected ErrCorruptedSegment but got %T: %s", err, err)
	}
	if corruptedError.Path != "TestSegmentError/0000000000000.dque" {
		t.Fatalf("unexpected file path: %s", corruptedError.Path)
	}
	if corruptedError.Error() != "segment file TestSegmentError/0000000000000.dque is corrupted: error reading gob data from file: unexpected EOF" {
		t.Fatalf("wrong error message: %s", corruptedError.Error())
	}
}

func unwrapError(err error) error {
	return err.(interface{ Unwrap() error }).Unwrap()
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
func TestSegment_Turbo(t *testing.T) {
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
	assert(t, seg.add(&item1{Name: "Number 1"}) == nil, "failed to add item1")
	assert(t, 1 == seg.size(), "Expected size of 1")
	assert(t, 1 == seg.syncCount, "syncCount must be 1")

	// Turn on turbo and expect sync count to stay the same.
	seg.turboOn()
	assert(t, seg.add(&item1{Name: "Number 2"}) == nil, "failed to add item2")
	assert(t, 2 == seg.size(), "Expected size of 2")
	assert(t, 1 == seg.syncCount, "syncCount must still be 1")

	// Turn off turbo and expect the syncCount to increase when remove is called.
	if err = seg.turboOff(); err != nil {
		t.Fatalf("Unexpecte error turning off turbo('%s')\n", testDir)
	}

	// seg.turboOff() calls seg.turboSync() which increments syncCount
	assert(t, 2 == seg.syncCount, "syncCount must be 2 now")

	_, err = seg.remove()
	if err != nil {
		t.Fatalf("Remove() failed with '%s'\n", err.Error())
	}
	// seg.remove() calls seg._sync() which increments syncCount
	assert(t, 3 == seg.syncCount, "syncCount must be 3 now")

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

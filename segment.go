package dque

//
// Copyright (c) 2018 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//

//
// This is a segment of a memory-efficient FIFO durable queue.  Items in the queue must be of the same type.
//
// Each qSegment instance corresponds to a file on disk.
//
// This segment is both persistent and in-memory so there is a memory limit to the size
// (which is why it is just a segment instead of being used for the entire queue).
//

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"sync"

	"github.com/pkg/errors"
)

var (
	errEmptySegment = errors.New("Segment is empty")
)

// qSegment represents a portion (segment) of a persistent queue
type qSegment struct {
	dirPath       string
	number        int
	objects       []interface{}
	objectBuilder func() interface{}
	file          *os.File
	mutex         sync.Mutex
	removeCount   int
	turbo         bool
	maybeDirty    bool // filesystem changes may not have been flushed to disk
}

// load reads all objects from the queue file into a slice
func (seg *qSegment) load() error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	// Open the file in read mode
	var err error
	seg.file, err = os.OpenFile(seg.filePath(), os.O_RDONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "error opening file: "+seg.filePath())
	}
	defer seg.file.Close()

	// Loop until we can load no more
	for {
		// Read the 4 byte length of the gob
		lenBytes := make([]byte, 4)
		bytesRead, err := seg.file.Read(lenBytes)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if bytesRead == 0 {
			log.Printf("qSegment.load() did nothing. %s files is new\n", seg.filePath())
			return nil
		}
		if bytesRead != 4 {
			return errors.New("not enough bytes were read")
		}

		// Convert the bytes into a 32-bit unsigned int
		gobLen := binary.LittleEndian.Uint32(lenBytes)
		if gobLen == 0 {
			// Remove the first item from the in-memory queue
			seg.objects = seg.objects[1:]
			//fmt.Println("TEMP: Detected delete in load()")
			seg.removeCount++
			continue
		}

		// Make a byte array the exact size of the gob
		// Then read the gob into it
		gobBytes := make([]byte, gobLen)
		_, err = seg.file.Read(gobBytes)
		if err != nil {
			return errors.Wrap(err, "error reading gob bytes")
		}

		// Decode the bytes into an object
		reader := bytes.NewReader(gobBytes)
		dec := gob.NewDecoder(reader)
		object := seg.objectBuilder()
		dec.Decode(object)

		// Add item to the objects slice
		seg.objects = append(seg.objects, object)

		//fmt.Printf("TEMP: Loaded: %#v\n", object)
	}

	//fmt.Printf("TEMP: Loaded %d objects into memory\n", len(seg.objects))
	return nil
}

// peek returns the first item in the segment without removing it.
// If the queue is already empty, the emptySegment error will be returned.
func (seg *qSegment) peek() (interface{}, error) {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if len(seg.objects) == 0 {
		// Queue is empty so return nil object (and emptySegment error)
		return nil, errEmptySegment
	}

	// Save a reference to the first item in the in-memory queue
	object := seg.objects[0]

	return object, nil
}

// remove removes and returns the first item in the segment and adds
// a zero length marker to the end of the queue file to signify a removal.
// If the queue is already empty, the emptySegment error will be returned.
func (seg *qSegment) remove() (interface{}, error) {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if len(seg.objects) == 0 {
		// Queue is empty so return nil object (and empty_segment error)
		return nil, errEmptySegment
	}

	// Create a 4-byte length of value zero (this signifies a removal)
	deleteLen := 0
	deleteLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(deleteLenBytes, uint32(deleteLen))

	// Write the 4-byte length (of zero) first
	seg.file.Write(deleteLenBytes)

	// Save a reference to the first item in the in-memory queue
	object := seg.objects[0]

	// Remove the first item from the in-memory queue
	seg.objects = seg.objects[1:]

	// Increment the delete count
	seg.removeCount++

	// Possibly force writes to disk
	if err := seg._sync(); err != nil {
		return nil, err
	}

	return object, nil
}

// Add adds an item to the in-memory queue segment and appends it to the persistent file
func (seg *qSegment) add(object interface{}) error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	// Encode the struct to a byte buffer
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(object)
	if err != nil {
		return errors.Wrap(err, "error gob encoding object")
	}

	// Count the bytes stored in the byte buffer
	// and store the count into a 4-byte byte array
	buffLen := len(buff.Bytes())
	buffLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffLenBytes, uint32(buffLen))

	// Write the 4-byte buffer length first
	seg.file.Write(buffLenBytes)

	// Then write the buffer bytes
	seg.file.Write(buff.Bytes())

	seg.objects = append(seg.objects, object)

	// Possibly force writes to disk
	return seg._sync()
}

// size returns the number of objects in this segment.
// The size does not include items that have been removed.
func (seg *qSegment) size() int {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	return len(seg.objects)
}

// sizeOnDisk returns the number of objects in memory plus removed objects. This
// number will match the number of objects still on disk.
// This number is used to keep the file from growing forever when items are
// removed about as fast as they are added.
func (seg *qSegment) sizeOnDisk() int {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	return len(seg.objects) + seg.removeCount
}

// delete wipes out the queue and its persistent state
func (seg *qSegment) delete() error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	if err := seg.file.Close(); err != nil {
		return errors.Wrap(err, "unable to close the segment file before deleting")
	}

	// Delete the storage for this queue
	err := os.Remove(seg.filePath())
	if err != nil {
		return errors.Wrap(err, "error deleting file: "+seg.filePath())
	}

	// Empty the in-memory slice of objects
	seg.objects = seg.objects[:0]

	seg.file = nil

	return nil
}

func (seg *qSegment) fileName() string {
	return fmt.Sprintf("%013d.dque", seg.number)
}

func (seg *qSegment) filePath() string {
	return path.Join(seg.dirPath, seg.fileName())
}

// turboOn allows the filesystem to decide when to sync file changes to disk
// Speed is be greatly increased by turning turbo on, however there is some
// risk of losing data should a power-loss occur.
func (seg *qSegment) turboOn() {
	seg.turbo = true
}

// turboOff re-enables the "safety" mode that syncs every file change to disk as
// they happen.
func (seg *qSegment) turboOff() error {
	if !seg.turbo {
		// turboOff is know to be called twice when the first and last ssegments
		// are the same.
		return nil
	}
	if err := seg.turboSync(); err != nil {
		return err
	}
	seg.turbo = false
	return nil
}

// turboSync does an fsync to disk if turbo is on.
func (seg *qSegment) turboSync() error {
	if !seg.turbo {
		// When the first and last segments are the same, this method
		// will be called twice.
		return nil
	}
	if seg.maybeDirty {
		if err := seg.file.Sync(); err != nil {
			return errors.Wrap(err, "unable to sync file changes.")
		}
		seg.maybeDirty = false
	}
	return nil
}

// _sync must only be called by the add and remove methods on qSegment.
// Only syncs if turbo is off
func (seg *qSegment) _sync() error {
	if seg.turbo {
		// We do *not* force a sync if turbo is on
		// We just mark it maybe Dirty
		seg.maybeDirty = true
		return nil
	}

	if err := seg.file.Sync(); err != nil {
		return errors.Wrap(err, "unable to sync file changes in _sync method.")
	}
	seg.maybeDirty = false
	return nil
}

// newQueueSegment creates a new, persistent  segment of the queue
func newQueueSegment(dirPath string, number int, turbo bool, builder func() interface{}) (*qSegment, error) {

	seg := qSegment{dirPath: dirPath, number: number, turbo: turbo, objectBuilder: builder}

	if !dirExists(seg.dirPath) {
		return nil, errors.New("dirPath is not a valid directory: " + seg.dirPath)
	}

	if fileExists(seg.filePath()) {
		return nil, errors.New("file already exists: " + seg.filePath())
	}

	// Create the file in append mode
	var err error
	seg.file, err = os.OpenFile(seg.filePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "error creating file: "+seg.filePath())
	}
	// Leave the file open for future writes

	return &seg, nil
}

// openQueueSegment reads an existing persistent segment of the queue into memory
func openQueueSegment(dirPath string, number int, turbo bool, builder func() interface{}) (*qSegment, error) {

	seg := qSegment{dirPath: dirPath, number: number, turbo: turbo, objectBuilder: builder}

	if !dirExists(seg.dirPath) {
		return nil, errors.New("dirPath is not a valid directory: " + seg.dirPath)
	}

	if !fileExists(seg.filePath()) {
		return nil, errors.New("file does not exist: " + seg.filePath())
	}

	// Load the items into memory
	if err := seg.load(); err != nil {
		return nil, errors.Wrap(err, "unable to load queue segment in "+dirPath)
	}

	// Re-open the file in append mode
	var err error
	seg.file, err = os.OpenFile(seg.filePath(), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "error opening file: "+seg.filePath())
	}
	// Leave the file open for future writes

	return &seg, nil
}

//
// Copyright (c) 2018 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
package main

//
// This is a segment of a memory-efficient FIFO durable queue.  Items in the queue must be of the same type.
//
// Each queue segment corresponds to a file on disk.
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
	emptySegment = errors.New("Segment is empty")
)

// qSegment is the portion of a persistent queue
type qSegment struct {
	dirPath       string
	number        int
	objects       []interface{}
	objectBuilder func() interface{}
	mutex         sync.Mutex
	removeCount   int
}

// load reads all objects from the queue file into a slice
func (seg *qSegment) load() error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	fmt.Printf("TEMP: Loading segment %d\n", seg.number)

	// Open the file in read mode
	file, err := os.OpenFile(seg.filePath(), os.O_RDONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "Error opening file: "+seg.filePath())
	}
	defer file.Close()

	// Loop until we can load no more
	for {
		// Read the 4 byte length of the gob
		lenBytes := make([]byte, 4)
		bytesRead, err := file.Read(lenBytes)
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
			return errors.New("Not enough bytes were read")
		}

		// Convert the bytes into a 32-bit unsigned int
		gobLen := binary.LittleEndian.Uint32(lenBytes)
		if gobLen == 0 {
			// Remove the first item from the in-memory queue
			seg.objects = seg.objects[1:]
			fmt.Println("TEMP: Detected delete in load()")
			seg.removeCount++
			continue
		}

		// Make a byte array the exact size of the gob
		// Then read the gob into it
		gobBytes := make([]byte, gobLen)
		bytesRead, err = file.Read(gobBytes)
		if err != nil {
			return errors.Wrap(err, "Error reading gob bytes")
		}

		// Decode the bytes into an object
		reader := bytes.NewReader(gobBytes)
		dec := gob.NewDecoder(reader)
		object := seg.objectBuilder()
		dec.Decode(object)

		// Add item to the objects slice
		seg.objects = append(seg.objects, object)

		// Brag about it
		fmt.Printf("TEMP: Loaded: %#v\n", object)
	}

	// Brag about it
	fmt.Printf("TEMP: Loaded %d objects into memory\n", len(seg.objects))
	return nil
}

// remove removes and returns the first item in the segment and adds
// a zero length marker to the end of the queue file to signify a removal.
// If the queue is already empty, the emptySegment error will be returned.
func (seg *qSegment) remove() (interface{}, error) {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	fmt.Printf("TEMP: Removing from segment %d size %d removed %d\n", seg.number, len(seg.objects), seg.removeCount)

	if len(seg.objects) == 0 {
		// Queue is empty so return nil object (and empty_segment error)
		return nil, emptySegment
	}

	// Create a 4-byte length of value zero (this signifies a removal)
	deleteLen := 0
	deleteLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(deleteLenBytes, uint32(deleteLen))

	// Open or create the file in append mode
	file, err := os.OpenFile(seg.filePath(), os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, errors.Wrap(err, "Error opening file: "+seg.filePath())
	}
	defer file.Close()

	// Write the 4-byte length (of zero) first
	file.Write(deleteLenBytes)

	// Save a reference to the first item in the in-memory queue
	object := seg.objects[0]

	// Remove the first item from the in-memory queue
	seg.objects = seg.objects[1:]

	// Increment the delete count
	seg.removeCount++

	fmt.Printf("TEMP: Removed from segment %d %#v\n", seg.number, object)

	return object, nil
}

// Add adds an item to the in-memory queue segment and appends it to the persistent file
func (seg *qSegment) add(object interface{}) error {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	fmt.Printf("TEMP: Adding to segment %d %#v size %d removed %d\n", seg.number, object, len(seg.objects), seg.removeCount)

	// Encode the struct to a byte buffer
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(object)
	if err != nil {
		//log.Fatalf("enc.Encode() failed with '%s'\n", err.Error())
		return err
	}

	// Count the bytes stored in the byte buffer
	// and store the count into a 4-byte byte array
	buffLen := len(buff.Bytes())
	buffLenBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(buffLenBytes, uint32(buffLen))
	//fmt.Println("TEMP: Length of encoded struct is ", buffLen)

	// Create or open the file in append mode
	file, err := os.OpenFile(seg.filePath(), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return errors.Wrap(err, "Error opening file: "+seg.filePath())
	}
	defer file.Close()

	// Write the 4-byte buffer length first
	file.Write(buffLenBytes)

	// Then write the buffer bytes
	file.Write(buff.Bytes())

	seg.objects = append(seg.objects, object)

	// Brag about it
	fmt.Printf("TEMP: Added: %#v\n", object)

	return nil
}

// size returns the number of objects in this segment.
// The size does not include items that have been removed.
func (seg *qSegment) size() int {

	// This is heavy-handed but its safe
	seg.mutex.Lock()
	defer seg.mutex.Unlock()

	return len(seg.objects)
}

// bigness returns the number of objects in memory plus removed objects.
// This number is used to keep my file from growing forever.
func (seg *qSegment) bigness() int {

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

	// Delete the storage for this queue
	err := os.Remove(seg.filePath())
	if err != nil {
		return errors.Wrap(err, "Error deleting file: "+seg.filePath())
	}

	// Empty the in-memory slice of objects
	seg.objects = seg.objects[:0]

	// Brag about it
	fmt.Printf("Deleted: %s\n", seg.filePath())

	return nil
}

func (seg *qSegment) fileName() string {
	return fmt.Sprintf("%013d.dque", seg.number)
}

func (seg *qSegment) filePath() string {
	return path.Join(seg.dirPath, seg.fileName())
}

// newQueueSegment creates a new, persistent  segment of the queue
func newQueueSegment(dirPath string, number int, builder func() interface{}) (*qSegment, error) {

	seg := qSegment{dirPath: dirPath, number: number, objectBuilder: builder}

	if !dirExists(seg.dirPath) {
		return nil, errors.New("dirPath is not a valid directory: " + seg.dirPath)
	}

	if fileExists(seg.filePath()) {
		return nil, errors.New("file already exists: " + seg.filePath())
	}

	return &seg, nil
}

// openQueueSegment reads an existing persistent segment of the queue into memory
func openQueueSegment(dirPath string, number int, builder func() interface{}) (*qSegment, error) {

	seg := qSegment{dirPath: dirPath, number: number, objectBuilder: builder}

	if !dirExists(seg.dirPath) {
		return nil, errors.New("dirPath is not a valid directory: " + seg.dirPath)
	}

	if !fileExists(seg.filePath()) {
		return nil, errors.New("file does not exist: " + seg.filePath())
	}

	// Load the items into memory
	if err := seg.load(); err != nil {
		return nil, errors.Wrap(err, "Unable to load queue segment in "+dirPath)
	}
	return &seg, nil
}

func buildFileName(num int) string {
	return fmt.Sprintf("%010d.dque", num)
}

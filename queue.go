//
// Copyright (c) 2018 Jon Carlson.  All rights reserved.
// Use of this source code is governed by an MIT-style
// license that can be found in the LICENSE file.
//
package dque

//
// A scalable, embedded, persistent FIFO queue implementation using gob encoding.
//

// Test Cases:
//
// Enqueue first item - file is created
// Enqueue until it creates a new segment - second file is created
// Dequeue all items from first segment - first file gets cleaned up
// Enqueue one and dequeue one in a loop.  Ensure we move beyond one segment.

// Questions:
//
// Can this store strings, or is a structure required by gob encoding?
//

import (
	"strconv"
	"sync"

	"github.com/pkg/errors"

	"io/ioutil"
	"math"
	"os"
	"path"
	"regexp"
)

var (
	filePattern *regexp.Regexp
	EMPTY       error = errors.New("dque is empty")
)

func init() {
	filePattern, _ = regexp.Compile("^([0-9]+)\\.dque$")
}

type Config struct {
	ItemsPerSegment int
}

type DQue struct {
	Name    string
	DirPath string
	Config  Config

	fullPath     string
	firstSegment *qSegment
	lastSegment  *qSegment
	mutex        sync.Mutex
	builder      func() interface{} // builds a structure to load via gob
}

// New creats a new durable queue
func New(name string, dirPath string, itemsPerSegment int, builder func() interface{}) (*DQue, error) {

	// Validation
	if len(name) == 0 {
		return nil, errors.New("the queue name requires a value.")
	}
	if len(dirPath) == 0 {
		return nil, errors.New("the queue directory requires a value.")
	}
	if !dirExists(dirPath) {
		return nil, errors.New("the given queue directory is not valid: " + dirPath)
	}
	fullPath := path.Join(dirPath, name)
	if dirExists(fullPath) {
		return nil, errors.New("the given queue directory already exists: " + fullPath + ". Use Open instead")
	}

	if err := os.Mkdir(fullPath, 0755); err != nil {
		return nil, errors.Wrap(err, "error creating queue directory "+fullPath)
	}

	q := DQue{Name: name, DirPath: dirPath}
	q.fullPath = fullPath
	q.Config.ItemsPerSegment = itemsPerSegment
	q.builder = builder
	q.load()
	return &q, nil
}

// Open opens an existing durable queue.
func Open(name string, dirPath string, itemsPerSegment int, builder func() interface{}) (*DQue, error) {

	// Validation
	if len(name) == 0 {
		return nil, errors.New("the queue name requires a value.")
	}
	if len(dirPath) == 0 {
		return nil, errors.New("the queue directory requires a value.")
	}
	if !dirExists(dirPath) {
		return nil, errors.New("the given queue directory is not valid (" + dirPath + ")")
	}
	fullPath := path.Join(dirPath, name)
	if !dirExists(fullPath) {
		return nil, errors.New("the given queue does not exist (" + fullPath + ")")
	}

	q := DQue{Name: name, DirPath: dirPath}
	q.fullPath = fullPath
	q.Config.ItemsPerSegment = itemsPerSegment
	q.builder = builder
	q.load()
	return &q, nil
}

// NewOrOpen either creates a new queue or opens an existing durable queue.
func NewOrOpen(name string, dirPath string, itemsPerSegment int, builder func() interface{}) (*DQue, error) {

	// Validation
	if len(name) == 0 {
		return nil, errors.New("the queue name requires a value.")
	}
	if len(dirPath) == 0 {
		return nil, errors.New("the queue directory requires a value.")
	}
	if !dirExists(dirPath) {
		return nil, errors.New("the given queue directory is not valid (" + dirPath + ")")
	}
	fullPath := path.Join(dirPath, name)
	if dirExists(fullPath) {
		return Open(name, dirPath, itemsPerSegment, builder)
	}

	return New(name, dirPath, itemsPerSegment, builder)
}

func NewConfig(itemsPerSegment int) Config {
	return Config{ItemsPerSegment: itemsPerSegment}
}

// Enqueue adds an item to the end of the queue
func (q *DQue) Enqueue(obj interface{}) error {

	// This is heavy-handed but its safe
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.firstSegment.dirPath) == 0 {
		// We need to load our state from disk
		if err := q.load(); err != nil {
			return errors.Wrap(err, "error loading the queue: "+q.Name)
		}
	}

	// Add the object to the last segment
	if err := q.lastSegment.add(obj); err != nil {
		return errors.Wrap(err, "error adding item to the last segment")
	}

	// If this segment is full then create a new one
	if q.lastSegment.sizeOnDisk() >= q.Config.ItemsPerSegment {
		// We have filled our last segment to capacity, so create a new one
		seg, err := newQueueSegment(q.fullPath, q.lastSegment.number+1, q.builder)
		if err != nil {
			return errors.Wrap(err, "error creating new queue segment: "+strconv.Itoa(q.lastSegment.number+1))
		}
		q.lastSegment = seg
	}

	return nil
}

// Dequeue removes and returns the first item in the queue.
// If the queue is empty, nil is returned
func (q *DQue) Dequeue() (interface{}, error) {

	// This is heavy-handed but its safe
	q.mutex.Lock()
	defer q.mutex.Unlock()

	if len(q.firstSegment.dirPath) == 0 {
		// We need to load our state from disk
		if err := q.load(); err != nil {
			return nil, errors.Wrap(err, "error loading queue "+q.Name)
		}
	}

	// Remove the first object from the first segment
	obj, err := q.firstSegment.remove()
	if err == emptySegment {
		return nil, EMPTY
	}
	if err != nil {
		return nil, errors.Wrap(err, "error removing item from the first segment")
	}

	// If this segment is empty and we've reached the max for this segment
	// then delete the file and open the next one.
	if q.firstSegment.size() == 0 &&
		q.firstSegment.sizeOnDisk() >= q.Config.ItemsPerSegment {

		// Delete the segment file
		if err := q.firstSegment.delete(); err != nil {
			return obj, errors.Wrap(err, "error deleting queue segment "+q.firstSegment.filePath()+". Queue is in an inconsistent state")
		}

		// We have only one segment and it's now empty so destroy it
		// and create a new one
		if q.firstSegment.number == q.lastSegment.number {

			// Create the next segment
			seg, err := newQueueSegment(q.fullPath, q.firstSegment.number+1, q.builder)
			if err != nil {
				return obj, errors.Wrap(err, "error creating new segment. Queue is in an inconsistent state")
			}
			q.firstSegment = seg
			q.lastSegment = seg

		} else {

			if q.firstSegment.number+1 == q.lastSegment.number {
				// We are down to a 1 segment queue
				q.firstSegment = q.lastSegment
			} else {

				// Open the next segment
				seg, err := openQueueSegment(q.fullPath, q.firstSegment.number+1, q.builder)
				if err != nil {
					return obj, errors.Wrap(err, "error creating new segment. Queue is in an inconsistent state")
				}
				q.firstSegment = seg
			}

		}
	}

	return obj, nil
}

// Size returns the number of items in the queue. This number will be accurate
// only if the itemsPerSegment value has not changed since the queue was last empty.
func (q *DQue) Size() int {
	if q.firstSegment.number == q.lastSegment.number {
		return q.firstSegment.size()
	}
	if q.firstSegment.number == q.lastSegment.number+1 {
		return q.firstSegment.size() + q.lastSegment.size()
	}
	numSegmentsBetween := (q.lastSegment.number - q.firstSegment.number - 1)
	return q.firstSegment.size() + (numSegmentsBetween * q.Config.ItemsPerSegment) + q.lastSegment.size()
}

// load populates the queue from disk
func (q *DQue) load() error {

	// Find all queue files
	files, err := ioutil.ReadDir(q.fullPath)
	if err != nil {
		return errors.Wrap(err, "Unable to read files in "+q.fullPath)
	}

	// Find the smallest and the largest file numbers
	minNum := math.MaxInt32
	maxNum := 0
	for _, f := range files {
		if !f.IsDir() && filePattern.MatchString(f.Name()) {
			// Extract number out of the filename
			fileNumStr := filePattern.FindStringSubmatch(f.Name())[1]
			fileNum, _ := strconv.Atoi(fileNumStr)
			if fileNum > maxNum {
				maxNum = fileNum
			}
			if fileNum < minNum {
				minNum = fileNum
			}
		}
	}

	// If files were found, set q.firstSegment and q.lastSegment
	if maxNum > 0 {

		// We found files
		seg, err := openQueueSegment(q.fullPath, minNum, q.builder)
		if err != nil {
			return errors.Wrap(err, "Unable to create queue segment in "+q.fullPath)
		}
		q.firstSegment = seg

		if minNum == maxNum {
			// We have only one segment so the
			// first and last are the same instance (in this case)
			q.lastSegment = q.firstSegment
		} else {
			// We have multiple segments
			seg, err = openQueueSegment(q.fullPath, maxNum, q.builder)
			if err != nil {
				return errors.Wrap(err, "Unable to create segment for "+q.fullPath)
			}
			q.lastSegment = seg
		}

	} else {
		// We found no files so build a new queue starting with segment 1
		seg, err := newQueueSegment(q.fullPath, 1, q.builder)
		if err != nil {
			return errors.Wrap(err, "Unable to create queue segment in "+q.fullPath)
		}

		// The first and last are the same instance (in this case)
		q.firstSegment = seg
		q.lastSegment = seg
	}

	return nil
}

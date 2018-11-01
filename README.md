# dque - A durable (persistent) queue

dque is an embeddable persistent FIFO queue of like structs.  It uses no 3rd party libraries (except for testing).

The queue is broken up into segments, each of which corresponds with a file on disk.

If there is more than one segment, items are added to the last segment while items are removed from the first segment.


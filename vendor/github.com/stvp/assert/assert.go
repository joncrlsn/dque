package assert

import (
	"fmt"
	"github.com/kr/pretty"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

var errorPrefix = "\U0001F4A9 "

// -- Assertion handlers

func assert(t *testing.T, success bool, f func(), callDepth int) {
	if !success {
		_, file, line, _ := runtime.Caller(callDepth + 1)
		t.Errorf("%s:%d", file, line)
		f()
		t.FailNow()
	}
}

func equal(t *testing.T, expected, got interface{}, callDepth int, messages ...interface{}) {
	fn := func() {
		for _, desc := range pretty.Diff(expected, got) {
			t.Error(errorPrefix, desc)
		}
		if len(messages) > 0 {
			t.Error(errorPrefix, "-", fmt.Sprint(messages...))
		}
	}
	assert(t, isEqual(expected, got), fn, callDepth+1)
}

func notEqual(t *testing.T, expected, got interface{}, callDepth int, messages ...interface{}) {
	fn := func() {
		t.Errorf("%s Unexpected: %#v", errorPrefix, got)
		if len(messages) > 0 {
			t.Error(errorPrefix, "-", fmt.Sprint(messages...))
		}
	}
	assert(t, !isEqual(expected, got), fn, callDepth+1)
}

func contains(t *testing.T, expected, got string, callDepth int, messages ...interface{}) {
	fn := func() {
		t.Errorf("%s Expected to find: %#v", errorPrefix, expected)
		t.Errorf("%s in: %#v", errorPrefix, got)
		if len(messages) > 0 {
			t.Error(errorPrefix, "-", fmt.Sprint(messages...))
		}
	}
	assert(t, strings.Contains(got, expected), fn, callDepth+1)
}

func notContains(t *testing.T, unexpected, got string, callDepth int, messages ...interface{}) {
	fn := func() {
		t.Errorf("%s Expected not to find: %#v", errorPrefix, unexpected)
		t.Errorf("%s in: %#v", errorPrefix, got)
		if len(messages) > 0 {
			t.Error(errorPrefix, "-", fmt.Sprint(messages...))
		}
	}
	assert(t, !strings.Contains(got, unexpected), fn, callDepth+1)
}

func withinDuration(t *testing.T, duration time.Duration, goalTime, gotTime time.Time, callDepth int, messages ...interface{}) {
	fn := func() {
		t.Errorf("%s Expected %v to be within %v of %v", errorPrefix, gotTime, duration, goalTime)
		if len(messages) > 0 {
			t.Error(errorPrefix, "-", fmt.Sprint(messages...))
		}
	}
	actualDuration := goalTime.Sub(gotTime)
	if actualDuration < time.Duration(0) {
		actualDuration = -actualDuration
	}
	assert(t, actualDuration <= duration, fn, callDepth+1)
}

// -- Matching

func isEqual(expected, got interface{}) bool {
	if expected == nil {
		return isNil(got)
	} else {
		return reflect.DeepEqual(expected, got)
	}
}

func isNil(got interface{}) bool {
	if got == nil {
		return true
	}
	value := reflect.ValueOf(got)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// -- Public API

func Equal(t *testing.T, expected, got interface{}, messages ...interface{}) {
	equal(t, expected, got, 1, messages...)
}

func NotEqual(t *testing.T, expected, got interface{}, messages ...interface{}) {
	notEqual(t, expected, got, 1, messages...)
}

func True(t *testing.T, got interface{}, messages ...interface{}) {
	equal(t, true, got, 1, messages...)
}

func False(t *testing.T, got interface{}, messages ...interface{}) {
	equal(t, false, got, 1, messages...)
}

func Nil(t *testing.T, got interface{}, messages ...interface{}) {
	equal(t, nil, got, 1, messages...)
}

func NotNil(t *testing.T, got interface{}, messages ...interface{}) {
	notEqual(t, nil, got, 1, messages...)
}

func Contains(t *testing.T, expected, got string, messages ...interface{}) {
	contains(t, expected, got, 1, messages...)
}

func NotContains(t *testing.T, unexpected, got string, messages ...interface{}) {
	notContains(t, unexpected, got, 1, messages...)
}

func WithinDuration(t *testing.T, duration time.Duration, goalTime, gotTime time.Time, messages ...interface{}) {
	withinDuration(t, duration, goalTime, gotTime, 1, messages...)
}

// Uses recover() to check for a panic.
// Usually used as `defer assert.Panic(expected)`.
func Panic( t *testing.T, expected interface{}, messages ...interface{} ) {
	got := recover()
	equal( t, expected, got, 1, messages... )
}

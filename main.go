package main

import (
	"fmt"
	"log"
	"net/http"
	"sync/atomic"
	"time"
)

type HandleFunc func() string

// Version bundles a handler with its OWN in-flight counter, so we can tell
// exactly when the old version is quiescent. A single global counter (as in the
// first PoC) also counts requests served by the new version and therefore may
// never reach zero under steady load.
type Version struct {
	name     string
	fn       HandleFunc
	inFlight atomic.Int64
}

type Engine struct {
	current atomic.Pointer[Version]
}

// acquire pins the current version for the duration of one request.
// Increment-then-recheck closes the race in which a request loads the old
// pointer, the swap + drain check runs (seeing 0), and only then the request
// increments the old counter and executes old code after "drained".
func (e *Engine) acquire() *Version {
	for {
		v := e.current.Load()
		v.inFlight.Add(1)
		if e.current.Load() == v {
			return v
		}
		v.inFlight.Add(-1) // swapped underneath us: retry on the new version
	}
}

func (v *Version) release() { v.inFlight.Add(-1) }

// swap publishes next and blocks until the previous version has drained
// or the timeout expires.
func (e *Engine) swap(next *Version, timeout time.Duration) error {
	old := e.current.Swap(next)
	deadline := time.Now().Add(timeout)
	for old.inFlight.Load() > 0 {
		if time.Now().After(deadline) {
			return fmt.Errorf("version %s still has %d in-flight requests after %v",
				old.name, old.inFlight.Load(), timeout)
		}
		fmt.Printf("Waiting for %s requests to finish... in-flight: %d\n", old.name, old.inFlight.Load())
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func main() {
	engine := &Engine{}

	v1 := &Version{name: "v1", fn: func() string {
		time.Sleep(500 * time.Millisecond)
		return "v1"
	}}
	v2 := &Version{name: "v2", fn: func() string { return "v2" }}

	engine.current.Store(v1)

	http.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		v := engine.acquire()
		defer v.release()
		fmt.Fprint(w, v.fn())
	})

	go func() {
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("listen: %v", err)
		}
	}()

	time.Sleep(5 * time.Second)

	fmt.Println("Swapping pointer in-memory")
	if err := engine.swap(v2, 10*time.Second); err != nil {
		log.Printf("drain incomplete: %v", err)
	} else {
		fmt.Println("All v1 requests finished. v1 can now be safely garbage-collected.")
	}

	select {}
}

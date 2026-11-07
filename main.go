package main

import (
	"fmt"
	"net/http"
	"sync/atomic"
	"time"
)

type HandleFunc func() string

type Engine struct {
	handler       atomic.Pointer[HandleFunc]
	inFlightCount atomic.Int64
}

func main() {
	engine := &Engine{}

	v1 := HandleFunc(func() string {
		time.Sleep(500 * time.Millisecond)
		return "v1"
	})

	v2 := HandleFunc(func() string {
		return "v2"
	})

	engine.handler.Store(&v1)

	http.HandleFunc("/api", func(w http.ResponseWriter, r *http.Request) {
		engine.inFlightCount.Add(1)
		defer engine.inFlightCount.Add(-1)

		handler := engine.handler.Load()
		if handler != nil {
			fn := *handler
			fmt.Fprint(w, fn())
		}
	})

	go func() {
		http.ListenAndServe(":8080", nil)
	}()

	time.Sleep(5 * time.Second)

	fmt.Println("Swapping pointer in-memory")
	engine.handler.Store(&v2)

	for engine.inFlightCount.Load() > 0 {
		fmt.Printf("Waiting for active requests to finish... Current in-flight: %d\n", engine.inFlightCount.Load())
		time.Sleep(100 * time.Millisecond)
	}

	fmt.Println("All active requests finished. v1 can now be safely garbage-collected.")

	select {}

}

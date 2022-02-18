package util

import (
	"log"
	"time"
)

func timeTrack(start time.Time, name string) {
	elapsed := time.Since(start)
	log.Printf("function %s took %s", name, elapsed)
}

package database

import "sync"

type masterStatus struct {
	mu sync.RWMutex
}

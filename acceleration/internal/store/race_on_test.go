//go:build race

package store

// raceDetector is true in a build with -race, which slows a scan of a long line past a wall-clock bound.
const raceDetector = true

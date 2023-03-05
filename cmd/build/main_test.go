package main

import (
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

func Test_calculateExpectedTiles(t *testing.T) {
	t.Run("whole world to z2", func(t *testing.T) {
		expected := uint32(21)
		zs := []maptile.Zoom{0, 1, 2}
		b := orb.Bound{
			Min: orb.Point{-180.0, -90.0},
			Max: orb.Point{180.0, 90.0},
		}
		actual := calculateExpectedTiles(b, zs)

		if expected != actual {
			t.Fatalf("Expected %d tiles, got %d", expected, actual)
		}
	})

	t.Run("twin cities to z5", func(t *testing.T) {
		expected := uint32(6)
		zs := []maptile.Zoom{0, 1, 2, 3, 4, 5}
		b := orb.Bound{
			Min: orb.Point{-93.5778, 44.6848},
			Max: orb.Point{-92.7482, 45.202},
		}
		actual := calculateExpectedTiles(b, zs)

		if expected != actual {
			t.Fatalf("Expected %d tiles, got %d", expected, actual)
		}
	})
}

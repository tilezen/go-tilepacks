package tilepack

import (
	"math"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/maptile"
)

const webMercatorLatLimit float64 = 85.05112877980659

type GenerateBoxesConsumerFunc func(ll maptile.Tile, ur maptile.Tile, z maptile.Zoom)

type GenerateRangesOptions struct {
	Bounds       orb.Bound
	Zooms        []maptile.Zoom
	ConsumerFunc GenerateBoxesConsumerFunc
}

type GenerateTilesConsumerFunc func(tile maptile.Tile)

type GenerateTilesOptions struct {
	Bounds       orb.Bound
	Zooms        []maptile.Zoom
	ConsumerFunc GenerateTilesConsumerFunc
	InvertedY    bool
}

func GenerateTileRanges(opts *GenerateRangesOptions) {
	bounds := opts.Bounds
	zooms := opts.Zooms
	consumer := opts.ConsumerFunc

	var boxes []orb.Bound
	if bounds.Min.X() > bounds.Max.X() {
		boxes = []orb.Bound{
			{
				Min: orb.Point{-180.0, bounds.Min.Y()},
				Max: bounds.Max,
			},
			{
				Min: bounds.Min,
				Max: orb.Point{180.0, bounds.Max.Y()},
			},
		}
	} else {
		boxes = []orb.Bound{bounds}
	}

	for _, box := range boxes {
		// Clamp the individual boxes to web mercator limits
		clampedBox := orb.Bound{
			Min: orb.Point{
				math.Max(-180.0, box.Min.X()),
				math.Max(-webMercatorLatLimit, box.Min.Y()),
			},
			Max: orb.Point{
				math.Min(180.0-0.00000001, box.Max.X()),
				math.Min(webMercatorLatLimit, box.Max.Y()),
			},
		}

		for _, z := range zooms {
			minTile := maptile.At(clampedBox.Min, z)
			maxTile := maptile.At(clampedBox.Max, z)

			// Flip Y because the XYZ tiling scheme has an inverted Y compared to lat/lon
			maxTile.Y, minTile.Y = minTile.Y, maxTile.Y

			consumer(minTile, maxTile, z)
		}
	}
}

func GenerateTiles(opts *GenerateTilesOptions) {
	rangeOpts := &GenerateRangesOptions{
		Bounds: opts.Bounds,
		Zooms:  opts.Zooms,
	}

	rangeOpts.ConsumerFunc = func(minTile maptile.Tile, maxTile maptile.Tile, z maptile.Zoom) {
		for x := minTile.X; x <= maxTile.X; x++ {
			for y := minTile.Y; y <= maxTile.Y; y++ {
				if opts.InvertedY {
					// https://gist.github.com/tmcw/4954720
					y = uint32(math.Pow(2.0, float64(z))) - 1 - y
				}

				opts.ConsumerFunc(maptile.New(x, y, z))
			}
		}
	}

	GenerateTileRanges(rangeOpts)
}

package tilepack

import (
	"fmt"
	"math"
)

const threeSixty float64 = 360.0
const oneEighty float64 = 180.0
const radius float64 = 6378137.0
const webMercatorLatLimit float64 = 85.05112877980659

type GenerateTilesConsumerFunc func(tile *Tile)

type GenerateTilesOptions struct {
	Bounds       *LngLatBbox
	Zooms        []uint
	ConsumerFunc GenerateTilesConsumerFunc
	InvertedY    bool
}

//Tile struct is the main object we deal with, represents a standard X/Y/Z tile
type Tile struct {
	X, Y, Z uint
}

//LngLat holds a standard geographic coordinate pair in decimal degrees
type LngLat struct {
	Lng, Lat float64
}

//LngLatBbox bounding box of a tile, in decimal degrees
type LngLatBbox struct {
	West, South, East, North float64
}

// Intersects returns true if this bounding box intersects with the other bounding box.
func (b *LngLatBbox) Intersects(o *LngLatBbox) bool {
	latOverlaps := (o.North > b.South) && (o.South < b.North)
	lngOverlaps := (o.East > b.West) && (o.West < b.East)
	return latOverlaps && lngOverlaps
}

//Bbox holds Spherical Mercator bounding box of a tile
type Bbox struct {
	Left, Bottom, Right, Top float64
}

//XY holds a Spherical Mercator point
type XY struct {
	X, Y float64
}

func deg2rad(deg float64) float64 {
	return deg * (math.Pi / oneEighty)
}

func rad2deg(rad float64) float64 {
	return rad * (oneEighty / math.Pi)
}

func min(a uint, b uint) uint {
	if a < b {
		return a
	}
	return b
}

func pow(a, b int) int {
	result := 1

	for 0 != b {
		if 0 != (b & 1) {
			result *= a

		}
		b >>= 1
		a *= a
	}

	return result
}

// GetTile returns a tile for a given longitude latitude and zoom level
func GetTile(lng float64, lat float64, zoom uint) *Tile {

	latRad := deg2rad(lat)
	n := math.Pow(2.0, float64(zoom))
	x := uint(math.Floor((lng + oneEighty) / threeSixty * n))
	y := uint(math.Floor((1.0 - math.Log(math.Tan(latRad)+(1.0/math.Cos(latRad)))/math.Pi) / 2.0 * n))

	return &Tile{x, y, zoom}

}

func GenerateTiles(opts *GenerateTilesOptions) {

	bounds := opts.Bounds
	zooms := opts.Zooms
	consumer := opts.ConsumerFunc

	var boxes []*LngLatBbox
	if bounds.West > bounds.East {
		boxes = []*LngLatBbox{
			&LngLatBbox{-180.0, bounds.South, bounds.East, bounds.North},
			&LngLatBbox{bounds.West, bounds.South, 180.0, bounds.North},
		}
	} else {
		boxes = []*LngLatBbox{bounds}
	}

	for _, box := range boxes {
		// Clamp the individual boxes to web mercator limits
		clampedBox := &LngLatBbox{
			West:  math.Max(-180.0, box.West),
			South: math.Max(-webMercatorLatLimit, box.South),
			East:  math.Min(180.0, box.East),
			North: math.Min(webMercatorLatLimit, box.North),
		}

		for _, z := range zooms {

			ll := GetTile(clampedBox.West, clampedBox.South, z)
			ur := GetTile(clampedBox.East, clampedBox.North, z)

			llx := ll.X
			if llx < 0 {
				llx = 0
			}

			ury := ur.Y
			if ury < 0 {
				ury = 0
			}

			for i := llx; i < min(ur.X+1, 1<<z); i++ {
				for j := ury; j < min(ll.Y+1, 1<<z); j++ {

					x := i
					y := j

					if opts.InvertedY {
						// https://gist.github.com/tmcw/4954720
						y = uint(math.Pow(2.0, float64(z))) - 1 - y
					}

					consumer(&Tile{Z: z, X: x, Y: y})
				}
			}
		}
	}
}

// Equals compares 2 tiles
func (tile *Tile) Equals(t2 *Tile) bool {

	return tile.X == t2.X && tile.Y == t2.Y && tile.Z == t2.Z

}

//Ul returns the upper left corner of the tile decimal degrees
func (tile *Tile) Ul() *LngLat {

	n := math.Pow(2.0, float64(tile.Z))
	lonDeg := float64(tile.X)/n*threeSixty - oneEighty
	latRad := math.Atan(math.Sinh(math.Pi * float64(1-(2*float64(tile.Y)/n))))
	latDeg := rad2deg(latRad)

	return &LngLat{lonDeg, latDeg}
}

//Bounds returns a LngLatBbox for a given tile
func (tile *Tile) Bounds() *LngLatBbox {
	a := tile.Ul()
	shifted := Tile{tile.X + 1, tile.Y + 1, tile.Z}
	b := shifted.Ul()
	return &LngLatBbox{a.Lng, b.Lat, b.Lng, a.Lat}
}

//Parent returns the tile above (i.e. at a lower zoon number) the given tile
func (tile *Tile) Parent() *Tile {

	if tile.Z == 0 && tile.X == 0 && tile.Y == 0 {
		return tile
	}

	if math.Mod(float64(tile.X), 2) == 0 && math.Mod(float64(tile.Y), 2) == 0 {
		return &Tile{tile.X / 2, tile.Y / 2, tile.Z - 1}
	}
	if math.Mod(float64(tile.X), 2) == 0 {
		return &Tile{tile.X / 2, (tile.Y - 1) / 2, tile.Z - 1}
	}
	if math.Mod(float64(tile.X), 2) != 0 && math.Mod(float64(tile.Y), 2) != 0 {
		return &Tile{(tile.X - 1) / 2, (tile.Y - 1) / 2, tile.Z - 1}
	}
	if math.Mod(float64(tile.X), 2) != 0 && math.Mod(float64(tile.Y), 2) == 0 {
		return &Tile{(tile.X - 1) / 2, tile.Y / 2, tile.Z - 1}
	}
	return nil
}

//Children returns the 4 tiles below (i.e. at a higher zoom number) the given tile
func (tile *Tile) Children() []*Tile {

	kids := []*Tile{
		{tile.X * 2, tile.Y * 2, tile.Z + 1},
		{tile.X*2 + 1, tile.Y * 2, tile.Z + 1},
		{tile.X*2 + 1, tile.Y*2 + 1, tile.Z + 1},
		{tile.X * 2, tile.Y*2 + 1, tile.Z + 1},
	}
	return kids
}

// ToString returns a string representation of the tile.
func (tile *Tile) ToString() string {
	return fmt.Sprintf("{%d/%d/%d}", tile.Z, tile.X, tile.Y)
}

//ToXY transforms WGS84 DD to Spherical Mercator meters
func ToXY(ll *LngLat) *XY {

	x := radius * deg2rad(ll.Lng)
	intrx := (math.Pi * 0.25) + (0.5 * deg2rad(ll.Lat))
	y := radius * math.Log(math.Tan(intrx))

	return &XY{x, y}
}

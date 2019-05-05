package tilepack

import (
	"reflect"
	"testing"
)

func TestTile_Bounds(t *testing.T) {
	type fields struct {
		X uint
		Y uint
		Z uint
	}
	tests := []struct {
		name   string
		fields fields
		want   *LngLatBbox
	}{
		{"z0 global", fields{0, 0, 0}, &LngLatBbox{-180.0, -webMercatorLatLimit, 180.0, webMercatorLatLimit}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tile := &Tile{
				X: tt.fields.X,
				Y: tt.fields.Y,
				Z: tt.fields.Z,
			}
			if got := tile.Bounds(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Tile.Bounds() = %v, want %v", got, tt.want)
			}
		})
	}
}

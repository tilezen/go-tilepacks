package tilepack

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/paulmach/orb"
)

type MbtilesMetadata struct {
	metadata map[string]string
}

func NewMbtilesMetadata(metadata map[string]string) *MbtilesMetadata {

	m := &MbtilesMetadata{
		metadata: metadata,
	}

	return m
}

func (m *MbtilesMetadata) Get(k string) (string, bool) {
	v, exists := m.metadata[k]
	return v, exists
}

func (m *MbtilesMetadata) Keys() []string {

	keys := make([]string, 0)

	for k, _ := range m.metadata {
		keys = append(keys, k)
	}

	return keys
}

func (m *MbtilesMetadata) Bounds() (orb.Bound, error) {

	var bounds orb.Bound

	str_bounds, exists := m.Get("bounds")

	if !exists {
		return bounds, fmt.Errorf("Metadata is missing bounds")
	}

	parts := strings.Split(str_bounds, ",")

	if len(parts) != 4 {
		return bounds, fmt.Errorf("Invalid bounds metadata")
	}

	minx, err := strconv.ParseFloat(parts[0], 64)

	if err != nil {
		return bounds, fmt.Errorf("Failed to parse minx, %w", err)
	}

	miny, err := strconv.ParseFloat(parts[1], 64)

	if err != nil {
		return bounds, fmt.Errorf("Failed to parse miny, %w", err)
	}

	maxx, err := strconv.ParseFloat(parts[2], 64)

	if err != nil {
		return bounds, fmt.Errorf("Failed to parse maxx, %w", err)
	}

	maxy, err := strconv.ParseFloat(parts[3], 64)

	if err != nil {
		return bounds, fmt.Errorf("Failed to parse maxy, %w", err)
	}

	min := orb.Point([2]float64{minx, miny})
	max := orb.Point([2]float64{maxx, maxy})

	bounds = orb.Bound{
		Min: min,
		Max: max,
	}

	return bounds, nil
}

func (m *MbtilesMetadata) Center() (orb.Point, error) {

	var pt orb.Point

	str_center, exists := m.Get("center")

	if !exists {
		return pt, fmt.Errorf("Metadata is missing center")
	}

	parts := strings.Split(str_center, ",")

	if len(parts) != 2 {
		return pt, fmt.Errorf("Invalid center metadata")
	}

	x, err := strconv.ParseFloat(parts[0], 64)

	if err != nil {
		return pt, fmt.Errorf("Failed to parse x, %w", err)
	}

	y, err := strconv.ParseFloat(parts[1], 64)

	if err != nil {
		return pt, fmt.Errorf("Failed to parse y, %w", err)
	}

	pt = orb.Point([2]float64{x, y})
	return pt, nil
}

func (m *MbtilesMetadata) MinZoom() (uint, error) {

	str_minzoom, exists := m.Get("minzoom")

	if !exists {
		return 0, fmt.Errorf("Metadata is missing minzoom")
	}

	i, err := strconv.Atoi(str_minzoom)

	if err != nil {
		return 0, fmt.Errorf("Failed to parse minzoom value, %w", err)
	}

	return uint(i), nil
}

func (m *MbtilesMetadata) MaxZoom() (uint, error) {

	str_maxzoom, exists := m.Get("maxzoom")

	if !exists {
		return 0, fmt.Errorf("Metadata is missing maxzoom")
	}

	i, err := strconv.Atoi(str_maxzoom)

	if err != nil {
		return 0, fmt.Errorf("Failed to parse maxzoom value, %w", err)
	}

	return uint(i), nil
}

func (m *MbtilesMetadata) Set(key string, value string) {
	m.metadata[key] = value
}

func (m *MbtilesMetadata) Format() (string, error) {
	return m.metadata["format"], nil
}

func (m *MbtilesMetadata) Name() (string, error) {
	return m.metadata["name"], nil
}

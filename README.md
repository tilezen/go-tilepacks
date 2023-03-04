# go-tilepacks

A Go-based tile downloader that saves to deduplicated files.

## Tools

### build

```
./bin/build -h
Usage of ./bin/build:
  -bounds string
    	Comma-separated bounding box in south,west,north,east format. Defaults to the whole world. (default "-90.0,-180.0,90.0,180.0")
  -bucket string
    	(For metatile, tapalcatl2 generator) The name of the S3 bucket to request t2 archives from.
  -cpuprofile string
    	Enables CPU profiling. Saves the dump to the given path.
  -dsn string
    	Path, or DSN string, to output files.
  -ensure-gzip
    	Ensure tile data is gzipped. Only applies to XYZ tiles. (default true)
  -file-transport-root string
    	The root directory for tiles if -url-template defines a file:// URL scheme
  -generator string
    	Which tile fetcher to use. Options are xyz, metatile, tapalcatl2. (default "xyz")
  -inverted-y
    	Invert the Y-value of tiles to match the TMS (as opposed to ZXY) tile format.
  -layer-name string
    	(For metatile, tapalcatl2 generator) The layer name to use for hash building.
  -materialized-zooms string
    	(For tapalcatl2 generator) Specifies the materialized zooms for t2 archives.
  -output-mode string
    	Valid modes are: disk, mbtiles. (default "mbtiles")
  -path-template string
    	(For metatile, tapalcatl2 generator) The template to use for the path part of the S3 path to the t2 archive.
  -timeout int
    	HTTP client timeout for tile requests. (default 60)
  -url-template string
    	(For xyz generator) URL template to make tile requests with.
  -workers int
    	Number of tile fetch workers to use. (default 25)
  -zooms string
    	Comma-separated list of zoom levels. (default "0,1,2,3,4,5,6,7,8,9,10")
```

## Job Creators

### HTTP

Required arguments:
* `-url-template`: template for an HTTP request per tile. Use `{z}` for zoom, `{x}` for x or column, and `{y}` for y or row.

### Metatile

Required arguments:
* `-bucket`: specify the S3 bucket to fetch from
* `-layer-name`: a string name for the layer part of the path template.
* `-path-template`: template for the path part of the request to S3. Use `{z}` for zoom, `{x}` for x or column, `{y}` for y or row, `{l}` for layer name, and `{h}` for the hash prefix (see below).

#### Hash Prefix

The "hash prefix" is used by the Tilezen tiler to spread the load of tile fetches across S3 shards. In practice, it is the first 5 characters of the MD5 sum of the evaluated template string `{zoom}/{x}/{y}.zip` (e.g. `12/242/533.zip`). 

### Tapalcatl 2

## Outputters

The following tile "outputter" are supported, as defined by the `-mode` flag:

### disk

Clone tiles to a local directory. Valid `-dsn` strings must be in the form of:

```
-dsn 'root={PATH_TO_DIRECTORY_ROOT} format={TILE_FORMAT}'
```

### mbtiles

Clone tiles to a MBTiles (SQLite) database. Valid `-dsn` strings must be in the form of:

```
-dsn '{PATH_TO_MBTILES_DATABASE}'
```

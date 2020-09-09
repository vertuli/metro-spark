# metro

Getting pings from [LA Metro's API](http://api.metro.net) and using Spark's Structured Streaming to create a history of vehicle paths.

Using [Spark 3.0.0](https://spark.apache.org/docs/3.0.0/) and Li Haoyi's shiny new build tool, [mill](http://www.lihaoyi.com/mill/)!


## pings
- `mill metro.pings` - gets pings from `api.metro.net` for the agencies `lametro` and `lametro-rail` and writes them as JSON lines to `data/pings/`, partitioned by `agency_id` and `localdate`.
- `mill metro.pings.test` - runs pings tests

## paths
- `mill metro.paths` - starts Structured Streaming process, viewable in the UI at `http://localhost:4040/StreamingQuery/`, and writes paths as JSON lines to `data/paths/`, partitioned by `agency_id` and `local_date`.
- `mill metro.paths.geojson` - creates `.geojson` files in `data/geojson/` (TODO: for paths specified by command line options)
- `mill metro.paths.test` - runs paths tests

## Visualization

![Animated map of LA Metro Buses](kepler.gif)
The output GeoJSON can be saved as a file and fed directly into [kepler.gl](http://kepler.gl/demo) for animated path visualization.

# 1️⃣🐝🏎️ The One Billion Row Challenge

The One Billion Row Challenge (1BRC) is a fun exploration of how far modern Java can be pushed for aggregating one billion rows from a text file. This repository is aimed to do the same in Go.

The original repository is at https://github.com/gunnarmorling/1brc, it contains the instructions to generate a `measurements.txt` file that the program will read.

The text file contains temperature values for a range of weather stations.
Each row is one measurement in the format `<string: station name>;<double: measurement>`, with the measurement value having exactly one fractional digit.
The following shows ten rows as an example:

```
Hamburg;12.0
Bulawayo;8.9
Palembang;38.8
St. John's;15.2
Cracow;12.6
Bridgetown;26.9
Istanbul;6.2
Roseau;34.4
Conakry;31.2
Istanbul;23.0
```

The task is to write a Go program which reads the file, calculates the min, mean, and max temperature value per weather station, and emits the results on stdout like this
(i.e. sorted alphabetically by station name, and the result values per station in the format `<min>/<mean>/<max>`, rounded to one fractional digit):

```
{Abha=-23.0/18.0/59.2, Abidjan=-16.2/26.0/67.3, Abéché=-10.0/29.4/69.0, Accra=-10.1/26.4/66.4, Addis Ababa=-23.7/16.0/67.0, Adelaide=-27.8/17.3/58.5, ...}
```

## Build & Run

```shell
$ go build -o build/onebilly ./main.go && ./build/onebilly
```


## Profiling

To understand where the performance bottlenecks are, a pprof file is generated at `./var/cpu.pprof`.

To spawn an http server to inspect it run:

```shell
$ go tool pprof -http=:7777 ./var/cpu.pprof
```

Then go to `http://localhost:7777`.

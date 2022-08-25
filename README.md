# S3S

S3S is a go binary instead of [vast-engineering/s3select](https://github.com/vast-engineering/s3select).

## Feature

- [x] Input JSON to Output JSON
## Usage

```console
$ ./s3s --help
NAME:
   s3s - Easy S3 select like searching in directories

USAGE:
   s3s [global options] command [command options] [arguments...]

VERSION:
   v0.1.0

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --delve                         like directory move before querying (default: false)
   --help, -h                      show help (default: false)
   --query value, -q value         a query for S3 Select (default: "SELECT * FROM S3Object s")
   --region value                  region of target s3 bucket exist (default: ENV["AWS_REGION"])
   --thread_count value, -t value  max number of api requests to concurrently (default: 150)
   --version, -v                   print the version (default: false)
   --where value, -w value         WHERE part of the query
```

```console
$ s3s s3://bucket/prefix
{"time":1654848930,"type":"speak"}
{"time":1654848969,"type":"sleep"}

// $ s3s s3://bucket/prefix_A s3://bucket/prefix_B s3://bucket/prefix_C
```

```console
$ s3s -q 'SELECT * FROM S3Object s WHERE s.type = "speak"' s3://bucket/prefix
{"time":1654848930,"type":"speak"}

// alternate
// $ s3s -w 's.type = "speak"' s3://bucket/prefix
```

### `-delve`, like directory move before querying

search from prefix

```console
$ s3s -delve s3://bucket/prefix
```

search from bucket list

```console
$ s3s -delve
```

```
  bucket/prefix/C/
  bucket/prefix/B/
  bucket/prefix/A/        # delve more lower path than this prefix
  Query↵ (s3://bucket/prefix/) # choose and execute s3select this prefix
> ←Back upper path        # back to parent prefix
5/5
>
```

Querying after Enter.

```
{"time":1654848930,"type":"speak"}
{"time":1654848969,"type":"sleep"}

...

bucket/prefix/A/ (print path as stderr at end)
```

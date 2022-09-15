# S3S

S3S is a go binary instead of [vast-engineering/s3select](https://github.com/vast-engineering/s3select).

## Feature

- [x] Input JSON to Output JSON
## Usage

```console
$ s3s --help
NAME:
   s3s - Easy S3 select like searching in directories

USAGE:
   s3s [global options] command [command options] [arguments...]

VERSION:
   v0.3.0

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --alb-logs, --alb_logs          (default: false)
   --cf-logs, --cf_logs            (default: false)
   --count, -c                     max number of results from each key to return (default: false)
   --csv                           (default: false)
   --delve                         like directory move before querying (default: false)
   --help, -h                      show help (default: false)
   --limit value, -l value         max number of results from each key to return (default: 0)
   --max-retries value, -M value   max number of api requests to retry (default: 20)
   --query value, -q value         a query for S3 Select
   --region value                  region of target s3 bucket exist (default: ENV["AWS_REGION"])
   --thread-count value, -t value  max number of api requests to concurrently (default: 150)
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

### CSV support

`--csv` option is no header csv only

```console
// 122, hello
$ s3s s3://bucket/prefix
{"_1":122,"_2":"hello"}
```

`--alb-logs` or `--cf-logs` option is tagging available instead of _1, _2, etc

- [Application Load Balancer Format](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html)
- [CloudFront Format](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html)

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

bucket/prefix/A/ (print path to stderr at end)
```

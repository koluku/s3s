# s3s

**s3s** is a go binary instead of [vast-engineering/s3select](https://github.com/vast-engineering/s3select).

## Features

s3s query all files lower than S3 prefix.

Available below:

- Input JSON to Output JSON
- Input CSV to Output JSON
- Input Application Load Balancer Logs to Output JSON
- Input CloudFront Logs to Output JSON

## Usage

```console
$ s3s --help
NAME:
   s3s - Easy S3 select like searching in directories

USAGE:
   s3s [global options] command [command options] [arguments...]

VERSION:
   v0.4.0

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --alb-logs, --alb_logs          (default: false)
   --cf-logs, --cf_logs            (default: false)
   --count, -c                     max number of results from each key to return (default: false)
   --csv                           (default: false)
   --debug                         erorr check for developper (default: false)
   --delve                         like directory move before querying (default: false)
   --dry-run, --dry_run            pre request for s3 select (default: false)
   --duration value                from current time if alb or cf (ex: "2h3m") (default: 0s)
   --help, -h                      show help (default: false)
   --limit value, -l value         max number of results from each key to return (default: 0)
   --max-retries value, -M value   max number of api requests to retry (default: 20)
   --query value, -q value         a query for S3 Select
   --region value                  region of target s3 bucket exist (default: ENV["AWS_REGION"])
   --since value                   end at if alb or cf (ex: "2006-01-02 15:04:05")
   --thread-count value, -t value  max number of api requests to concurrently (default: 150)
   --until value                   start at if alb or cf (ex: "2006-01-02 15:04:05")
   --version, -v                   print the version (default: false)
   --where value, -w value         WHERE part of the query
```

s3s is execution S3 Select from json to json (default).

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

s3s can execute S3 Select from csv to json when `--csv` option enabled.

```console
// 122, hello
$ s3s s3://bucket/prefix
{"_1":122,"_2":"hello"}
```

### ALB and CF logs support

`--alb-logs` is a format for Application Load Balancer (ALB).
`--cf-logs` is a format for CloudFront (CF).

Each options are tagging available instead of `_1`, `_2`, etc.

- [Application Load Balancer Format](https://docs.aws.amazon.com/elasticloadbalancing/latest/application/load-balancer-access-logs.html)
- [CloudFront Format](https://docs.aws.amazon.com/AmazonCloudFront/latest/DeveloperGuide/AccessLogs.html)

And also, `--where` replace column names to column numbers.
But `--query` does not replace columns for execution raw query.

```console
// below query is same as $ s3s --alb-logs --query="'SELECT * FROM S3Object s WHERE s.`_2` = '2022-09-01T00:00:00.000000Z'" s3://prefix
$ s3s --alb-logs --where="s.`time` = '2022-09-01T00:00:00.000000Z'" s3://prefix
```

|index|ALB|CF|
|-|-|-|
|_1|type|date|
|_2|time|time|
|_3|elb|x-edge-location|
|_4|client:port|sc-bytes|
|_5|target:port|c-ip|
|_6|request_processing_time|cs-method|
|_7|target_processing_time|cs(Host)|
|_8|response_processing_time|cs-uri-stem|
|_9|elb_status_code|sc-status|
|_10|target_status_code|cs(Referer)|
|_11|received_bytes|cs(User-Agent)|
|_12|sent_bytes|cs-uri-query|
|_13|request|cs(Cookie)|
|_14|user_agent|x-edge-result-type|
|_15|ssl_cipher|x-edge-request-id|
|_16|ssl_protocol|x-host-header|
|_17|target_group_arn|cs-protocol|
|_18|trace_id|cs-bytes|
|_19|domain_name|time-taken|
|_20|chosen_cert_arn|x-forwarded-for|
|_21|matched_rule_priority|ssl-protocol|
|_22|request_creation_time|ssl-cipher|
|_23|actions_executed|x-edge-response-result-type|
|_24|redirect_url|cs-protocol-version|
|_25|error_reason|fle-status|
|_26|target:port_list|fle-encrypted-fields|
|_27|target_status_code_list|c-port|
|_28|classification|time-to-first-byte|
|_29|classification_reason|x-edge-detailed-result-type|
|_30||sc-content-type|
|_31||sc-range-start|
|_32||sc-range-end|

Support log range when alb and cf.
time format is `2006-01-02 15:04:05` as UTC.

- `--duration` is a duration from now.
- `--since` is start time
- `--until` is end time

However, s3s stop when you target cloudfront and using `--duration` or `--since` only, because s3s hit too many keys.

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

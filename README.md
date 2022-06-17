# S3S

S3S is a go binary instead of [vast-engineering/s3select](https://github.com/vast-engineering/s3select).

## Feature

- [x] Input JSON to Output JSON
## Usage

```console
$ ./s3s help
NAME:
   s3s - Easy S3 Select like searching directory

USAGE:
   s3s [global options] command [command options] [arguments...]

VERSION:
   0.0.0

COMMANDS:
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h               show help (default: false)
   --query value, -q value  SQL query for s3 select (default: "SELECT * FROM S3Object s")
   --region value           region of target s3 bucket exist
   --version, -v            print the version (default: false)
   --where value, -w value  WHERE part of the SQL query
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


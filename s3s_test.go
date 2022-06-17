package s3s

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type mockS3API struct {
	S3API
}

func (m mockS3API) ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error) {
	return &s3.ListObjectsV2Output{
		Contents: []types.Object{
			{Key: aws.String("logs/2022/04/01/00/00/my-health-logs.gz")},
			{Key: aws.String("logs/2022/04/01/00/01/my-health-logs.gz")},
			{Key: aws.String("logs/2022/04/01/00/02/my-health-logs.gz")},
		},
	}, nil
}

func TestGetS3Keys(t *testing.T) {
	app := &App{s3client: &mockS3API{}}
	ctx := context.Background()
	bucket := ""
	prefix := ""
	keys, err := GetS3Keys(ctx, app, bucket, prefix)
	if err != nil {
		t.Errorf("Expected no error, but got %v.", err)
	}
	if len(keys) == 0 {
		t.Errorf("Expected list of ec2 instance id, but got empty.")
	}

	expectedKeys := []string{
		"logs/2022/04/01/00/00/my-health-logs.gz",
		"logs/2022/04/01/00/01/my-health-logs.gz",
		"logs/2022/04/01/00/02/my-health-logs.gz",
	}
	for i, key := range keys {
		if expectedKeys[i] != key {
			t.Errorf("Expected %s, but got %s.", expectedKeys[i], key)
		}
	}
}

func TestSuggestCompressionType(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want types.CompressionType
	}{
		{
			name: "sss",
			key:  "all-logs/2022/06/16/1626.gz",
			want: types.CompressionTypeGzip,
		},
		{
			name: "aaa",
			key:  "all-logs/2022/06/16/1626.bz2",
			want: types.CompressionTypeBzip2,
		},
		{
			name: "cc",
			key:  "all-logs/2022/06/16/1626.json",
			want: types.CompressionTypeNone,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := suggestCompressionType(tt.key)

			if got != tt.want {
				t.Errorf("%s", tt.name)
			}
		})
	}
}

package s3s

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestSuggestCompressionType(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want types.CompressionType
	}{
		{
			name: "GZIP",
			key:  "all-logs/2022/06/16/1626.gz",
			want: types.CompressionTypeGzip,
		},
		{
			name: "BZIP2",
			key:  "all-logs/2022/06/16/1626.bz2",
			want: types.CompressionTypeBzip2,
		},
		{
			name: "JSON as None",
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

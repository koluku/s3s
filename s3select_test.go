package s3s

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestSuggestCompressionType(t *testing.T) {
	cases := []struct {
		name  string
		input *s3SelectInput
		want  types.CompressionType
	}{
		{
			name: "GZIP",
			input: &s3SelectInput{
				Key: "all-logs/2022/06/16/1626.gz",
			},
			want: types.CompressionTypeGzip,
		},
		{
			name: "BZIP2",
			input: &s3SelectInput{
				Key: "all-logs/2022/06/16/1626.bz2",
			},
			want: types.CompressionTypeBzip2,
		},
		{
			name: "JSON as None",
			input: &s3SelectInput{
				Key: "all-logs/2022/06/16/1626.json",
			},
			want: types.CompressionTypeNone,
		},
	}

	for _, tt := range cases {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := tt.input.suggestCompressionType()

			if got != tt.want {
				t.Errorf("%s", tt.name)
			}
		})
	}
}

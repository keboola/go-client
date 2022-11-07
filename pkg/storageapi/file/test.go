package file

import (
	"compress/gzip"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/keboola/go-client/pkg/storageapi"
)

type UploadTestCase struct {
	Name string
	File *storageapi.File
}

func UploadTestCases() []*UploadTestCase {
	return []*UploadTestCase{
		{
			Name: "default",
			File: &storageapi.File{
				IsPublic:    false,
				IsSliced:    false,
				IsEncrypted: false,
				Name:        "test",
				Tags:        []string{"tag1", "tag2"},
			},
		},
		{
			Name: "encrypted",
			File: &storageapi.File{
				IsPublic:    false,
				IsSliced:    false,
				IsEncrypted: true,
				Name:        "test",
				Tags:        []string{"tag1", "tag2"},
			},
		},
		/*{
			Name: "public",
			File: &storageapi.File{
				IsPublic:    true,
				IsSliced:    false,
				IsEncrypted: false,
				Name:        "test",
				Tags:        []string{"tag1", "tag2"},
			},
		},
		{
			Name: "encrypted and public",
			File: &storageapi.File{
				IsPublic:    true,
				IsSliced:    false,
				IsEncrypted: true,
				Name:        "test",
				Tags:        []string{"tag1", "tag2"},
			},
		},*/
	}
}

func GetUploadedFile(t *testing.T, file *storageapi.File) (string, error) {
	t.Helper()

	resp, err := http.Get(file.Url) //nolint:noctx
	assert.NoError(t, err)
	defer resp.Body.Close()
	assert.NoError(t, err)
	fileRes, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	fileResStr := string(fileRes)
	if strings.HasPrefix(fileResStr, "<?xml") {
		return "", fmt.Errorf("getting url responsed: %s", fileResStr)
	}
	return fileResStr, nil
}

func Decompress(t *testing.T, str string) string {
	t.Helper()

	sr := strings.NewReader(str)
	gr, err := gzip.NewReader(sr)
	assert.NoError(t, err)
	defer gr.Close()
	o, err := io.ReadAll(gr)
	assert.NoError(t, err)
	return string(o)
}

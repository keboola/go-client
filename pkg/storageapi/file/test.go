package file

import (
	"bytes"
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
	fileResponse, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)
	fileResponseStr := string(fileResponse)
	if strings.HasPrefix(fileResponseStr, "<?xml") {
		return "", fmt.Errorf("getting uploaded file on url %s failed with response: %s", file.Url, fileResponseStr)
	}

	gr, err := gzip.NewReader(bytes.NewReader(fileResponse))
	assert.NoError(t, err)
	o, err := io.ReadAll(gr)
	assert.NoError(t, err)
	return string(o), nil
}

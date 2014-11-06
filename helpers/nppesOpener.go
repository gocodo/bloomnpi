package helpers

import (
	"archive/zip"
	"io"
	"regexp"
	"errors"
)

type ReadCloser struct {
	zipReader *zip.ReadCloser
	fileReader *io.ReadCloser
	Filename string
}

// select npidata_20050523-20141012.csv but not npidata_20050523-20141012FileHeader.csv
var npiFileRegexp = regexp.MustCompile(`npidata.+\d+\.csv`)

func npiFile(r *zip.ReadCloser) (*zip.File, error) {
	for _, f := range r.File {
		if npiFileRegexp.MatchString(f.Name) {
			return f, nil
		}
	}

	return nil, errors.New("No NPI data found in zip")
}

func OpenReader (filename string) (*ReadCloser, error) {
	r, err := zip.OpenReader(filename)
	if err != nil {
		return nil, err
	}

	file, err := npiFile(r)
	if err != nil {
		return nil, err
	}

	rc, err := file.Open()
	if err != nil {
		return nil, err
	}

	return &ReadCloser{r, &rc, file.Name}, nil
}

func (r *ReadCloser) Close() error {
	fileErr := (*r.fileReader).Close()
	zipErr := (*r.zipReader).Close()

	if fileErr != nil {
		return fileErr
	}

	if zipErr != nil {
		return zipErr
	}

	return nil
}

func (r *ReadCloser) Read(p []byte) (n int, err error) {
	return (*r.fileReader).Read(p)
}
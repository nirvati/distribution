// Package bunny provides a storagedriver.StorageDriver implementation to
// store blobs in Bunny.net's Object Storage.
package bunny

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	storagedriver "github.com/distribution/distribution/v3/registry/storage/driver"
	"github.com/distribution/distribution/v3/registry/storage/driver/factory"
	bunny "github.com/l0wl3vel/bunny-storage-go-sdk"
	"github.com/sirupsen/logrus"
)

func init() {
	factory.Register(driverName, &bunnyDriverFactory{})
}

const (
	driverName = "bunny"
)

type bunnyDriverFactory struct{}

func (factory *bunnyDriverFactory) Create(ctx context.Context, parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	params, err := NewParameters(parameters)
	if err != nil {
		return nil, err
	}
	return New(ctx, params)
}

func New(ctx context.Context, params *DriverParameters) (storagedriver.StorageDriver, error) {
	client := bunny.NewClient(*params.Hostname.JoinPath(params.StorageZone), params.apiKey)
	logrusLogger := logrus.New()
	logrusLogger.SetLevel(logrus.InfoLevel)
	client.WithLogger(logrusLogger)
	return &driver{
		pullZone: params.Pullzone,
		client:   client,
	}, nil
}

var _ storagedriver.StorageDriver = &driver{}

type driver struct {
	pullZone url.URL
	client   bunny.Client
}

// Delete implements driver.StorageDriver.
func (d *driver) Delete(ctx context.Context, path string) error {
	info, err := d.client.Describe(path)
	if err != nil && err.Error() == "Not Found" {
		// Don't fail for not found, just return nil
		return nil
	}
	if err != nil {
		return err
	}
	return d.client.Delete(path, info.IsDirectory)
}

// GetContent implements driver.StorageDriver.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	fmt.Println("Getting content for path:", path)
	res, err := d.client.Download(path)
	if err != nil {
		fmt.Println("Error downloading content:", err)
		return nil, err
	}
	return res, nil
}

// List implements driver.StorageDriver.
func (d *driver) List(ctx context.Context, searchPath string) ([]string, error) {
	fmt.Println("Listing contents of path:", searchPath)
	entries, err := d.client.List(searchPath)
	fmt.Println("Entries found:", len(entries))
	if err != nil {
		fmt.Println("Error listing contents:", err)
		return nil, err
	}
	var result []string
	for _, entry := range entries {
		filepath := path.Join(entry.Path, entry.ObjectName)
		parts := strings.Split(filepath, "/")
		if parts[0] == "" {
			parts = parts[1:]
		}
		if len(parts) > 0 {
			parts = parts[1:]
		}
		filepath = "/" + path.Join(parts...)
		fmt.Println("Entry path:", filepath)
		result = append(result, filepath)
	}
	return result, nil
}

// Move implements driver.StorageDriver.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	fmt.Println("Moving from", sourcePath, "to", destPath)
	// Bunny Storage does not support moving files directly, so we need to download and re-upload.
	content, err := d.client.Download(sourcePath)
	if err != nil {
		fmt.Println("Error downloading source file:", err)
		return err
	}
	err = d.client.Upload(destPath, content, true)
	if err != nil {
		fmt.Println("Error uploading to destination path:", err)
		return err
	}
	return d.client.Delete(sourcePath, false) // Delete the source file after moving

}

// Name implements driver.StorageDriver.
func (d *driver) Name() string {
	return driverName
}

// PutContent implements driver.StorageDriver.
func (d *driver) PutContent(ctx context.Context, path string, content []byte) error {
	fmt.Println("Putting content to path:", path)
	err := d.client.Upload(path, content, true)
	if err != nil {
		fmt.Println("Error uploading content:", err)
		return err
	}
	fmt.Println("Content uploaded successfully to path:", path)
	return nil
}

// Reader implements driver.StorageDriver.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	fmt.Println("Creating reader for path:", path, "with offset:", offset)
	info, err := d.client.Describe(path)
	if err != nil {
		if err.Error() == "Not Found" {
			fmt.Println("File not found:", path)
			return nil, storagedriver.PathNotFoundError{Path: path}
		}
		fmt.Println("Error describing file:", err)
		return nil, err
	}
	return &bunnyFileReader{
		client:   d.client,
		path:     path,
		offset:   offset,
		fileSize: int64(info.Length),
	}, nil
}

// RedirectURL implements driver.StorageDriver.
func (d *driver) RedirectURL(r *http.Request, path string) (string, error) {
	fmt.Println("Creating redirect URL for path:", path)
	return d.pullZone.JoinPath(path).String(), nil
}

// Stat implements driver.StorageDriver.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	fmt.Println("Getting file info for path:", path)
	info, err := d.client.Describe(path)
	if err != nil {
		if err.Error() == "Not Found" {
			fmt.Println("File not found:", path)
			return nil, storagedriver.PathNotFoundError{Path: path}
		} else {
			fmt.Println("Error describing file:", err)
			return nil, err
		}
	}
	modTime, err := time.Parse("2006-01-02T15:04:05.000", info.LastChanged)
	if err != nil {
		modTime, err = time.Parse("2006-01-02T15:04:05.00", info.LastChanged)
		if err != nil {
			modTime, err = time.Parse("2006-01-02T15:04:05.0", info.LastChanged)
			if err != nil {
				fmt.Println("Error parsing modification time:", err)
				return nil, err
			}
		}
	}
	return &storagedriver.FileInfoInternal{
		FileInfoFields: storagedriver.FileInfoFields{
			Path:    info.Path,
			Size:    int64(info.Length),
			IsDir:   info.IsDirectory,
			ModTime: modTime,
		},
	}, nil
}

// Walk implements driver.StorageDriver.
func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn, options ...func(*storagedriver.WalkOptions)) error {
	return storagedriver.WalkFallback(ctx, d, path, f, options...)
}

// Writer implements driver.StorageDriver.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	if append {
		fmt.Println("Appending to existing file:", path)
		// Get the current file as the starting buffer
		content, err := d.client.Download(path)
		if err != nil {
			return nil, err
		}
		return &BunnyFileWriter{
			client: d.client,
			path:   path,
			buffer: content,
		}, nil
	} else {
		fmt.Println("Creating new file:", path)
		// Start with an empty buffer for new files
		return &BunnyFileWriter{
			client: d.client,
			path:   path,
			buffer: []byte{},
		}, nil
	}
}

type bunnyFileReader struct {
	client   bunny.Client
	path     string
	offset   int64
	fileSize int64
}

// Close implements io.ReadCloser.
func (b *bunnyFileReader) Close() error {
	fmt.Println("Closing BunnyFileReader for path:", b.path)
	return nil
}

// Read implements io.ReadCloser.
func (b *bunnyFileReader) Read(p []byte) (n int, err error) {
	if b.offset >= b.fileSize {
		fmt.Println("Reached end of file for path:", b.path)
		return 0, io.EOF // End of file
	}
	fmt.Println("Reading from BunnyFileReader for path:", b.path, "at offset:", b.offset, "with buffer size:", len(p))
	fmt.Println("Range start:", b.offset, "Range end:", b.offset+int64(len(p)))
	data, err := b.client.DownloadPartial(b.path, b.offset, b.offset+int64(len(p)))
	if err != nil {
		fmt.Println("Error reading from BunnyFileReader:", err)
		return 0, err
	}
	n = copy(p, data)
	b.offset += int64(n)
	if n == 0 {
		return 0, io.EOF
	}
	return n, nil
}

var _ io.ReadCloser = &bunnyFileReader{}

type BunnyFileWriter struct {
	client      bunny.Client
	path        string
	buffer      []byte
	isCancelled bool
}

// Cancel implements driver.FileWriter.
func (b *BunnyFileWriter) Cancel(context.Context) error {
	fmt.Println("Cancelling BunnyFileWriter for path:", b.path)
	b.isCancelled = true
	return nil
}

// Commit implements driver.FileWriter.
func (b *BunnyFileWriter) Commit(context.Context) error {
	fmt.Println("Comitting to BunnyFileWriter for path:", b.path)
	return nil
}

// Size implements driver.FileWriter.
func (b *BunnyFileWriter) Size() int64 {
	return int64(len(b.buffer))
}

var _ storagedriver.FileWriter = &BunnyFileWriter{}

func (b *BunnyFileWriter) Write(p []byte) (n int, err error) {
	fmt.Println("Writing to BunnyFileWriter for path:", b.path)
	b.buffer = append(b.buffer, p...)
	return len(p), nil
}

func (b *BunnyFileWriter) Close() error {
	fmt.Println("Closing BunnyFileWriter for path:", b.path)
	/*if len(b.buffer) == 0 {
		fmt.Println("No data to write, skipping upload.")
		return nil // Nothing to write
	}*/
	if b.isCancelled {
		fmt.Println("Upload cancelled, not writing to Bunny.")
		return nil // If cancelled, do not write
	}
	err := b.client.Upload(b.path, b.buffer, true)
	fmt.Println("Upload completed for path:", b.path)
	if err != nil {
		fmt.Println("Error uploading to Bunny:", err)
	}
	return err
}

// Package b2 implements Storage based on an Backblaze B2 bucket.
package b2

import (
	"context"
	"fmt"

	"github.com/Backblaze/blazer/b2"
	"github.com/pkg/errors"
	"gopkg.in/kothar/go-backblaze.v0"

	"github.com/kopia/kopia/internal/iocopy"
	"github.com/kopia/kopia/internal/timestampmeta"
	"github.com/kopia/kopia/repo/blob"
	"github.com/kopia/kopia/repo/blob/retrying"
)

const (
	b2storageType = "b2"

	timeMapKey = "kopia-mtime" // case is important, must be all-lowercase
)

type b2Storage struct {
	Options
	blob.DefaultProviderImplementation

	cli    *b2.Client
	bucket *b2.Bucket
}

func (s *b2Storage) GetBlob(ctx context.Context, id blob.ID, offset, length int64, output blob.OutputBuffer) error {
	fileName := s.getObjectNameString(id)

	if offset < 0 {
		return blob.ErrInvalidRange
	}

	if length == 0 {
		return nil
	}

	output.Reset()

	attempt := func() error {
		r := s.bucket.Object(fileName).NewRangeReader(ctx, offset, length)
		defer r.Close() //nolint:errcheck

		return iocopy.JustCopy(output, r)
	}

	if err := attempt(); err != nil {
		return translateError(err)
	}

	//nolint:wrapcheck
	return blob.EnsureLengthExactly(output.Length(), length)
}

func (s *b2Storage) GetMetadata(ctx context.Context, id blob.ID) (blob.Metadata, error) {
	fileName := s.getObjectNameString(id)
	object := s.bucket.Object(fileName)
	attrs, err := object.Attrs(ctx)

	if err != nil {
		return blob.Metadata{}, translateError(err)
	}

	bm := blob.Metadata{
		BlobID:    id,
		Length:    attrs.Size,
		Timestamp: attrs.UploadTimestamp,
	}

	if t, ok := timestampmeta.FromValue(attrs.Info[timeMapKey]); ok {
		bm.Timestamp = t
	}

	return bm, nil
}

func translateError(err error) error {
	//	if err == nil {
	//		return nil
	//	}
	//
	//	var b2err *backblaze.B2Error
	//	if errors.As(err, &b2err) {
	//		switch b2err.Status {
	//		case http.StatusNotFound:
	//			// Normal "not found". That's fine.
	//			return blob.ErrBlobNotFound
	//
	//		case http.StatusBadRequest:
	//			if b2err.Code == "already_hidden" || b2err.Code == "no_such_file" {
	//				// Special case when hiding a file that is already hidden. It's basically
	//				// not found.
	//				return blob.ErrBlobNotFound
	//			}
	//
	//			if b2err.Code == "bad_request" && strings.HasPrefix(b2err.Message, "Bad file") {
	//				// returned in GetMetadata() when fileId is not found.
	//				return blob.ErrBlobNotFound
	//			}
	//
	//		case http.StatusRequestedRangeNotSatisfiable:
	//			return blob.ErrInvalidRange
	//		}
	//	}
	//
	return err
}

func (s *b2Storage) PutBlob(ctx context.Context, id blob.ID, data blob.Bytes, opts blob.PutOptions) error {
	switch {
	case opts.HasRetentionOptions():
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "blob-retention")
	case opts.DoNotRecreate:
		return errors.Wrap(blob.ErrUnsupportedPutBlobOption, "do-not-recreate")
	}

	fileName := s.getObjectNameString(id)
	object := s.bucket.Object(fileName)
	reader := data.Reader()
	writer := object.NewWriter(ctx, b2.WithAttrsOption(&b2.Attrs{Info: timestampmeta.ToMap(opts.SetModTime, timeMapKey)}))

	defer reader.Close() //nolint:errcheck

	if err := iocopy.JustCopy(writer, reader); err != nil {
		return translateError(err)
	}

	if err := writer.Close(); err != nil {
		return translateError(err)
	}

	if opts.GetModTime != nil {
		attrs, err := object.Attrs(ctx)
		if err != nil {
			return translateError(err)
		}

		*opts.GetModTime = attrs.UploadTimestamp
	}

	return nil
}

func (s *b2Storage) DeleteBlob(ctx context.Context, id blob.ID) error {
	object := s.bucket.Object(s.getObjectNameString(id))
	err := object.Hide(ctx)
	err = translateError(err)

	if errors.Is(err, blob.ErrBlobNotFound) {
		// Deleting failed because it already is deleted? Fine.
		return nil
	}

	return nil
}

func (s *b2Storage) getObjectNameString(id blob.ID) string {
	return s.Prefix + string(id)
}

func (s *b2Storage) ListBlobs(ctx context.Context, prefix blob.ID, callback func(blob.Metadata) error) error {
	const maxFileQuery = 1000

	fullPrefix := s.getObjectNameString(prefix)
	iter := s.bucket.List(ctx, b2.ListPageSize(maxFileQuery), b2.ListPrefix(fullPrefix))

	for iter.Next() {
		attrs, err := iter.Object().Attrs(ctx)
		if err != nil {
			return translateError(err)
		}

		bm := blob.Metadata{
			BlobID:    blob.ID(attrs.Name[len(s.Prefix):]),
			Length:    attrs.Size,
			Timestamp: attrs.LastModified,
		}

		if t, ok := timestampmeta.FromValue(attrs.Info[timeMapKey]); ok {
			bm.Timestamp = t
		}

		if err := callback(bm); err != nil {
			return err
		}
	}

	if iter.Err() != nil {
		return errors.Wrapf(iter.Err(), "unable to iterate over bucket %s", s.BucketName)
	}

	return nil
}

func (s *b2Storage) ConnectionInfo() blob.ConnectionInfo {
	return blob.ConnectionInfo{
		Type:   b2storageType,
		Config: &s.Options,
	}
}

func (s *b2Storage) DisplayName() string {
	return fmt.Sprintf("B2: %v", s.BucketName)
}

func (s *b2Storage) String() string {
	return fmt.Sprintf("b2://%s/%s", s.BucketName, s.Prefix)
}

// New creates new B2-backed storage with specified options.
func New(ctx context.Context, opt *Options, isCreate bool) (blob.Storage, error) {
	_ = isCreate

	if opt.BucketName == "" {
		return nil, errors.New("bucket name must be specified")
	}

	cli, err := backblaze.NewB2(backblaze.Credentials{KeyID: opt.KeyID, ApplicationKey: opt.Key})
	if err != nil {
		return nil, errors.Wrap(err, "unable to create client")
	}

	bucket, err := cli.Bucket(opt.BucketName)
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open bucket %q", opt.BucketName)
	}

	if bucket == nil {
		return nil, errors.Errorf("bucket not found: %s", opt.BucketName)
	}

	return retrying.NewWrapper(&b2Storage{
		Options: *opt,
		cli:     nil,
		bucket:  nil,
	}), nil
}

func init() {
	blob.AddSupportedStorage(b2storageType, Options{}, New)
}

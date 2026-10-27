package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"image"
	_ "image/jpeg"
	"image/png"
	"io"
	"math"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"golang.org/x/image/draw"
)

const maxInputBytes = 10 << 20

type resizePayload struct {
	SourceKey string `json:"source_key"`
	Width     int    `json:"width"`
	Height    int    `json:"height"`
}

func (p resizePayload) validate() error {
	if p.Width < 1 || p.Width > 1024 || p.Height < 1 || p.Height > 1024 {
		return errors.New("resize_image width and height must be 1..1024")
	}
	if !strings.HasPrefix(p.SourceKey, "inputs/") || len(p.SourceKey) > 512 || len(p.SourceKey) <= len("inputs/") {
		return errors.New("source_key must be a nonempty key under inputs/")
	}
	for _, segment := range strings.Split(p.SourceKey, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return errors.New("source_key contains an invalid path segment")
		}
		for _, r := range segment {
			if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '_' || r == '-') {
				return errors.New("source_key contains unsupported characters")
			}
		}
	}
	return nil
}

type ObjectStore struct {
	Client *minio.Client
	Bucket string
}

func openObjectStore(ctx context.Context, endpoint, accessKey, secretKey, bucket string) (*ObjectStore, error) {
	if endpoint == "" || accessKey == "" || secretKey == "" || bucket == "" {
		return nil, errors.New("object store configuration is incomplete")
	}
	c, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(accessKey, secretKey, ""), Secure: false, Region: "us-east-1"})
	if err != nil {
		return nil, err
	}
	exists, err := c.BucketExists(ctx, bucket)
	if err != nil {
		return nil, err
	}
	if !exists {
		if err = c.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
			// Another worker may have created the bucket concurrently.
			if exists, recheck := c.BucketExists(ctx, bucket); recheck != nil || !exists {
				return nil, err
			}
		}
	}
	return &ObjectStore{Client: c, Bucket: bucket}, nil
}

func (s *ObjectStore) resize(ctx context.Context, j Job) ([]byte, error) {
	var p resizePayload
	if err := json.Unmarshal(j.Payload, &p); err != nil {
		return nil, err
	}
	if err := p.validate(); err != nil {
		return nil, err
	}
	obj, err := s.Client.GetObject(ctx, s.Bucket, p.SourceKey, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer obj.Close()
	info, err := obj.Stat()
	if err != nil {
		return nil, fmt.Errorf("source object: %w", err)
	}
	if info.Size > maxInputBytes {
		return nil, errors.New("source object exceeds 10 MiB")
	}
	data, err := io.ReadAll(io.LimitReader(obj, maxInputBytes+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxInputBytes {
		return nil, errors.New("source object exceeds 10 MiB")
	}
	config, format, err := image.DecodeConfig(bytes.NewReader(data))
	if err != nil || (format != "png" && format != "jpeg") {
		return nil, errors.New("source must be a valid PNG or JPEG")
	}
	if config.Width < 1 || config.Height < 1 || config.Width > 8000 || config.Height > 8000 || int64(config.Width)*int64(config.Height) > 20_000_000 {
		return nil, errors.New("source image dimensions exceed limit")
	}
	src, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return nil, errors.New("source image could not be decoded")
	}
	scale := math.Min(1, math.Min(float64(p.Width)/float64(config.Width), float64(p.Height)/float64(config.Height)))
	w := max(1, int(math.Round(float64(config.Width)*scale)))
	h := max(1, int(math.Round(float64(config.Height)*scale)))
	dst := image.NewRGBA(image.Rect(0, 0, w, h))
	draw.ApproxBiLinear.Scale(dst, dst.Bounds(), src, src.Bounds(), draw.Over, nil)
	var output bytes.Buffer
	if err = png.Encode(&output, dst); err != nil {
		return nil, err
	}
	key := "jobs/" + j.ID + "/thumbnail.png"
	_, err = s.Client.PutObject(ctx, s.Bucket, key, bytes.NewReader(output.Bytes()), int64(output.Len()), minio.PutObjectOptions{ContentType: "image/png"})
	if err != nil {
		return nil, err
	}
	return json.Marshal(struct {
		OutputKey  string `json:"output_key"`
		Width      int    `json:"width"`
		Height     int    `json:"height"`
		SourceETag string `json:"source_etag"`
	}{key, w, h, info.ETag})
}

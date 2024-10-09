package s3setlock

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// Default values
var (
	DefaultLeaseDuration = 60 * time.Second
)

type S3Client interface {
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
}

func newOptions() *Options {
	return &Options{
		Logger:        slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError})),
		LeaseDuration: DefaultLeaseDuration,
		Delay:         true,
		ctx:           context.Background(),
	}
}

type Options struct {
	NoPanic       bool
	Logger        *slog.Logger
	Delay         bool
	Client        S3Client
	Region        string
	LeaseDuration time.Duration
	ctx           context.Context
}

// WithLogger is a setting to enable the log output of S3 Locker. By default, Logger that does not output anywhere is specified.
func WithLogger(logger *slog.Logger) func(opts *Options) {
	return func(opts *Options) {
		opts.Logger = logger
	}
}

// WithClient specifies the AWS SDK Client. If not specified, the default client is created.
func WithClient(client S3Client) func(opts *Options) {
	return func(opts *Options) {
		opts.Client = client
	}
}

// WithRegion specifies the AWS Region. Default AWS_DEFAULT_REGION env
func WithRegion(region string) func(opts *Options) {
	return func(opts *Options) {
		opts.Region = region
	}
}

// WithLeaseDuration affects the heartbeat interval and TTL after Lock acquisition. The default is 10 seconds
func WithLeaseDuration(d time.Duration) func(opts *Options) {
	return func(opts *Options) {
		opts.LeaseDuration = d
	}
}

// WithContext specifies the Context used by Lock() and Unlock().
func WithContext(ctx context.Context) func(opts *Options) {
	return func(opts *Options) {
		opts.ctx = ctx
	}
}

// WithNoPanic changes the behavior so that it does not panic if an error occurs in the Lock () and Unlock () functions.
// Check the LastErr () function to see if an error has occurred when WithNoPanic is specified.
func WithNoPanic() func(opts *Options) {
	return func(opts *Options) {
		opts.NoPanic = true
	}
}

// WithDelay will delay the acquisition of the lock if it fails to acquire the lock. This is similar to the N option of setlock.
// The default is delay enalbed(true). Specify false if you want to exit immediately if Lock acquisition fails.
func WithDelay(delay bool) func(opts *Options) {
	return func(opts *Options) {
		opts.Delay = delay
	}
}

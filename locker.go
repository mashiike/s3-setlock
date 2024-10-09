package s3setlock

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"math/rand"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
)

// Locker is a locker that uses S3 as a lock storage
type Locker struct {
	bucketName string
	objectKey  string

	lastError     error
	mu            sync.Mutex
	noPanic       bool
	delay         bool
	logger        *slog.Logger
	leaseDuration time.Duration
	defaultCtx    context.Context
	locked        bool
	lockedObject  *s3.PutObjectOutput
	client        S3Client

	unlockSignal chan struct{}
	wg           sync.WaitGroup
}

var (
	loadDefaultConfig = sync.OnceValues(func() (aws.Config, error) {
		return config.LoadDefaultConfig(context.Background())
	})
)

// New returns *Locker
func New(urlStr string, optFns ...func(*Options)) (*Locker, error) {
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if u.Scheme != "s3" {
		return nil, errors.New("scheme is required s3")
	}
	if u.Host == "" {
		return nil, errors.New("bucket_name is required: s3://<bucket_name>/<object_key>")
	}
	objectKey := strings.TrimPrefix(u.Path, "/")
	if objectKey == "" {
		return nil, errors.New("object_key is required: s3://<bucket_name>/<object_key>")
	}
	if strings.HasSuffix(objectKey, "/") {
		return nil, errors.New("object_key is not directory: s3://<bucket_name>/<object_key>")
	}
	bucketName := u.Host
	opts := newOptions()
	for _, optFn := range optFns {
		optFn(opts)
	}
	if opts.LeaseDuration > 24*time.Hour {
		return nil, errors.New("lease duration is so long, please set under 24 hour")
	}
	if opts.LeaseDuration < 100*time.Millisecond {
		return nil, errors.New("lease duration is so short, please set over 100 milli second")
	}
	client := opts.Client
	if client == nil {
		awsCfg, err := loadDefaultConfig()
		if err != nil {
			return nil, err
		}
		s3Opts := []func(*s3.Options){}
		if opts.Region != "" {
			s3Opts = append(s3Opts, func(o *s3.Options) {
				o.Region = opts.Region
			})
		}
		client = s3.NewFromConfig(awsCfg, s3Opts...)
	}

	return &Locker{
		bucketName:    bucketName,
		objectKey:     objectKey,
		logger:        opts.Logger,
		noPanic:       opts.NoPanic,
		delay:         opts.Delay,
		client:        client,
		leaseDuration: opts.LeaseDuration,
		defaultCtx:    opts.ctx,
	}, nil
}

var (
	_            sync.Locker = (*Locker)(nil)
	randReader               = rand.New(rand.NewSource(time.Now().UnixNano()))
	randReaderMu sync.Mutex
)

func randBytes(n int) []byte {
	randReaderMu.Lock()
	defer randReaderMu.Unlock()
	b := make([]byte, n)
	randReader.Read(b)
	return b
}

func (l *Locker) LockGranted() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.locked
}

func (l *Locker) Lock() {
	lockGranted, err := l.LockWithError(l.defaultCtx)
	if err != nil {
		l.bailout(err)
	}
	if !lockGranted {
		l.bailout(errors.New("lock was not granted"))
	}
}

func (l *Locker) Unlock() {
	if err := l.UnlockWithError(l.defaultCtx); err != nil {
		l.bailout(err)
	}
}

const (
	heartbeetIntervalRatio = 0.5
)

func (l *Locker) LockWithError(ctx context.Context) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.DebugContext(ctx, "start LockWithError")
	if l.locked {
		return true, errors.New("aleady lock granted")
	}
	lockGranted, err := l.tryLock(ctx)
	if err != nil {
		l.logger.DebugContext(ctx, "failed to tryLock", "reason", err.Error())
		return false, err
	}
	for !lockGranted && l.delay {
		_, expiredAt, err := l.checkLockObject(ctx)
		if err != nil {
			l.logger.DebugContext(ctx, "failed to check locked object", "reason", err.Error())
			return false, nil
		}
		nextTryTime := time.Until(expiredAt) + time.Second
		l.logger.DebugContext(ctx, "delayed to tryLock", "until", expiredAt, "next_try_time", nextTryTime)
		select {
		case <-time.After(nextTryTime):
			lockGranted, err = l.tryLock(ctx)
			if err != nil {
				l.logger.DebugContext(ctx, "failed to tryLock", "reason", err.Error())
				return false, err
			}
		case <-ctx.Done():
			l.logger.DebugContext(ctx, "context done")
			return false, ctx.Err()
		}
	}
	if !lockGranted {
		l.logger.DebugContext(ctx, "lock not granted")
		return false, nil
	}
	l.locked = true
	l.unlockSignal = make(chan struct{})
	l.wg = sync.WaitGroup{}
	l.logger.DebugContext(ctx, "lock granted",
		"bucket", l.bucketName,
		"object_key", l.objectKey,
		"version_id", dereference(l.lockedObject.VersionId),
		"etag", dereference(l.lockedObject.ETag),
	)
	l.wg.Add(1)
	go func() {
		l.logger.DebugContext(ctx, "start heartbeat", "bucket", l.bucketName, "object_key", l.objectKey)
		defer func() {
			l.logger.DebugContext(ctx, "end heartbeat", "bucket", l.bucketName, "object_key", l.objectKey)
			l.wg.Done()
		}()
		nextHeartbeatTime := time.Now().Add(time.Duration(float64(l.leaseDuration) * heartbeetIntervalRatio))
		for {
			sleepTime := time.Until(nextHeartbeatTime)
			l.logger.DebugContext(ctx, "wait for next heartbeat time", "until", nextHeartbeatTime, "sleep_time", sleepTime)
			select {
			case <-ctx.Done():
				return
			case <-l.unlockSignal:
				return
			case <-time.After(sleepTime):
			}
			if err = l.heartbeat(ctx); err != nil {
				l.logger.DebugContext(ctx, "failed to heartbeat", "reason", err.Error())
				return
			}
			nextHeartbeatTime = time.Now().Add(time.Duration(float64(l.leaseDuration) * heartbeetIntervalRatio))
		}
	}()
	return true, nil
}

func dereference[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

func (l *Locker) lockerMetadata() map[string]string {
	return map[string]string{
		"x-amz-meta-locker":    "s3-setlock/v" + Version,
		"x-amz-meta-locked-at": time.Now().Format(time.RFC3339),
	}
}

func (l *Locker) tryLock(ctx context.Context) (bool, error) {
	output, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(l.bucketName),
		Key:         aws.String(l.objectKey),
		IfNoneMatch: aws.String("*"),
		Body:        bytes.NewReader(randBytes(512)),
		Metadata:    l.lockerMetadata(),
	})
	if err != nil {
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			l.logger.DebugContext(ctx, "failed to PutObject", "reason", err.Error())
			return false, err
		}
		l.logger.DebugContext(ctx, "failed to PutObject", "code", apiErr.ErrorCode(), "message", apiErr.ErrorMessage())
		if apiErr.ErrorCode() != "PreconditionFailed" && apiErr.ErrorCode() != "ConditionalRequestConflict" {
			return false, err
		}
		_, expiredAt, checkErr := l.checkLockObject(ctx)
		if checkErr != nil {
			l.logger.DebugContext(ctx, "failed to check locked object", "reason", checkErr.Error())
			return false, nil
		}
		if !time.Now().After(expiredAt) {
			// locked object is not expired
			return false, nil
		}
		// force unlock
		if err := l.deleteLockObject(ctx); err != nil {
			l.logger.DebugContext(ctx, "failed to delete locked object", "reason", err.Error())
		}
		l.logger.WarnContext(ctx, "lock object is expired, force unlocked", "bucket", l.bucketName, "object_key", l.objectKey)
		output, err = l.client.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(l.bucketName),
			Key:         aws.String(l.objectKey),
			IfNoneMatch: aws.String("*"),
			Body:        bytes.NewReader(randBytes(512)),
			Metadata:    l.lockerMetadata(),
		})
		if err != nil {
			// can not create lock object
			return false, nil
		}
	}
	l.lockedObject = output
	return true, nil
}

func (l *Locker) UnlockWithError(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.logger.DebugContext(ctx, "start UnlockWithError", "bucket", l.bucketName, "object_key", l.objectKey)
	if !l.locked {
		return errors.New("not lock granted")
	}
	close(l.unlockSignal)
	l.wg.Wait()
	lockGranted, expiredAt, err := l.checkLockObject(ctx)
	if err != nil {
		l.logger.DebugContext(ctx, "failed to check locked object", "reason", err.Error())
		l.locked = false
		l.lockedObject = nil
		return nil
	}
	if !lockGranted {
		l.logger.DebugContext(ctx, "can not found locked object", "bucket", l.bucketName, "object_key", l.objectKey)
		l.locked = false
		l.lockedObject = nil
		return nil
	}

	l.logger.DebugContext(ctx, "try delete locked object", "bucket", l.bucketName, "object_key", l.objectKey, "version_id", dereference(l.lockedObject.VersionId), "etag", dereference(l.lockedObject.ETag), "expired_at", expiredAt)
	if err := l.deleteLockObject(ctx); err != nil {
		l.logger.WarnContext(ctx, "failed to delete locked object", "reason", err.Error())
		return nil
	}
	l.locked = false
	l.lockedObject = nil
	return nil
}

func (l *Locker) checkLockObject(ctx context.Context) (bool, time.Time, error) {
	var versionID, etag *string
	if l.lockedObject != nil {
		versionID = l.lockedObject.VersionId
		etag = l.lockedObject.ETag
	}
	output, err := l.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:    aws.String(l.bucketName),
		Key:       aws.String(l.objectKey),
		IfMatch:   etag,
		VersionId: versionID,
	})
	if err != nil {
		now := time.Now()
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			l.logger.DebugContext(ctx, "failed to HeadObject", "reason", err.Error())
			return false, now, err
		}
		if apiErr.ErrorCode() != "NotFound" && apiErr.ErrorCode() != "PreconditionFailed" {
			l.logger.DebugContext(ctx, "failed to HeadObject", "code", apiErr.ErrorCode(), "message", apiErr.ErrorMessage())
			return false, now, err
		}
		return false, now, nil
	}
	expiredAt := dereference(output.LastModified).Add(l.leaseDuration)
	return true, expiredAt, nil
}

func (l *Locker) deleteLockObject(ctx context.Context) error {
	var versionID *string
	if l.lockedObject != nil {
		versionID = l.lockedObject.VersionId
	}
	_, err := l.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket:    aws.String(l.bucketName),
		Key:       aws.String(l.objectKey),
		VersionId: versionID,
	})
	if err != nil {
		var apiErr smithy.APIError
		if !errors.As(err, &apiErr) {
			return err
		}
		if apiErr.ErrorCode() != "NoSuchKey" {
			return err
		}
		return nil
	}
	return nil
}

func (l *Locker) heartbeat(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.locked {
		return errors.New("not lock granted")
	}
	locked, _, err := l.checkLockObject(ctx)
	if err != nil {
		return err
	}
	if !locked {
		return errors.New("can not found locked object")
	}
	output, err := l.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(l.bucketName),
		Key:      aws.String(l.objectKey),
		Body:     bytes.NewReader(randBytes(512)),
		Metadata: l.lockerMetadata(),
	})
	if err != nil {
		return err
	}
	beforeEtag := dereference(l.lockedObject.ETag)
	l.lockedObject = output
	l.logger.DebugContext(ctx, "heartbeat", "bucket", l.bucketName, "object_key", l.objectKey, "version_id", dereference(l.lockedObject.VersionId), "etag", dereference(l.lockedObject.ETag), "before_etag", beforeEtag)
	return nil
}

func (l *Locker) LastError() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lastError
}

func (l *Locker) Reset() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastError = nil
}

type bailoutErr struct {
	err error
}

func (l *Locker) bailout(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lastError = err
	if !l.noPanic {
		panic(bailoutErr{err: err})
	}
}

// Recover for Lock() and Unlock() panic
func Recover(e interface{}) error {
	if e != nil {
		b, ok := e.(bailoutErr)
		if !ok {
			panic(e)
		}
		return b.err
	}
	return nil
}

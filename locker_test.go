package s3setlock_test

import (
	"bytes"
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go"
	s3setlock "github.com/mashiike/s3-setlock"
	"github.com/stretchr/testify/require"
)

func runE2ETest(t *testing.T, bucketName string, client s3setlock.S3Client) {
	t.Parallel()
	defer func() {
		if err, ok := s3setlock.AsBailout(recover()); ok {
			require.NoError(t, err)
		}
	}()
	var buf bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	var wgStart, wgEnd sync.WaitGroup
	wgStart.Add(1)
	var total1, total2 int
	var lastTime1, lastTime2 time.Time
	workerNum := 10
	countMax := 10
	f1 := func(workerID int, l sync.Locker) {
		err := s3setlock.HandleBailout(func() error {
			l.Lock()
			defer l.Unlock()
			logger.Info("start f1", "worker_id", workerID)
			for i := 0; i < countMax; i++ {
				total1 += 1
				time.Sleep(10 * time.Millisecond)
			}
			lastTime1 = time.Now()
			logger.Info("finish f1", "worker_id", workerID)
			return nil
		})
		require.NoError(t, err)
	}
	f2 := func(workerID int, l sync.Locker) {
		err := s3setlock.HandleBailout(func() error {
			l.Lock()
			defer l.Unlock()
			logger.Info("start f2", "worker_id", workerID)

			for i := 0; i < countMax; i++ {
				total2 += 1
				time.Sleep(20 * time.Millisecond)
			}
			lastTime2 = time.Now()

			logger.Info("finish f2", "worker_id", workerID)
			return nil
		})
		require.NoError(t, err)
	}
	opts := []func(*s3setlock.Options){
		s3setlock.WithLogger(logger),
	}
	if client != nil {
		opts = append(opts, s3setlock.WithClient(client))
	}
	lockObjectURL1 := lockObjectURL(bucketName, "item1")
	lockObjectURL2 := lockObjectURL(bucketName, "item2")
	t.Logf("lockObjectURL1 = %s", lockObjectURL1)
	t.Logf("lockObjectURL2 = %s", lockObjectURL2)
	for i := 0; i < workerNum; i++ {
		wgEnd.Add(2)
		go func(workerID int) {
			defer wgEnd.Done()
			locker := newLocker(t, lockObjectURL1, opts...)
			wgStart.Wait()
			f1(workerID, locker)
		}(i + 1)
		go func(workerID int) {
			defer wgEnd.Done()
			locker := newLocker(t, lockObjectURL2, opts...)
			wgStart.Wait()
			f2(workerID, locker)
		}(i + 1)
	}
	wgStart.Done()
	wgEnd.Wait()
	t.Log(buf.String())
	require.EqualValues(t, workerNum*countMax, total1)
	require.EqualValues(t, workerNum*countMax, total2)
	t.Logf("f1 last = %s", lastTime1)
	t.Logf("f2 last = %s", lastTime2)
	require.False(t, strings.Contains(buf.String(), `"level:"ERROR"`))
}

var (
	randReader   = rand.New(rand.NewSource(time.Now().UnixNano()))
	randReaderMu sync.Mutex
)

func lockObjectURL(bucketName string, itemName string) string {
	randReaderMu.Lock()
	defer randReaderMu.Unlock()
	b := make([]byte, 32)
	randReader.Read(b)
	return fmt.Sprintf("s3://%s/s3-setlock-test/%x/%s", bucketName, b, itemName)
}

func newLocker(t *testing.T, s3URL string, opts ...func(*s3setlock.Options)) *s3setlock.Locker {
	t.Helper()
	opts = append(opts,
		s3setlock.WithDelay(true),
		s3setlock.WithLeaseDuration(500*time.Millisecond),
	)
	locker, err := s3setlock.New(
		s3URL,
		opts...,
	)
	require.NoError(t, err)
	return locker
}

func TestE2E__WithAWS(t *testing.T) {
	bucketName, ok := os.LookupEnv("TEST_S3_SETLOCK_BUCKET_NAME")
	if !ok || bucketName == "" {
		t.Skip("TEST_S3_SETLOCK_BUCKET_NAME is not set")
	}
	runE2ETest(t, bucketName, nil)
}

type mockClient struct {
	mu           sync.Mutex
	etag         map[string]string
	lastModified map[string]time.Time
}

var _ s3setlock.S3Client = (*mockClient)(nil)

func dereference[T any](p *T) T {
	if p == nil {
		var zero T
		return zero
	}
	return *p
}

func (m *mockClient) HeadObject(_ context.Context, input *s3.HeadObjectInput, _ ...func(*s3.Options)) (*s3.HeadObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.etag == nil {
		m.etag = make(map[string]string)
	}
	if m.lastModified == nil {
		m.lastModified = make(map[string]time.Time)
	}
	s3URL := fmt.Sprintf("s3://%s/%s", dereference(input.Bucket), dereference(input.Key))
	etag, ok := m.etag[s3URL]
	if !ok {
		return nil, &smithy.GenericAPIError{
			Code:    "NoSuchKey",
			Message: "The specified key does not exist.",
		}
	}
	ifMatch := dereference(input.IfMatch)
	if ifMatch == "" || ifMatch == "*" || ifMatch == etag {
		return &s3.HeadObjectOutput{
			ETag:         aws.String(m.etag[s3URL]),
			LastModified: aws.Time(m.lastModified[s3URL]),
		}, nil
	}
	return nil, &smithy.GenericAPIError{
		Code:    "PreconditionFailed",
		Message: "Precondition Failed",
	}
}

func (m *mockClient) PutObject(_ context.Context, input *s3.PutObjectInput, _ ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.etag == nil {
		m.etag = make(map[string]string)
	}
	if m.lastModified == nil {
		m.lastModified = make(map[string]time.Time)
	}
	s3URL := fmt.Sprintf("s3://%s/%s", dereference(input.Bucket), dereference(input.Key))
	bs, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}
	hasher := md5.New()
	hasher.Write(bs)
	ifNoneMatch := dereference(input.IfNoneMatch)
	if _, ok := m.etag[s3URL]; ifNoneMatch == "*" && ok {
		return nil, &smithy.GenericAPIError{
			Code:    "PreconditionFailed",
			Message: "Precondition Failed",
		}
	}
	m.etag[s3URL] = fmt.Sprintf("%x", hasher.Sum(nil))
	m.lastModified[s3URL] = time.Now()
	return &s3.PutObjectOutput{
		ETag: aws.String(m.etag[s3URL]),
	}, nil
}

func (m *mockClient) DeleteObject(_ context.Context, input *s3.DeleteObjectInput, _ ...func(*s3.Options)) (*s3.DeleteObjectOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.etag == nil {
		m.etag = make(map[string]string)
	}
	if m.lastModified == nil {
		m.lastModified = make(map[string]time.Time)
	}
	s3URL := fmt.Sprintf("s3://%s/%s", dereference(input.Bucket), dereference(input.Key))
	if _, ok := m.etag[s3URL]; !ok {
		return nil, &smithy.GenericAPIError{
			Code:    "NoSuchKey",
			Message: "The specified key does not exist.",
		}
	}
	delete(m.etag, s3URL)
	delete(m.lastModified, s3URL)
	return &s3.DeleteObjectOutput{}, nil
}

func TestE2E__WithMock(t *testing.T) {
	runE2ETest(t, "s3-setlock-dummy", &mockClient{})
}

func TestNoBailout(t *testing.T) {
	defer func() {
		if err, ok := s3setlock.AsBailout(recover()); ok {
			require.NoError(t, err, "check no panic")
		}
	}()
	// aleady locked client
	client := &mockClient{
		etag: map[string]string{
			"s3://s3-setlock-dummy/test/item3": "etag",
		},
		lastModified: map[string]time.Time{
			"s3://s3-setlock-dummy/test/item3": time.Now(),
		},
	}
	locker, err := s3setlock.New(
		"s3://s3-setlock-dummy/test/item3",
		s3setlock.WithClient(client),
		s3setlock.WithNoBailout(),
		s3setlock.WithDelay(false),
		s3setlock.WithLeaseDuration(24*time.Hour),
	)
	require.NoError(t, err)
	locker.Lock()
	require.Error(t, locker.LastError())
	locker.Reset()
	locker.Unlock()
	require.Error(t, locker.LastError())
}

func TestDoubleLock(t *testing.T) {
	defer func() {
		if err, ok := s3setlock.AsBailout(recover()); ok {
			require.NoError(t, err, "check no panic")
		}
	}()
	client := &mockClient{}
	locker, err := s3setlock.New(
		"s3://s3-setlock-dummy/test/item4",
		s3setlock.WithClient(client),
		s3setlock.WithDelay(true),
		s3setlock.WithLeaseDuration(200*time.Millisecond),
		s3setlock.WithNoBailout(),
	)
	require.NoError(t, err)
	var wg sync.WaitGroup
	workerCount := 5
	wg.Add(workerCount)
	counter := 0
	blockerCh := make(chan struct{})
	for i := 0; i < workerCount; i++ {
		go func() {
			defer wg.Done()
			<-blockerCh
			locker.Lock()
			defer locker.Unlock()
			counter++
			time.Sleep(500 * time.Millisecond)
		}()
	}
	close(blockerCh)
	wg.Wait()
	require.Equal(t, 5, counter)
}

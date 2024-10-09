package s3setlock_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3setlock "github.com/mashiike/s3-setlock"
)

func Example() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	bucketName, client, err := newS3Client(ctx)
	if err != nil {
		log.Fatal(err)
	}
	locker, err := s3setlock.New(fmt.Sprintf("s3://%s/myobject.lock", bucketName),
		s3setlock.WithContext(ctx),
		s3setlock.WithClient(client),
		s3setlock.WithDelay(true),
		s3setlock.WithLeaseDuration(60*time.Second),
	)
	if err != nil {
		log.Fatal(err)
	}
	var wg sync.WaitGroup
	counter := 0
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				locker.Lock()
				counter++
				locker.Unlock()
			}
		}()
	}
	wg.Wait()
	fmt.Println(counter)
	// Output:
	// 100
}

func newS3Client(ctx context.Context) (string, s3setlock.S3Client, error) {
	bucketName, ok := os.LookupEnv("TEST_S3_SETLOCK_BUCKET_NAME")
	if !ok || bucketName == "" {
		return "s3-setlock-test", &mockClient{}, nil
	}
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return "", nil, err
	}
	return bucketName, s3.NewFromConfig(awsCfg), nil
}

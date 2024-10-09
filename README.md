# s3-setlock

![Latest GitHub release](https://img.shields.io/github/release/mashiike/s3-setlock.svg)
![Github Actions test](https://github.com/mashiike/s3-setlock/workflows/Test/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/mashiike/s3-setlock)](https://goreportcard.com/report/mashiike/s3-setlock) 
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/s3-setlock/blob/master/LICENSE)
[![Documentation](https://godoc.org/github.com/mashiike/s3-setlock?status.svg)](https://godoc.org/github.com/mashiike/s3-setlock)

Like the setlock command using Amazon S3.  

This tool uses conditional requests as described in the [Amazon S3 User Guide](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-requests.html) to implement locking mechanisms. This ensures that operations are performed only if certain conditions are met, providing a robust way to handle concurrent access to S3 objects.

## Usage as command line

```
Usage: s3-setlock [ -nNxX ] [--endpoint <endpoint>] [--timeout <duration>] [--log-level <level> --log-format <json/text>] [--version] s3://<bucket_name>/<object_key> your_command
Flags:
  -n
        No delay. If fn is locked by another process, setlock gives up.
  -N
        (Default.) Delay. If fn is locked by another process, setlock waits until it can obtain a new lock.
  -x
        If fn cannot be update-item (or put-item) or locked, setlock exits zero.
  -X
        (Default.) If fn cannot be update-item (or put-item) or locked, setlock prints an error message and exits nonzero.
  --log-format string
        log format (text, json)
  --log-level string
        minimum log level (debug, info, warn, error)
  --region string
        aws region
  --timeout string
        set command timeout
  --version
        show version
```

for Example 
```
$s3-setlock s3://mybucket/mykey sleep 60
```

## Usage as library

example code is below.
```go
package s3setlock_test

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	s3setlock "github.com/mashiike/s3-setlock"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	locker, err := s3setlock.New(
        "s3://mybucket/myobject.lock",
		s3setlock.WithContext(ctx),
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
```

Note: If Lock or Unlock fails, for example because you can't connect to DynamoDB, it will panic.  
      If you don't want it to panic, use `LockWithError()` and `UnlockWithError()`. Alternatively, use the `WithNoPanic` option.

more information see [go doc](https://godoc.org/github.com/mashiike/s3-setlock).

## Install 

### binary packages

[Releases](https://github.com/mashiike/s3-setlock/releases).

### Homebrew tap

```console
$ brew install mashiike/tap/s3-setlock
```

## License

see [LICENSE](https://github.com/mashiike/s3-setlock/blob/main/LICENSE) file.

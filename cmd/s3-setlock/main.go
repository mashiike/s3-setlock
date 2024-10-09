package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"time"

	s3setlock "github.com/mashiike/s3-setlock"
)

func main() {
	os.Exit(_main())
}

func _main() int {
	var (
		n, N, x, X, versionFlag        bool
		level, region, timeout, format string
	)
	flag.CommandLine.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: s3-setlock [ -nNxX ] [--endpoint <endpoint>] [--timeout <duration>] [--log-level <level> --log-format <json/text>] [--version] s3://<bucket_name>/<object_key> your_command\n")
		printDefaults(flag.CommandLine)
	}
	flag.BoolVar(&n, "n", false, "No delay. If fn is locked by another process, setlock gives up.")
	flag.BoolVar(&N, "N", false, "(Default.) Delay. If fn is locked by another process, setlock waits until it can obtain a new lock.")
	flag.BoolVar(&x, "x", false, "If fn cannot be update-item (or put-item) or locked, setlock exits zero.")
	flag.BoolVar(&X, "X", false, "(Default.) If fn cannot be update-item (or put-item) or locked, setlock prints an error message and exits nonzero.")
	flag.BoolVar(&versionFlag, "version", false, "show version")
	flag.StringVar(&region, "region", "", "aws region")
	flag.StringVar(&timeout, "timeout", "", "set command timeout")
	flag.StringVar(&level, "log-level", "info", "minimum log level (debug, info, warn, error)")
	flag.StringVar(&format, "log-format", "text", "log format (text, json)")

	args := make([]string, 1, len(os.Args))
	args[0] = os.Args[0]
	for _, arg := range os.Args[1:] {
		// long flags
		if strings.HasPrefix(arg, "--") && len(arg) > 2 {
			if strings.Contains(arg, "=") {
				parts := strings.SplitN(arg[2:], "=", 2)
				args = append(args, "--"+parts[0])
				args = append(args, parts[1])
			} else {
				args = append(args, arg)
			}
			continue
		}
		//short flags
		if strings.HasPrefix(arg, "-") && len(arg) > 1 {
			for i := 1; i < len(arg); i++ {
				args = append(args, "-"+string(arg[i]))
			}
			continue
		}
		args = append(args, arg)
	}
	if err := flag.CommandLine.Parse(args[1:]); err != nil {
		fmt.Fprintf(flag.CommandLine.Output(), "s3-setlock: %v\n", err)
		return 1
	}
	if versionFlag {
		fmt.Fprintf(flag.CommandLine.Output(), "s3-setlock version: v%s\n", s3setlock.Version)
		fmt.Fprintf(flag.CommandLine.Output(), "go runtime version: %s\n", runtime.Version())
		return 0
	}
	offset := 0
	if flag.NArg() < 1 {
		flag.CommandLine.Usage()
		fmt.Fprintf(flag.CommandLine.Output(), "\ns3-setlock: missing s3 url\n")
		return 1
	}
	if flag.Arg(1) == "--" {
		offset = 1
	}
	if flag.NArg()-offset < 2 {
		flag.CommandLine.Usage()
		fmt.Fprintf(flag.CommandLine.Output(), "\ns3-setlock: missing your command\n")
		return 1
	}
	args = flag.Args()
	if offset > 0 {
		args = append(args[0:offset], args[offset+1:]...)
	}
	var leveler slog.Level
	if err := leveler.UnmarshalText([]byte(level)); err != nil {
		fmt.Fprintf(flag.CommandLine.Output(), "s3-setlock: invalid log level: %s\n", level)
		return 4
	}
	var logger *slog.Logger
	switch format {
	case "text":
		logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			Level: leveler,
		})).With("application", "s3-setlock")
	case "json":
		logger = slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
			Level: leveler,
		})).With("application", "s3-setlock")
	default:
		fmt.Fprintf(flag.CommandLine.Output(), "s3-setlock: invalid log format: %s\n", format)
		return 4
	}
	// -N and -n both specified, Delay is true by default
	// -N and -n both not specified, Delay is true by default
	// -N specified, -n not specified, Delay is true
	// -N not specified, -n specified, Delay is false
	delay := N || (!N && !n)
	optFns := []func(*s3setlock.Options){
		s3setlock.WithDelay(delay),
		s3setlock.WithLogger(logger),
		s3setlock.WithRegion(region),
	}
	locker, err := s3setlock.New(args[0], optFns...)
	if err != nil {
		logger.Error(err.Error())
		return 2
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	if timeout != "" {
		t, err := time.ParseDuration(timeout)
		if err != nil {
			logger.Error("failed timeout parse", "reason", err.Error())
			return 7
		}
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, t)
		defer cancel()
	}
	lockGranted, err := locker.LockWithError(ctx)
	if err != nil {
		logger.Error("unable to lock", "reason", err.Error())
		return 6
	}
	if !lockGranted {
		logger.Warn("lock was not granted")
		if x && !X {
			return 0
		}
		return 3
	}
	defer func() {
		if err := locker.UnlockWithError(context.Background()); err != nil {
			logger.Error("unable to unlock", "reason", err.Error())
		}
	}()

	cmd := exec.CommandContext(ctx, args[1], args[2:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err = cmd.Run()
	if err != nil {
		logger.Error("unable to run command", "reason", err.Error())
		return 5
	}
	return 0
}

func printDefaults(flagSet *flag.FlagSet) {
	shortFlags := make([]*flag.Flag, 0, flagSet.NFlag())
	longFlags := make([]*flag.Flag, 0, flagSet.NFlag())

	flagSet.VisitAll(func(f *flag.Flag) {
		if len(f.Name) > 1 {
			longFlags = append(longFlags, f)
		} else {
			shortFlags = append(shortFlags, f)
		}
	})
	fmt.Fprintln(flagSet.Output(), "Flags:")
	sort.Slice(shortFlags, func(i, j int) bool {
		if strings.EqualFold(shortFlags[i].Name, shortFlags[j].Name) {
			return shortFlags[i].Name > shortFlags[j].Name
		}
		return strings.ToLower(shortFlags[i].Name) < strings.ToLower(shortFlags[j].Name)
	})
	sort.Slice(longFlags, func(i, j int) bool {
		return strings.ToLower(longFlags[i].Name) < strings.ToLower(longFlags[j].Name)
	})
	flags := append(shortFlags, longFlags...)
	for _, f := range flags {
		var builder strings.Builder
		if len(f.Name) > 1 {
			//long flag
			fmt.Fprintf(&builder, "  --%s", f.Name)
		} else {
			//short flag
			fmt.Fprintf(&builder, "  -%s", f.Name)
		}
		name, usage := flag.UnquoteUsage(f)
		if len(name) > 0 {
			builder.WriteString(" ")
			builder.WriteString(name)
		}
		builder.WriteString("\t")
		if builder.Len() <= 4 { // space, space, '-', 'x'.
			builder.WriteString("\t")
		} else {
			builder.WriteString("\n    \t")
		}
		builder.WriteString(strings.ReplaceAll(usage, "\n", "\n    \t"))
		fmt.Fprint(flagSet.Output(), builder.String(), "\n")
	}
}

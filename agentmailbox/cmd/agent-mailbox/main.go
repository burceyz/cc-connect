// Command agent-mailbox runs the local agent-mailbox service.
//
// Usage:
//
//	agent-mailbox serve --runtime <private directory>
//	agent-mailbox version
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"agentmailbox/internal/mailbox"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "serve":
		if err := serve(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "agent-mailbox:", err)
			os.Exit(1)
		}
	case "version":
		fmt.Println(mailbox.ProtocolName, mailbox.ProtocolVersion)
	default:
		usage()
		os.Exit(2)
	}
}

func serve(args []string) error {
	flags := flag.NewFlagSet("serve", flag.ContinueOnError)
	runtimeDir := flags.String("runtime", "", "private runtime directory (socket, database, admin token)")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *runtimeDir == "" {
		return fmt.Errorf("--runtime is required")
	}
	// All service files must be private even before explicit chmod calls.
	syscall.Umask(0o077)
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return mailbox.Run(ctx, *runtimeDir, logger)
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: agent-mailbox serve --runtime <private directory>")
}

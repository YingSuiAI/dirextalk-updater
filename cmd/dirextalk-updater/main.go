package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/YingSuiAI/dirextalk-updater/internal/buildinfo"
	"github.com/YingSuiAI/dirextalk-updater/internal/updater"
)

func main() {
	configPath := flag.String("config", "/etc/dirextalk-updater/config.json", "root-owned updater configuration")
	flag.Parse()
	command := "serve"
	if flag.NArg() > 0 {
		command = flag.Arg(0)
	}
	var err error
	switch command {
	case "serve":
		if err = updater.CheckSupportedHost(); err != nil {
			break
		}
		err = runServer(*configPath)
	case "version":
		err = writeVersion()
	case "pin-initial-latest":
		if err = updater.CheckSupportedHost(); err == nil {
			err = pinInitialLatest(*configPath)
		}
	default:
		err = fmt.Errorf("unknown command %q", command)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "dirextalk-updater:", err)
		os.Exit(1)
	}
}

type initialLatestPinner interface {
	PinInitialLatest(context.Context) error
}

type initialLatestRuntimeFactory func(updater.CaddyMode, updater.ComposeProject) (initialLatestPinner, error)

func pinInitialLatest(configPath string) error {
	config, err := updater.LoadConfigFile(configPath)
	if err != nil {
		return err
	}
	return pinInitialLatestWithConfig(context.Background(), config, func(mode updater.CaddyMode, project updater.ComposeProject) (initialLatestPinner, error) {
		return updater.NewComposeRuntime(mode, project)
	})
}

func pinInitialLatestWithConfig(ctx context.Context, config updater.Config, factory initialLatestRuntimeFactory) error {
	runtime, err := factory(config.CaddyMode, config.ComposeProject)
	if err != nil {
		return err
	}
	return runtime.PinInitialLatest(ctx)
}

func writeVersion() error {
	encoder := json.NewEncoder(os.Stdout)
	encoder.SetIndent("", "  ")
	return encoder.Encode(buildinfo.Current())
}

func runServer(configPath string) error {
	config, controlToken, err := loadRuntimeConfig(configPath)
	if err != nil {
		return err
	}
	store := updater.NewStateStore(filepath.Join(config.StateDir, "runtime.json"))
	runtime, err := updater.NewComposeRuntime(config.CaddyMode, config.ComposeProject)
	if err != nil {
		return err
	}
	if err := runtime.Recover(context.Background()); err != nil {
		return fmt.Errorf("recover updater backup state: %w", err)
	}
	engine := updater.NewJobEngine(store, runtime)
	hostGate := updater.NewHostOperationGate()
	watchdog := updater.NewWatchdog(store, runtime, hostGate)
	watchdog.SetLogger(log.Printf)
	service, err := updater.NewService(
		store,
		controlToken,
		updater.WithDirectJobRuntime(runtime),
		updater.WithJobEngine(engine),
		updater.WithHostOperationGate(hostGate),
	)
	if err != nil {
		return err
	}
	listener, err := updater.ListenUnix(config.SocketPath)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: service.Handler(), ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	go service.RunJobs(ctx)
	go watchdog.Run(ctx)
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdownCtx)
	}()
	if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve updater API: %w", err)
	}
	return nil
}

func loadRuntimeConfig(configPath string) (updater.Config, string, error) {
	config, err := updater.LoadConfigFile(configPath)
	if err != nil {
		return updater.Config{}, "", err
	}
	controlToken, err := updater.LoadControlToken(config.ControlTokenFile)
	if err != nil {
		return updater.Config{}, "", err
	}
	return config, controlToken, nil
}

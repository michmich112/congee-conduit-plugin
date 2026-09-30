package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/michmich112/conduit-plugin/embed"
	"github.com/michmich112/conduit-plugin/handler"
	sdk "github.com/michmich112/congee/sdk/plugin"
)

func main() {
	dataDir := os.Getenv(sdk.EnvPluginDataDir)
	if dataDir == "" {
		dataDir = "."
	}
	for _, a := range os.Args[1:] {
		switch {
		case a == "--hook=uninstall":
			if err := handler.RunUninstallHook(dataDir); err != nil {
				fmt.Fprintf(os.Stderr, "uninstall: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		case a == "--hook=update":
			if err := handler.RunUpdateHook(context.Background(), dataDir, os.Getenv(sdk.EnvPluginSettings)); err != nil {
				fmt.Fprintf(os.Stderr, "update: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		case a == "--hook=install" || a == "--hook=launch":
			if err := handler.RunLifecycleHook(context.Background(), dataDir, os.Getenv(sdk.EnvPluginSettings)); err != nil {
				fmt.Fprintf(os.Stderr, "asset setup: %v\n", err)
				os.Exit(1)
			}
			os.Exit(0)
		case strings.HasPrefix(a, "--hook="):
			fmt.Fprintf(os.Stderr, "unknown hook %s\n", a)
			os.Exit(1)
		}
	}
	embed.PrepareRuntimeLibrary(dataDir)
	sel := embed.Select(embed.DefaultModelPath(dataDir))
	switch sel.Source {
	case embed.SourceExplicitFake:
		fmt.Fprintf(os.Stderr, "embedder %s: CONDUIT_EMBEDDER=fake\n", sel.ModelID)
	case embed.SourceONNX:
		fmt.Fprintf(os.Stderr, "embedder onnx model_id=%s\n", sel.ModelID)
	case embed.SourceHTTP:
		fmt.Fprintf(os.Stderr, "embedder http model_id=%s dim=%d\n", sel.ModelID, sel.Dim)
	default:
		fmt.Fprintf(os.Stderr, "embedder unavailable: %s\n", sel.Error)
		fmt.Fprintf(os.Stderr, "vector rank disabled until a real model loads, an external provider is verified, or CONDUIT_EMBEDDER=fake\n")
	}
	h := handler.New(dataDir, sel)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
	defer stop()
	err := sdk.Serve(ctx, h)
	_ = h.Close()
	if err != nil && ctx.Err() == nil {
		os.Exit(1)
	}
}

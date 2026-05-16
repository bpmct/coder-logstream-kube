//go:build ignore

package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"time"

	"cdr.dev/slog/v3"
	"cdr.dev/slog/v3/sloggers/sloghuman"
	"github.com/coder/coder/v2/codersdk"
	"github.com/coder/coder/v2/codersdk/agentsdk"
	"github.com/google/uuid"
)

var logSourceID = uuid.MustParse("cabdacf8-7c90-425c-9815-cae3c75d1169")

func main() {
	agentURL := os.Getenv("CODER_AGENT_URL")
	agentToken := os.Getenv("CODER_AGENT_TOKEN")
	if agentURL == "" || agentToken == "" {
		fmt.Fprintln(os.Stderr, "CODER_AGENT_URL and CODER_AGENT_TOKEN must be set")
		os.Exit(1)
	}

	u, err := url.Parse(agentURL)
	if err != nil {
		panic(err)
	}

	logger := slog.Make(sloghuman.Sink(os.Stderr)).Leveled(slog.LevelDebug)
	client := agentsdk.New(u, agentsdk.WithFixedToken(agentToken))

	ctx := context.Background()

	_, err = client.PostLogSource(ctx, agentsdk.PostLogSourceRequest{
		ID:          logSourceID,
		Icon:        "/icon/k8s.png",
		DisplayName: "Kubernetes",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "post log source: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("log source registered")

	ls := agentsdk.NewLogSender(logger)
	sl := ls.GetScriptLogger(logSourceID)

	arpc, err := client.ConnectRPC20(ctx)
	if err != nil {
		panic(err)
	}
	sendCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go ls.SendLoop(sendCtx, arpc)

	send := func(level codersdk.LogLevel, msg string, delay time.Duration) {
		time.Sleep(delay)
		if err := sl.Send(ctx, agentsdk.Log{
			CreatedAt: time.Now(),
			Output:    msg,
			Level:     level,
		}); err != nil {
			fmt.Fprintf(os.Stderr, "send: %v\n", err)
		}
		fmt.Printf("[%v] %s\n", level, msg)
	}

	// Karpenter scale-up + image pull scenario
	send(codersdk.LogLevelInfo, "🐳 Created pod: workspace-bpmct-smart-events-kube-main", 0)
	send(codersdk.LogLevelInfo, "Waiting for an available node (cluster may be scaling up)…", 800*time.Millisecond)
	send(codersdk.LogLevelInfo, "Cluster is scaling up to accommodate your workspace — hang tight…", 2*time.Second)
	send(codersdk.LogLevelWarn,  "Still waiting for a node — the cluster is scaling up in the background. No action needed.", 3*time.Second)
	send(codersdk.LogLevelInfo, "Workspace assigned to node ip-10-0-1-5.us-east-2.compute.internal", 4*time.Second)
	send(codersdk.LogLevelInfo, "Downloading workspace image: codercom/enterprise-base:ubuntu", 500*time.Millisecond)
	send(codersdk.LogLevelInfo, "Your workspace is on its way…", 2*time.Second)
	send(codersdk.LogLevelInfo, "Image ready (took 34s): codercom/enterprise-base:ubuntu", 2*time.Second)
	send(codersdk.LogLevelInfo, "Workspace container created, starting up…", 300*time.Millisecond)
	send(codersdk.LogLevelInfo, "Workspace is running, waiting for agent to connect…", 500*time.Millisecond)

	if err := sl.Flush(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "flush: %v\n", err)
	}
	if err := ls.WaitUntilEmpty(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "wait: %v\n", err)
	}
	fmt.Println("done")
}

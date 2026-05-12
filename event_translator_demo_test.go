package main

import (
	"fmt"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestSmartEventsDemo renders a side-by-side comparison of raw k8s events
// vs. the smart-translated output. Run with:
//
//	go test -v -run TestSmartEventsDemo
func TestSmartEventsDemo(t *testing.T) {
	t.Log("") // blank line for readability

	type demoEvent struct {
		reason     string
		message    string
		timeOffset time.Duration
	}

	type scenario struct {
		name   string
		events []demoEvent
	}

	scenarios := []scenario{
		{
			name: "Happy path: Karpenter scale-up + fresh image pull",
			events: []demoEvent{
				{"FailedScheduling", "0/3 nodes are available: 3 node(s) had taint {node.kubernetes.io/not-ready: }, that the pod didn't tolerate.", 0},
				{"FailedScheduling", "0/3 nodes are available: 3 node(s) had taint {node.kubernetes.io/not-ready: }, that the pod didn't tolerate.", 5 * time.Second},
				{"FailedScheduling", "0/3 nodes are available: 3 Insufficient cpu.", 10 * time.Second},
				{"TriggeredScaleUp", "pod triggered scale-up: [{MachineDeployment/default/workers 2->3 (max: 10)}]", 12 * time.Second},
				{"Scheduled", "Successfully assigned default/ws-abc123 to ip-10-0-1-5.us-east-2.compute.internal", 45 * time.Second},
				{"Pulling", `Pulling image "docker.io/codercom/enterprise-base:ubuntu"`, 46 * time.Second},
				{"Pulled", `Successfully pulled image "docker.io/codercom/enterprise-base:ubuntu" in 34.2s`, 80 * time.Second},
				{"Created", `Created container workspace`, 81 * time.Second},
				{"Started", `Started container workspace`, 82 * time.Second},
			},
		},
		{
			name: "PVC cross-AZ re-attach (slow, past threshold)",
			events: []demoEvent{
				{"Scheduled", "Successfully assigned default/ws-def456 to ip-10-0-2-8.us-east-2.compute.internal", 0},
				{"FailedAttachVolume", `AttachVolume.Attach failed for volume "pvc-abc": context deadline exceeded`, 5 * time.Second},
				{"FailedAttachVolume", `AttachVolume.Attach failed for volume "pvc-abc": context deadline exceeded`, 45 * time.Second},
				{"FailedAttachVolume", `AttachVolume.Attach failed for volume "pvc-abc": context deadline exceeded`, 100 * time.Second},
				{"SuccessfulAttachVolume", `AttachVolume.Attach succeeded for volume "pvc-abc"`, 105 * time.Second},
				{"Pulling", `Pulling image "ghcr.io/coder/coder:latest"`, 106 * time.Second},
				{"Pulled", `Successfully pulled image "ghcr.io/coder/coder:latest" in 8.1s`, 115 * time.Second},
				{"Created", `Created container workspace`, 116 * time.Second},
				{"Started", `Started container workspace`, 117 * time.Second},
			},
		},
		{
			name: "ImagePullBackOff: bad image name (escalates after 3 attempts)",
			events: []demoEvent{
				{"Scheduled", "Successfully assigned default/ws-ghi789 to k3d-coder-demo-agent-0", 0},
				{"Pulling", `Pulling image "myregistry.internal/noexist:latest"`, 1 * time.Second},
				{"BackOff", `Back-off pulling image "myregistry.internal/noexist:latest" for container "workspace"`, 30 * time.Second},
				{"BackOff", `Back-off pulling image "myregistry.internal/noexist:latest" for container "workspace"`, 60 * time.Second},
				{"BackOff", `Back-off pulling image "myregistry.internal/noexist:latest" for container "workspace"`, 120 * time.Second},
				{"BackOff", `Back-off pulling image "myregistry.internal/noexist:latest" for container "workspace"`, 180 * time.Second},
			},
		},
		{
			name: "OOMKilled",
			events: []demoEvent{
				{"Scheduled", "Successfully assigned default/ws-jkl012 to k3d-coder-demo-agent-0", 0},
				{"Pulled", `Container image "docker.io/codercom/enterprise-base:ubuntu" already present on machine`, 1 * time.Second},
				{"Created", `Created container workspace`, 2 * time.Second},
				{"Started", `Started container workspace`, 3 * time.Second},
				{"OOMKilling", `Memory cgroup out of memory: Kill process 1234 (node) score 1000 or sacrifice child`, 120 * time.Second},
			},
		},
		{
			name: "Readiness probe failures during normal startup (all suppressed)",
			events: []demoEvent{
				{"Scheduled", "Successfully assigned default/ws-mno345 to k3d-coder-demo-agent-0", 0},
				{"Pulled", `Container image "docker.io/codercom/enterprise-base:ubuntu" already present on machine`, 1 * time.Second},
				{"Created", `Created container workspace`, 2 * time.Second},
				{"Started", `Started container workspace`, 3 * time.Second},
				{"Unhealthy", `Readiness probe failed: dial tcp 10.0.0.1:8080: connect: connection refused`, 5 * time.Second},
				{"Unhealthy", `Readiness probe failed: dial tcp 10.0.0.1:8080: connect: connection refused`, 7 * time.Second},
				{"Unhealthy", `Readiness probe failed: dial tcp 10.0.0.1:8080: connect: connection refused`, 9 * time.Second},
			},
		},
	}

	for _, sc := range scenarios {
		state := newPodEventState()
		baseTime := time.Now()

		t.Logf("\n%s\n%s", sc.name, strings.Repeat("─", len(sc.name)))

		header := fmt.Sprintf("%-50s │ %-65s", "RAW (today, verbatim k8s)", "SMART (proposed)")
		t.Log(header)
		t.Log(strings.Repeat("─", 118))

		for _, de := range sc.events {
			event := &corev1.Event{
				ObjectMeta: metav1.ObjectMeta{Name: "evt"},
				Reason:     de.reason,
				Message:    de.message,
			}
			now := baseTime.Add(de.timeOffset)

			raw := trunc(de.message, 48)
			interp := state.InterpretEvent(event, now)

			var smart string
			if interp == nil {
				smart = "─ (suppressed — expected noise)"
			} else {
				icon := "ℹ"
				switch string(interp.Level) {
				case "warn":
					icon = "⚠"
				case "error":
					icon = "✗"
				}
				smart = fmt.Sprintf("%s %s", icon, trunc(interp.UserMessage, 62))
			}

			t.Logf("%-50s │ %s", raw, smart)
		}
	}
}

func trunc(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

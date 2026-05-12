package main

import (
	"fmt"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/coder/coder/v2/codersdk"
)

func makeEvent(reason, message string) *corev1.Event {
	return &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-event",
		},
		Reason:  reason,
		Message: message,
	}
}

func TestEventTranslator_NoiseSuppression(t *testing.T) {
	t.Parallel()

	// These are all raw k8s events that today get piped raw to the user.
	// Every one of these should either be silenced or translated.
	noiseCases := []struct {
		reason  string
		message string
		desc    string
	}{
		// Note: first FailedScheduling fires a single friendly info message, then
		// subsequent ones are suppressed. Test the suppression of the second event:
		// (tested separately in TestEventTranslator_FailedSchedulingOnlyOnce)

		{
			"Unhealthy",
			"Readiness probe failed: dial tcp 10.0.0.1:8080: connect: connection refused",
			"readiness probe fail during normal startup",
		},
		{
			"BackOff",
			"Back-off pulling image \"docker.io/myorg/myimage:latest\" for container \"workspace\"",
			"first image pull backoff (transient)",
		},
		{
			"NodeNotReady",
			"Node k3d-coder-demo-agent-0 is not ready",
			"transient node flap",
		},
	}

	for _, tc := range noiseCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			state := newPodEventState()
			event := makeEvent(tc.reason, tc.message)
			result := state.InterpretEvent(event, time.Now())

			// First backoff should show a friendly warn, not the raw message
			if tc.reason == "BackOff" && result != nil {
				if result.UserMessage == event.Message {
					t.Errorf("raw k8s message leaked to user: %q", result.UserMessage)
				}
				if result.Level == codersdk.LogLevelError {
					t.Error("first backoff should not be an error")
				}
				return
			}

			// All others should be nil (suppressed) on first occurrence
			if result != nil {
				t.Errorf("expected noise to be suppressed, got: level=%v msg=%q", result.Level, result.UserMessage)
			}
		})
	}
}

func TestEventTranslator_RealErrors(t *testing.T) {
	t.Parallel()

	errorCases := []struct {
		reason  string
		message string
		desc    string
	}{
		{
			"OOMKilling",
			"Memory cgroup out of memory: Kill process 1234 (myapp) score 1000 or sacrifice child",
			"OOM kill",
		},
		{
			"ErrImageNeverPull",
			`Container "workspace" is invalid for image "myimage:latest": imagePullPolicy: Never, but the image is not present`,
			"image never pull",
		},
		{
			"NotTriggerScaleUp",
			"pod didn't trigger scale-up (it wouldn't fit if a new node is added)",
			"autoscaler cannot scale up",
		},
	}

	for _, tc := range errorCases {
		tc := tc
		t.Run(tc.desc, func(t *testing.T) {
			t.Parallel()
			state := newPodEventState()
			event := makeEvent(tc.reason, tc.message)
			result := state.InterpretEvent(event, time.Now())

			if result == nil {
				t.Fatal("expected an error result, got nil (event suppressed)")
			}
			if result.Level != codersdk.LogLevelError {
				t.Errorf("expected LogLevelError, got %v", result.Level)
			}
			if result.UserMessage == event.Message {
				t.Errorf("raw k8s message leaked to user: %q", result.UserMessage)
			}
			t.Logf("human message: %s", result.UserMessage)
		})
	}
}

func TestEventTranslator_PhaseProgression(t *testing.T) {
	t.Parallel()

	state := newPodEventState()
	now := time.Now()

	seq := []struct {
		reason  string
		msg     string
		wantNil bool
		wantPhase Phase
	}{
		{
			"FailedScheduling",
			"0/3 nodes are available: 3 Insufficient cpu.",
			false,
			PhaseScheduling,
		},
		{
			"TriggeredScaleUp",
			"pod triggered scale-up: [{MachineDeployment/default/worker 2->3 (max: 10)}]",
			false,
			PhaseScheduling,
		},
		{
			"Scheduled",
			"Successfully assigned default/ws-pod to k3d-coder-demo-agent-0",
			false,
			PhaseScheduling,
		},
		{
			"Pulling",
			`Pulling image "docker.io/codercom/enterprise-base:ubuntu"`,
			false,
			PhasePulling,
		},
		{
			"Pulled",
			`Successfully pulled image "docker.io/codercom/enterprise-base:ubuntu" in 12.3s`,
			false,
			PhaseStarting,
		},
		{
			"Created",
			`Created container workspace`,
			false,
			PhaseStarting,
		},
		{
			"Started",
			`Started container workspace`,
			false,
			PhaseRunning,
		},
	}

	for _, step := range seq {
		step := step
		event := makeEvent(step.reason, step.msg)
		result := state.InterpretEvent(event, now)

		if step.wantNil && result != nil {
			t.Errorf("step %q: expected nil, got %q", step.reason, result.UserMessage)
		}
		if !step.wantNil && result == nil {
			t.Errorf("step %q: expected result, got nil (suppressed)", step.reason)
		}
		if result != nil {
			// Never pipe raw k8s messages
			if result.UserMessage == event.Message {
				t.Errorf("step %q: raw k8s message leaked: %q", step.reason, result.UserMessage)
			}
			t.Logf("[%v] %s", result.Level, result.UserMessage)
		}
		if state.currentPhase != step.wantPhase {
			t.Errorf("step %q: phase = %v, want %v", step.reason, state.currentPhase, step.wantPhase)
		}
	}
}

func TestEventTranslator_FailedSchedulingOnlyOnce(t *testing.T) {
	t.Parallel()

	state := newPodEventState()
	event := makeEvent("FailedScheduling", "0/3 nodes are available: 3 Insufficient cpu.")

	// First event: should produce ONE friendly info message
	first := state.InterpretEvent(event, time.Now())
	if first == nil {
		t.Fatal("expected a friendly message on first FailedScheduling, got nil")
	}
	if first.Level != codersdk.LogLevelInfo {
		t.Errorf("expected info level on first FailedScheduling, got %v", first.Level)
	}
	if first.UserMessage == event.Message {
		t.Errorf("raw k8s message leaked: %q", first.UserMessage)
	}
	t.Logf("first: %s", first.UserMessage)

	// Subsequent events within threshold: silenced
	for i := 0; i < 5; i++ {
		subsequent := state.InterpretEvent(event, time.Now())
		if subsequent != nil {
			t.Errorf("subsequent FailedScheduling event not suppressed (iteration %d): %q", i, subsequent.UserMessage)
		}
	}
}

func TestEventTranslator_SlowSchedulingEscalates(t *testing.T) {
	t.Parallel()

	state := newPodEventState()
	// Start in scheduling state (as if pod was created 2 min ago)
	state.currentPhase = PhaseScheduling
	state.phaseEnteredAt = time.Now().Add(-2 * time.Minute)

	event := makeEvent("FailedScheduling", "0/3 nodes are available: 3 Insufficient cpu.")
	result := state.InterpretEvent(event, time.Now())

	if result == nil {
		t.Fatal("expected a warning after threshold, got nil")
	}
	if result.Level != codersdk.LogLevelWarn {
		t.Errorf("expected warn after threshold, got %v: %s", result.Level, result.UserMessage)
	}
	t.Logf("escalated message: %s", result.UserMessage)
}

func TestEventTranslator_ImagePullBackoffEscalates(t *testing.T) {
	t.Parallel()

	state := newPodEventState()
	event := makeEvent("BackOff", `Back-off pulling image "docker.io/badorg/noexist:latest" for container "workspace"`)

	// Simulate 5 backoff events in a row
	var last *EventInterpretation
	for i := 0; i < 5; i++ {
		last = state.InterpretEvent(event, time.Now())
	}

	if last == nil {
		t.Fatal("expected error result after 5 backoffs, got nil")
	}
	if last.Level != codersdk.LogLevelError {
		t.Errorf("expected error after repeated backoffs, got %v: %s", last.Level, last.UserMessage)
	}
	t.Logf("escalated message: %s", last.UserMessage)
}

func TestEventTranslator_HeartbeatMessage(t *testing.T) {
	t.Parallel()

	state := newPodEventState()
	state.currentPhase = PhaseScheduling

	// No heartbeat when < 60s
	state.phaseEnteredAt = time.Now().Add(-30 * time.Second)
	if msg := state.HeartbeatMessage(time.Now()); msg != "" {
		t.Errorf("expected no heartbeat at 30s, got %q", msg)
	}

	// Heartbeat when between 60s and 10min
	state.phaseEnteredAt = time.Now().Add(-90 * time.Second)
	if msg := state.HeartbeatMessage(time.Now()); msg == "" {
		t.Error("expected heartbeat at 90s, got empty")
	} else {
		t.Logf("heartbeat: %s", msg)
	}

	// No heartbeat in pulling phase (different threshold)
	state.currentPhase = PhaseRunning
	if msg := state.HeartbeatMessage(time.Now()); msg != "" {
		t.Errorf("expected no heartbeat in Running phase, got %q", msg)
	}
}

func TestEventTranslator_ImageShorteningAndExtraction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		msg     string
		want    string
	}{
		{`Pulling image "docker.io/codercom/enterprise-base:ubuntu"`, "codercom/enterprise-base:ubuntu"},
		{`Pulling image "ghcr.io/coder/coder:latest"`, "coder/coder:latest"},
		{`Pulling image "quay.io/prometheus/prometheus:v2.48.0"`, "prometheus/prometheus:v2.48.0"},
		{`Pulling image "myregistry.company.com/myimage:v1.2.3"`, "myregistry.company.com/myimage:v1.2.3"},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.msg, func(t *testing.T) {
			t.Parallel()
			state := newPodEventState()
			event := makeEvent("Pulling", tc.msg)
			result := state.InterpretEvent(event, time.Now())
			if result == nil {
				t.Fatal("expected result, got nil")
			}
			want := fmt.Sprintf("Downloading workspace image: %s", tc.want)
			if result.UserMessage != want {
				t.Errorf("got %q, want %q", result.UserMessage, want)
			}
		})
	}
}

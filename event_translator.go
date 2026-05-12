// Package main - event_translator.go
// Translates raw Kubernetes events into human-readable, phase-aware messages.
// This is the core of the "smart events" concept from the provisioning UX discussion.
//
// Goals:
//   - Never surface a raw k8s event reason/message directly to end users
//   - Distinguish "expected transient noise" (autoscaler seeking a node, image pull in progress)
//     from "real failures" (ImagePullBackOff after multiple attempts, OOMKilled, etc.)
//   - Emit a single concise status line that updates in place (via phase tracking)
//   - Emit a "still waiting…" heartbeat so users know progress is happening
package main

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/coder/coder/v2/codersdk"
)

// Phase represents the high-level lifecycle stage of a workspace pod.
type Phase int

const (
	PhaseUnknown Phase = iota
	PhasePending        // pod created, not yet scheduled
	PhaseScheduling     // scheduler is working (may be waiting for a node)
	PhasePulling        // at least one container is pulling an image
	PhaseStarting       // containers starting/running their init
	PhaseRunning        // agent should be connecting soon
	PhaseFailed         // something is genuinely wrong
)

func (p Phase) String() string {
	switch p {
	case PhasePending:
		return "Pending"
	case PhaseScheduling:
		return "Scheduling"
	case PhasePulling:
		return "Pulling image"
	case PhaseStarting:
		return "Starting"
	case PhaseRunning:
		return "Running"
	case PhaseFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// EventKind categorizes a raw k8s event.
type EventKind int

const (
	KindNoise     EventKind = iota // expected transient noise, don't show
	KindProgress                   // normal in-progress state, show as info
	KindSlowWarn                   // expected but slow, show as warn after threshold
	KindRealError                  // genuine failure, show as error immediately
)

// EventInterpretation is the result of classifying a raw k8s event.
type EventInterpretation struct {
	Kind        EventKind
	Phase       Phase
	UserMessage string // human-readable, non-k8s message
	Level       codersdk.LogLevel
}

// TransientThreshold is how long we allow a "slow" condition before
// upgrading it from noise to a visible warning.
const TransientThreshold = 90 * time.Second

// podEventState tracks running state per pod so we can debounce and
// suppress duplicate/redundant messages.
type podEventState struct {
	currentPhase     Phase
	phaseEnteredAt   time.Time
	lastSlowWarnSent map[string]time.Time // reason → last time we sent a slow-warn for it
	shownReasons     map[string]struct{}  // reasons we've already surfaced at least once
	imagePullStart   map[string]time.Time // container → time pull started
	backoffCount     map[string]int       // container/reason → consecutive count
}

func newPodEventState() *podEventState {
	return &podEventState{
		currentPhase:     PhaseUnknown,
		phaseEnteredAt:   time.Now(),
		lastSlowWarnSent: map[string]time.Time{},
		shownReasons:     map[string]struct{}{},
		imagePullStart:   map[string]time.Time{},
		backoffCount:     map[string]int{},
	}
}

// InterpretEvent translates a raw Kubernetes event into a user-friendly message
// (or nil if the event should be silenced).
func (s *podEventState) InterpretEvent(event *corev1.Event, now time.Time) *EventInterpretation {
	reason := event.Reason
	msg := event.Message

	switch reason {
	// ── Scheduling ───────────────────────────────────────────────────────────
	case "Scheduled":
		s.transitionPhase(PhaseScheduling, now)
		node := extractNodeFromScheduled(msg)
		if node != "" {
			return &EventInterpretation{
				Kind:        KindProgress,
				Phase:       PhaseScheduling,
				UserMessage: fmt.Sprintf("Workspace assigned to node %s", node),
				Level:       codersdk.LogLevelInfo,
			}
		}
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       PhaseScheduling,
			UserMessage: "Workspace assigned to a node, preparing to start",
			Level:       codersdk.LogLevelInfo,
		}

	case "NotTriggerScaleUp":
		// Cluster autoscaler says no node is available and it won't scale. Real problem.
		return &EventInterpretation{
			Kind:        KindRealError,
			Phase:       PhaseFailed,
			UserMessage: "No available nodes and cluster autoscaler cannot scale up. Contact your administrator.",
			Level:       codersdk.LogLevelError,
		}

	case "FailedScheduling":
		// Could be transient (autoscaler is spinning up a node) or real.
		// Heuristic: if we've been in this state < threshold, it's noise.
		elapsed := now.Sub(s.phaseEnteredAt)
		s.transitionPhase(PhaseScheduling, now)

		// Check for specific patterns that indicate autoscaler activity
		if containsAny(msg, "0/", "nodes are available", "Insufficient", "node(s) had") {
			if elapsed < TransientThreshold {
				// Autoscaler is likely working. Show once, quietly.
				if _, shown := s.shownReasons[reason]; !shown {
					s.shownReasons[reason] = struct{}{}
					return &EventInterpretation{
						Kind:        KindProgress,
						Phase:       PhaseScheduling,
						UserMessage: "Waiting for an available node (cluster may be scaling up)…",
						Level:       codersdk.LogLevelInfo,
					}
				}
				return nil // silence repeats
			}
			// Past threshold — upgrade to a visible warning
			last := s.lastSlowWarnSent[reason]
			if now.Sub(last) > 60*time.Second {
				s.lastSlowWarnSent[reason] = now
				return &EventInterpretation{
					Kind:        KindSlowWarn,
					Phase:       PhaseScheduling,
					UserMessage: fmt.Sprintf("Still waiting for a node after %s. Autoscaler may be slow or at capacity.", elapsed.Round(time.Second)),
					Level:       codersdk.LogLevelWarn,
				}
			}
		}
		return nil

	case "TriggeredScaleUp":
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       PhaseScheduling,
			UserMessage: "Cluster is scaling up to accommodate your workspace — hang tight…",
			Level:       codersdk.LogLevelInfo,
		}

	// ── Image pull ───────────────────────────────────────────────────────────
	case "Pulling":
		container := extractContainerFromMsg(msg)
		s.imagePullStart[container] = now
		s.transitionPhase(PhasePulling, now)

		image := extractImageFromMsg(msg)
		if image != "" {
			return &EventInterpretation{
				Kind:        KindProgress,
				Phase:       PhasePulling,
				UserMessage: fmt.Sprintf("Downloading workspace image: %s", shortenImage(image)),
				Level:       codersdk.LogLevelInfo,
			}
		}
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       PhasePulling,
			UserMessage: "Downloading workspace image…",
			Level:       codersdk.LogLevelInfo,
		}

	case "Pulled":
		image := extractImageFromMsg(msg)
		container := extractContainerFromMsg(msg)
		elapsed := ""
		if start, ok := s.imagePullStart[container]; ok {
			elapsed = fmt.Sprintf(" (took %s)", now.Sub(start).Round(time.Second))
		}
		delete(s.imagePullStart, container)
		s.transitionPhase(PhaseStarting, now)
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       PhaseStarting,
			UserMessage: fmt.Sprintf("Image ready%s: %s", elapsed, shortenImage(image)),
			Level:       codersdk.LogLevelInfo,
		}

	case "ErrImageNeverPull":
		return &EventInterpretation{
			Kind:        KindRealError,
			Phase:       PhaseFailed,
			UserMessage: fmt.Sprintf("Image cannot be pulled (policy=Never and image is not present): %s", extractImageFromMsg(msg)),
			Level:       codersdk.LogLevelError,
		}

	case "BackOff":
		if containsAny(msg, "image", "pull") {
			// ImagePullBackOff — transient initially, real after N occurrences.
			container := extractContainerFromMsg(msg)
			s.backoffCount[container]++
			count := s.backoffCount[container]
			if count < 3 {
				// Probably still pulling or retrying — show once quietly.
				if count == 1 {
					return &EventInterpretation{
						Kind:        KindProgress,
						Phase:       PhasePulling,
						UserMessage: "Image pull is taking longer than expected, retrying…",
						Level:       codersdk.LogLevelWarn,
					}
				}
				return nil
			}
			// 3+ backoffs: surface as an error
			return &EventInterpretation{
				Kind:        KindRealError,
				Phase:       PhaseFailed,
				UserMessage: fmt.Sprintf("Workspace image pull keeps failing (attempt %d). Check the image name/registry or contact your administrator.", count),
				Level:       codersdk.LogLevelError,
			}
		}
		// CrashLoopBackOff or similar
		container := extractContainerFromMsg(msg)
		s.backoffCount[container]++
		count := s.backoffCount[container]
		if count < 3 {
			return nil
		}
		return &EventInterpretation{
			Kind:        KindRealError,
			Phase:       PhaseFailed,
			UserMessage: fmt.Sprintf("Workspace container keeps crashing (attempt %d). Check your workspace template configuration.", count),
			Level:       codersdk.LogLevelError,
		}

	// ── Container lifecycle ──────────────────────────────────────────────────
	case "Created":
		s.transitionPhase(PhaseStarting, now)
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       PhaseStarting,
			UserMessage: "Workspace container created, starting up…",
			Level:       codersdk.LogLevelInfo,
		}

	case "Started":
		s.transitionPhase(PhaseRunning, now)
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       PhaseRunning,
			UserMessage: "Workspace is running, waiting for agent to connect…",
			Level:       codersdk.LogLevelInfo,
		}

	case "Killing":
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       PhaseUnknown,
			UserMessage: "Workspace container is stopping",
			Level:       codersdk.LogLevelInfo,
		}

	case "OOMKilling", "OOMKilled":
		return &EventInterpretation{
			Kind:        KindRealError,
			Phase:       PhaseFailed,
			UserMessage: "Workspace ran out of memory (OOMKilled). Try a workspace template with more memory, or reduce your workload.",
			Level:       codersdk.LogLevelError,
		}

	// ── Volume / PVC ─────────────────────────────────────────────────────────
	case "FailedAttachVolume", "FailedMount":
		elapsed := now.Sub(s.phaseEnteredAt)
		if elapsed < TransientThreshold {
			if _, shown := s.shownReasons[reason]; !shown {
				s.shownReasons[reason] = struct{}{}
				return &EventInterpretation{
					Kind:        KindProgress,
					Phase:       s.currentPhase,
					UserMessage: "Waiting for workspace storage to attach…",
					Level:       codersdk.LogLevelInfo,
				}
			}
			return nil
		}
		last := s.lastSlowWarnSent[reason]
		if now.Sub(last) > 60*time.Second {
			s.lastSlowWarnSent[reason] = now
			return &EventInterpretation{
				Kind:        KindSlowWarn,
				Phase:       s.currentPhase,
				UserMessage: fmt.Sprintf("Storage attachment is taking longer than expected (%s). Your disk may be re-attaching from another zone.", elapsed.Round(time.Second)),
				Level:       codersdk.LogLevelWarn,
			}
		}
		return nil

	case "SuccessfulAttachVolume", "SuccessfulMountVolume":
		return &EventInterpretation{
			Kind:        KindProgress,
			Phase:       s.currentPhase,
			UserMessage: "Workspace storage attached successfully",
			Level:       codersdk.LogLevelInfo,
		}

	// ── Node pressure / toleration noise ────────────────────────────────────
	case "NodeNotReady", "NodeNotSchedulable":
		// These fire when a node briefly flaps. Almost always transient.
		return nil

	case "Preempting", "Preempted":
		return &EventInterpretation{
			Kind:        KindSlowWarn,
			Phase:       PhaseScheduling,
			UserMessage: "Workspace pod is being preempted by a higher-priority workload. It will be rescheduled shortly.",
			Level:       codersdk.LogLevelWarn,
		}

	// ── Readiness / liveness probes ──────────────────────────────────────────
	case "Unhealthy":
		if containsAny(msg, "Readiness probe") {
			// Readiness probe failures during startup are completely expected.
			return nil
		}
		if containsAny(msg, "Liveness probe") {
			s.backoffCount["liveness"]++
			if s.backoffCount["liveness"] < 3 {
				return nil
			}
			return &EventInterpretation{
				Kind:        KindRealError,
				Phase:       PhaseFailed,
				UserMessage: "Workspace container liveness probe keeps failing. The workspace may be misconfigured or crashing.",
				Level:       codersdk.LogLevelError,
			}
		}
		return nil

	default:
		// Silently drop anything we don't explicitly know about.
		// This prevents raw k8s jargon from leaking to users.
		return nil
	}
}

// HeartbeatMessage returns a "still waiting" message if we've been in a slow
// phase for a while with no updates. Returns empty string if no heartbeat needed.
func (s *podEventState) HeartbeatMessage(now time.Time) string {
	elapsed := now.Sub(s.phaseEnteredAt)
	// Only heartbeat during "expected waiting" phases
	switch s.currentPhase {
	case PhaseScheduling, PhasePulling:
		if elapsed > 60*time.Second && elapsed < 10*time.Minute {
			return fmt.Sprintf("Still %s… (%s elapsed)", strings.ToLower(s.currentPhase.String()), elapsed.Round(time.Second))
		}
	}
	return ""
}

func (s *podEventState) transitionPhase(p Phase, now time.Time) {
	if p > s.currentPhase {
		s.currentPhase = p
		s.phaseEnteredAt = now
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

func containsAny(s string, subs ...string) bool {
	sl := strings.ToLower(s)
	for _, sub := range subs {
		if strings.Contains(sl, strings.ToLower(sub)) {
			return true
		}
	}
	return false
}

func extractImageFromMsg(msg string) string {
	// "Pulling image "docker.io/foo/bar:latest""
	// "Successfully pulled image "docker.io/foo/bar:latest""
	lower := strings.ToLower(msg)
	for _, prefix := range []string{`pulling image "`, `pulled image "`, `failed to pull image "`} {
		if idx := strings.Index(lower, prefix); idx != -1 {
			rest := msg[idx+len(prefix):]
			if end := strings.Index(rest, `"`); end != -1 {
				return rest[:end]
			}
		}
	}
	return ""
}

func extractContainerFromMsg(msg string) string {
	// "Back-off pulling image "..." for container "mycontainer""
	// "Container mycontainer failed liveness probe"
	if idx := strings.Index(msg, `container "`); idx != -1 {
		rest := msg[idx+len(`container "`):]
		if end := strings.Index(rest, `"`); end != -1 {
			return rest[:end]
		}
	}
	return "unknown"
}

func extractNodeFromScheduled(msg string) string {
	// "Successfully assigned default/mypod to ip-10-0-1-5.us-east-2.compute.internal"
	const to = " to "
	if idx := strings.Index(msg, to); idx != -1 {
		return msg[idx+len(to):]
	}
	return ""
}

func shortenImage(image string) string {
	// Strip registry prefix for common registries to save space
	for _, prefix := range []string{
		"docker.io/library/",
		"docker.io/",
		"index.docker.io/library/",
		"index.docker.io/",
		"ghcr.io/",
		"gcr.io/",
		"quay.io/",
	} {
		if strings.HasPrefix(image, prefix) {
			return image[len(prefix):]
		}
	}
	return image
}

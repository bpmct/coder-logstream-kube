// Package main - event_translator.go
// Translates raw Kubernetes events into human-readable, phase-aware messages.
//
// Simple 1:1 reason→message mappings live in event_rules.yaml (loaded via
// event_rules_loader.go). Cases requiring dynamic logic are handled here.
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
	PhaseUnknown    Phase = iota
	PhasePending          // pod created, not yet scheduled
	PhaseScheduling       // scheduler is working (may be waiting for a node)
	PhasePulling          // at least one container is pulling an image
	PhaseStarting         // containers starting their init
	PhaseRunning          // agent should be connecting soon
	PhaseFailed           // something is genuinely wrong
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
	UserMessage string
	Level       codersdk.LogLevel
}

// TransientThreshold is how long we allow a "slow" condition before
// upgrading it from noise to a visible warning.
const TransientThreshold = 90 * time.Second

// podEventState tracks per-pod state for debouncing and deduplication.
type podEventState struct {
	currentPhase     Phase
	phaseEnteredAt   time.Time
	lastSlowWarnSent map[string]time.Time // reason → last slow-warn emission
	shownReasons     map[string]struct{}  // reasons surfaced at least once
	imagePullStart   map[string]time.Time // container → pull-start time
	backoffCount     map[string]int       // container/reason → consecutive count
	staticRules      *StaticRules
}

func newPodEventState() *podEventState {
	rules, err := LoadStaticRules()
	if err != nil {
		rules = &StaticRules{byReason: map[string]staticRule{}, heartbeats: map[Phase]HeartbeatConfig{}}
	}
	return &podEventState{
		currentPhase:     PhaseUnknown,
		phaseEnteredAt:   time.Now(),
		lastSlowWarnSent: map[string]time.Time{},
		shownReasons:     map[string]struct{}{},
		imagePullStart:   map[string]time.Time{},
		backoffCount:     map[string]int{},
		staticRules:      rules,
	}
}

// InterpretEvent translates a raw Kubernetes event into a user-friendly
// message, or nil if the event should be silenced.
func (s *podEventState) InterpretEvent(event *corev1.Event, now time.Time) *EventInterpretation {
	reason, msg := event.Reason, event.Message

	switch reason {

	case "Scheduled":
		s.transitionPhase(PhaseScheduling, now)
		text := "Workspace assigned to a node, preparing to start"
		if node := extractNodeFromScheduled(msg); node != "" {
			text = "Workspace assigned to node " + node
		}
		return info(KindProgress, PhaseScheduling, text)

	case "FailedScheduling":
		s.transitionPhase(PhaseScheduling, now)
		if !containsAny(msg, "0/", "nodes are available", "Insufficient", "node(s) had") {
			return nil
		}
		return s.showOnceOrThrottle(reason, now,
			"Waiting for an available node (cluster may be scaling up)…",
			"Still waiting for a node — the cluster is scaling up in the background. No action needed.",
			PhaseScheduling,
		)

	case "Pulling":
		container := extractContainerFromMsg(msg)
		s.imagePullStart[container] = now
		s.transitionPhase(PhasePulling, now)
		text := "Downloading workspace image…"
		if image := extractImageFromMsg(msg); image != "" {
			text = "Downloading workspace image: " + shortenImage(image)
		}
		return info(KindProgress, PhasePulling, text)

	case "Pulled":
		container := extractContainerFromMsg(msg)
		elapsed := ""
		if start, ok := s.imagePullStart[container]; ok {
			elapsed = fmt.Sprintf(" (took %s)", now.Sub(start).Round(time.Second))
			delete(s.imagePullStart, container)
		}
		s.transitionPhase(PhaseStarting, now)
		return info(KindProgress, PhaseStarting,
			fmt.Sprintf("Image ready%s: %s", elapsed, shortenImage(extractImageFromMsg(msg))))

	case "BackOff":
		return s.handleBackOff(msg)

	case "FailedAttachVolume", "FailedMount":
		return s.showOnceOrThrottle(reason, now,
			"Waiting for workspace storage to attach…",
			"Storage is taking a while to attach — this can happen when re-attaching across zones. Hang tight.",
			s.currentPhase,
		)

	case "Unhealthy":
		return s.handleUnhealthy(msg)

	default:
		if rule, ok := s.staticRules.Lookup(reason); ok {
			return s.applyStaticRule(rule, now)
		}
		return nil // silently drop unknown events — never leak raw k8s jargon
	}
}

// HeartbeatMessage returns a calm reassurance when a phase is taking a while.
func (s *podEventState) HeartbeatMessage(now time.Time) string {
	hcfg, ok := s.staticRules.Heartbeat(s.currentPhase)
	if !ok || len(hcfg.Messages) == 0 {
		return ""
	}
	elapsed := now.Sub(s.phaseEnteredAt)
	after := time.Duration(hcfg.AfterSeconds) * time.Second
	if elapsed < after || elapsed > 10*time.Minute {
		return ""
	}
	return hcfg.Messages[(int(elapsed.Seconds())/hcfg.AfterSeconds)%len(hcfg.Messages)]
}

// ── private helpers ───────────────────────────────────────────────────────────

// showOnceOrThrottle implements the common pattern:
//   - before TransientThreshold: show firstMsg once, then silence
//   - after TransientThreshold: show slowMsg at most once per 60s
func (s *podEventState) showOnceOrThrottle(reason string, now time.Time, firstMsg, slowMsg string, phase Phase) *EventInterpretation {
	if now.Sub(s.phaseEnteredAt) < TransientThreshold {
		if _, shown := s.shownReasons[reason]; shown {
			return nil
		}
		s.shownReasons[reason] = struct{}{}
		return info(KindProgress, phase, firstMsg)
	}
	if now.Sub(s.lastSlowWarnSent[reason]) <= 60*time.Second {
		return nil
	}
	s.lastSlowWarnSent[reason] = now
	return &EventInterpretation{Kind: KindSlowWarn, Phase: phase, UserMessage: slowMsg, Level: codersdk.LogLevelWarn}
}

// handleBackOff handles BackOff events (ImagePullBackOff and CrashLoopBackOff).
func (s *podEventState) handleBackOff(msg string) *EventInterpretation {
	if containsAny(msg, "image", "pull") {
		return s.countedBackoff("pull:"+extractContainerFromMsg(msg), PhasePulling,
			"Image pull is taking longer than expected, retrying…",
			"Workspace image pull keeps failing (attempt %d). Check the image name/registry or contact your administrator.")
	}
	return s.countedBackoff(extractContainerFromMsg(msg), PhaseFailed,
		"", // no first-occurrence message for crash loops
		"Workspace container keeps crashing (attempt %d). Check your workspace template configuration.")
}

// countedBackoff increments the counter for key and returns:
//   - firstMsg (info) on the first occurrence (if non-empty)
//   - nil while count < 3
//   - error with errFmt (printf, %d=count) once count reaches 3+
func (s *podEventState) countedBackoff(key string, phase Phase, firstMsg, errFmt string) *EventInterpretation {
	s.backoffCount[key]++
	count := s.backoffCount[key]
	switch {
	case count == 1 && firstMsg != "":
		return &EventInterpretation{Kind: KindProgress, Phase: phase, UserMessage: firstMsg, Level: codersdk.LogLevelWarn}
	case count < 3:
		return nil
	default:
		return &EventInterpretation{Kind: KindRealError, Phase: PhaseFailed,
			UserMessage: fmt.Sprintf(errFmt, count), Level: codersdk.LogLevelError}
	}
}

// handleUnhealthy handles probe failures:
//   - readiness probes during startup → always silent
//   - liveness probes → silent until 3+ failures, then error
func (s *podEventState) handleUnhealthy(msg string) *EventInterpretation {
	switch {
	case containsAny(msg, "Readiness probe"):
		return nil
	case containsAny(msg, "Liveness probe"):
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
	default:
		return nil
	}
}

// applyStaticRule converts a compiled staticRule into an EventInterpretation.
func (s *podEventState) applyStaticRule(rule staticRule, now time.Time) *EventInterpretation {
	if rule.kind == KindNoise {
		return nil
	}
	phase := s.currentPhase
	if rule.phase != PhaseUnknown {
		phase = rule.phase
		s.transitionPhase(rule.phase, now)
	}
	return &EventInterpretation{Kind: rule.kind, Phase: phase, UserMessage: rule.message, Level: rule.level}
}

func (s *podEventState) transitionPhase(p Phase, now time.Time) {
	if p > s.currentPhase {
		s.currentPhase = p
		s.phaseEnteredAt = now
	}
}

// info is a convenience constructor for KindProgress / LogLevelInfo results.
func info(kind EventKind, phase Phase, msg string) *EventInterpretation {
	return &EventInterpretation{Kind: kind, Phase: phase, UserMessage: msg, Level: codersdk.LogLevelInfo}
}

// ── string helpers ────────────────────────────────────────────────────────────

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
	if idx := strings.Index(msg, `container "`); idx != -1 {
		rest := msg[idx+len(`container "`):]
		if end := strings.Index(rest, `"`); end != -1 {
			return rest[:end]
		}
	}
	return "unknown"
}

func extractNodeFromScheduled(msg string) string {
	// "Successfully assigned default/mypod to <node>"
	if idx := strings.Index(msg, " to "); idx != -1 {
		return msg[idx+4:]
	}
	return ""
}

func shortenImage(image string) string {
	for _, prefix := range []string{
		"docker.io/library/", "docker.io/",
		"index.docker.io/library/", "index.docker.io/",
		"ghcr.io/", "gcr.io/", "quay.io/",
	} {
		if strings.HasPrefix(image, prefix) {
			return image[len(prefix):]
		}
	}
	return image
}

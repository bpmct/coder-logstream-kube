// Package main - event_rules_loader.go
// Loads simple 1:1 Kubernetes event reason → user message mappings from a YAML
// file (event_rules.yaml) that is embedded at build time.
//
// Rules that require dynamic logic (backoff counting, threshold checks, message
// extraction) live in event_translator.go instead.
package main

import (
	_ "embed"
	"fmt"
	"strings"

	"github.com/coder/coder/v2/codersdk"
	"sigs.k8s.io/yaml"
)

// eventRuleYAML is the on-disk representation of one static rule entry.
type eventRuleYAML struct {
	Reasons []string `json:"reasons"`
	Kind    string   `json:"kind"`    // noise | progress | slow_warn | error
	Phase   string   `json:"phase"`   // unknown | current | scheduling | pulling | starting | running | failed
	Level   string   `json:"level"`   // info | warn | error  (omitted for noise)
	Message string   `json:"message"` // human-readable text; empty for noise rules
}

// heartbeatConfigYAML is the on-disk shape for per-phase heartbeat config.
type heartbeatConfigYAML struct {
	AfterSeconds int      `json:"after_seconds"`
	Messages     []string `json:"messages"`
}

// eventRulesFileYAML mirrors the top-level structure of event_rules.yaml.
type eventRulesFileYAML struct {
	Rules      []eventRuleYAML                `json:"rules"`
	Heartbeats map[string]heartbeatConfigYAML `json:"heartbeats"`
}

// staticRule is the compiled, ready-to-use form of an eventRuleYAML entry.
type staticRule struct {
	kind    EventKind
	phase   Phase // PhaseUnknown means "keep current phase" (both "unknown" and "current" map here)
	level   codersdk.LogLevel
	message string // empty → silence (noise rules)
}

// HeartbeatConfig is the compiled heartbeat config for one phase.
type HeartbeatConfig struct {
	AfterSeconds int
	Messages     []string
}

// StaticRules is the compiled lookup table: reason → staticRule.
type StaticRules struct {
	byReason   map[string]staticRule
	heartbeats map[Phase]HeartbeatConfig
}

//go:embed event_rules.yaml
var embeddedRulesYAML []byte

// LoadStaticRules loads and compiles the embedded event_rules.yaml.
func LoadStaticRules() (*StaticRules, error) {
	return parseStaticRules(embeddedRulesYAML)
}

// parseStaticRules is split out so it can be tested without the embed.
func parseStaticRules(data []byte) (*StaticRules, error) {
	var f eventRulesFileYAML
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, fmt.Errorf("parse event_rules.yaml: %w", err)
	}

	sr := &StaticRules{
		byReason:   make(map[string]staticRule),
		heartbeats: make(map[Phase]HeartbeatConfig),
	}

	for _, r := range f.Rules {
		if len(r.Reasons) == 0 {
			continue
		}
		compiled, err := compileRule(r)
		if err != nil {
			return nil, fmt.Errorf("invalid rule for reasons %v: %w", r.Reasons, err)
		}
		for _, reason := range r.Reasons {
			sr.byReason[reason] = compiled
		}
	}

	for phaseName, hcfg := range f.Heartbeats {
		p, err := parsePhase(phaseName)
		if err != nil {
			return nil, fmt.Errorf("unknown phase %q in heartbeats: %w", phaseName, err)
		}
		sr.heartbeats[p] = HeartbeatConfig{
			AfterSeconds: hcfg.AfterSeconds,
			Messages:     hcfg.Messages,
		}
	}

	return sr, nil
}

// Lookup returns the static rule for the given event reason, or (zero, false).
func (sr *StaticRules) Lookup(reason string) (staticRule, bool) {
	r, ok := sr.byReason[reason]
	return r, ok
}

// Heartbeat returns the heartbeat config for the given phase, or (zero, false).
func (sr *StaticRules) Heartbeat(phase Phase) (HeartbeatConfig, bool) {
	h, ok := sr.heartbeats[phase]
	return h, ok
}

// ── compilers ────────────────────────────────────────────────────────────────

func compileRule(r eventRuleYAML) (staticRule, error) {
	kind, err := parseKind(r.Kind)
	if err != nil {
		return staticRule{}, err
	}
	if kind == KindNoise {
		return staticRule{kind: KindNoise}, nil
	}
	phase, err := parsePhase(r.Phase)
	if err != nil {
		return staticRule{}, err
	}
	level, err := parseLevel(r.Level)
	if err != nil {
		return staticRule{}, err
	}
	if r.Message == "" {
		return staticRule{}, fmt.Errorf("message is required for non-noise rules")
	}
	return staticRule{
		kind:    kind,
		phase:   phase,
		level:   level,
		message: r.Message,
	}, nil
}

func parseKind(s string) (EventKind, error) {
	switch strings.ToLower(s) {
	case "noise":
		return KindNoise, nil
	case "progress":
		return KindProgress, nil
	case "slow_warn":
		return KindSlowWarn, nil
	case "error":
		return KindRealError, nil
	default:
		return 0, fmt.Errorf("unknown kind %q", s)
	}
}

func parsePhase(s string) (Phase, error) {
	switch strings.ToLower(s) {
	case "unknown", "current", "":
		// Both "unknown" and "current" mean "keep the phase we're already in".
		// We represent this as PhaseUnknown (zero value) and check for it in
		// applyStaticRule.
		return PhaseUnknown, nil
	case "pending":
		return PhasePending, nil
	case "scheduling":
		return PhaseScheduling, nil
	case "pulling":
		return PhasePulling, nil
	case "starting":
		return PhaseStarting, nil
	case "running":
		return PhaseRunning, nil
	case "failed":
		return PhaseFailed, nil
	default:
		return 0, fmt.Errorf("unknown phase %q", s)
	}
}

func parseLevel(s string) (codersdk.LogLevel, error) {
	switch strings.ToLower(s) {
	case "info", "":
		return codersdk.LogLevelInfo, nil
	case "warn":
		return codersdk.LogLevelWarn, nil
	case "error":
		return codersdk.LogLevelError, nil
	default:
		return "", fmt.Errorf("unknown level %q", s)
	}
}

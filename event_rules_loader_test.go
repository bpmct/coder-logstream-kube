package main

import (
	"testing"

	"github.com/coder/coder/v2/codersdk"
)

func TestParseStaticRulesValid(t *testing.T) {
	t.Parallel()

	data := []byte(`
rules:
  - reasons: [Created]
    kind: progress
    phase: starting
    level: info
    message: "Container created"

  - reasons: [NodeNotReady, NodeNotSchedulable]
    kind: noise

  - reasons: [OOMKilling, OOMKilled]
    kind: error
    phase: failed
    level: error
    message: "OOM killed"

heartbeats:
  scheduling:
    after_seconds: 60
    messages:
      - "Hang tight…"
  pulling:
    after_seconds: 45
    messages:
      - "Still downloading…"
`)

	sr, err := parseStaticRules(data)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// "Created" should be a progress/starting/info rule
	r, ok := sr.Lookup("Created")
	if !ok {
		t.Fatal("expected rule for Created")
	}
	if r.kind != KindProgress {
		t.Errorf("Created: want KindProgress, got %v", r.kind)
	}
	if r.phase != PhaseStarting {
		t.Errorf("Created: want PhaseStarting, got %v", r.phase)
	}
	if r.level != codersdk.LogLevelInfo {
		t.Errorf("Created: want LogLevelInfo, got %v", r.level)
	}
	if r.message != "Container created" {
		t.Errorf("Created: unexpected message %q", r.message)
	}

	// "NodeNotReady" and "NodeNotSchedulable" should both be noise
	for _, noise := range []string{"NodeNotReady", "NodeNotSchedulable"} {
		r, ok := sr.Lookup(noise)
		if !ok {
			t.Fatalf("expected rule for %s", noise)
		}
		if r.kind != KindNoise {
			t.Errorf("%s: want KindNoise, got %v", noise, r.kind)
		}
	}

	// "OOMKilling" and "OOMKilled" should both map to the same error rule
	for _, oom := range []string{"OOMKilling", "OOMKilled"} {
		r, ok := sr.Lookup(oom)
		if !ok {
			t.Fatalf("expected rule for %s", oom)
		}
		if r.kind != KindRealError {
			t.Errorf("%s: want KindRealError, got %v", oom, r.kind)
		}
		if r.phase != PhaseFailed {
			t.Errorf("%s: want PhaseFailed, got %v", oom, r.phase)
		}
	}

	// Heartbeats
	hb, ok := sr.Heartbeat(PhaseScheduling)
	if !ok {
		t.Fatal("expected heartbeat config for PhaseScheduling")
	}
	if hb.AfterSeconds != 60 {
		t.Errorf("scheduling heartbeat: want 60s, got %d", hb.AfterSeconds)
	}
	if len(hb.Messages) != 1 || hb.Messages[0] != "Hang tight…" {
		t.Errorf("scheduling heartbeat: unexpected messages %v", hb.Messages)
	}

	hb2, ok := sr.Heartbeat(PhasePulling)
	if !ok {
		t.Fatal("expected heartbeat config for PhasePulling")
	}
	if hb2.AfterSeconds != 45 {
		t.Errorf("pulling heartbeat: want 45s, got %d", hb2.AfterSeconds)
	}
}

func TestParseStaticRulesUnknownReason(t *testing.T) {
	t.Parallel()

	sr, err := parseStaticRules([]byte(`rules: []`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_, ok := sr.Lookup("SomeRandomEvent")
	if ok {
		t.Error("expected no rule for unknown reason")
	}
}

func TestParseStaticRulesInvalidKind(t *testing.T) {
	t.Parallel()

	data := []byte(`
rules:
  - reasons: [Foo]
    kind: badkind
    phase: starting
    level: info
    message: "msg"
`)
	_, err := parseStaticRules(data)
	if err == nil {
		t.Fatal("expected error for unknown kind")
	}
}

func TestParseStaticRulesMissingMessage(t *testing.T) {
	t.Parallel()

	data := []byte(`
rules:
  - reasons: [Foo]
    kind: progress
    phase: starting
    level: info
`)
	_, err := parseStaticRules(data)
	if err == nil {
		t.Fatal("expected error when message is missing for non-noise rule")
	}
}

func TestLoadStaticRulesEmbedded(t *testing.T) {
	t.Parallel()

	// Ensure the embedded event_rules.yaml loads without error and contains
	// the expected well-known reasons.
	sr, err := LoadStaticRules()
	if err != nil {
		t.Fatalf("LoadStaticRules: %v", err)
	}

	wellKnown := []string{
		"TriggeredScaleUp",
		"NotTriggerScaleUp",
		"Created",
		"Started",
		"Killing",
		"OOMKilling",
		"OOMKilled",
		"SuccessfulAttachVolume",
		"SuccessfulMountVolume",
		"NodeNotReady",
		"NodeNotSchedulable",
		"Preempting",
		"Preempted",
		"ErrImageNeverPull",
	}
	for _, r := range wellKnown {
		if _, ok := sr.Lookup(r); !ok {
			t.Errorf("embedded rules missing reason %q", r)
		}
	}

	// Heartbeats should be present for scheduling and pulling
	for _, phase := range []Phase{PhaseScheduling, PhasePulling} {
		if _, ok := sr.Heartbeat(phase); !ok {
			t.Errorf("embedded rules missing heartbeat for phase %v", phase)
		}
	}
}

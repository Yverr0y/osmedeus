package parser

import (
	"errors"
	"testing"
)

// A Loader built with an empty workflows directory must say so rather than
// silently resolving every lookup against the process working directory and
// reporting a misleading "not found". Regression test for #321/#322.
func TestLoader_EmptyDirReportsNotConfigured(t *testing.T) {
	l := NewLoader("")

	t.Run("by name", func(t *testing.T) {
		_, err := l.LoadWorkflow("domain-extensive")
		if !errors.Is(err, ErrWorkflowsDirNotConfigured) {
			t.Fatalf("got %v, want ErrWorkflowsDirNotConfigured", err)
		}
	})

	t.Run("by relative path", func(t *testing.T) {
		_, err := l.LoadWorkflowByPath("common/enum-subdomain.yaml")
		if !errors.Is(err, ErrWorkflowsDirNotConfigured) {
			t.Fatalf("got %v, want ErrWorkflowsDirNotConfigured", err)
		}
	})

	t.Run("list", func(t *testing.T) {
		if _, _, err := l.ListAllWorkflows(); !errors.Is(err, ErrWorkflowsDirNotConfigured) {
			t.Fatalf("got %v, want ErrWorkflowsDirNotConfigured", err)
		}
	})

	t.Run("load all", func(t *testing.T) {
		if _, err := l.LoadAllWorkflows(); !errors.Is(err, ErrWorkflowsDirNotConfigured) {
			t.Fatalf("got %v, want ErrWorkflowsDirNotConfigured", err)
		}
	})
}

// A configured loader is unaffected by the guard.
func TestLoader_ConfiguredDirStillWorks(t *testing.T) {
	l := NewLoader(t.TempDir())

	_, err := l.LoadWorkflow("nope")
	if errors.Is(err, ErrWorkflowsDirNotConfigured) {
		t.Fatal("configured loader must not report a missing workflows dir")
	}
	if err == nil {
		t.Fatal("expected a not-found error")
	}
}

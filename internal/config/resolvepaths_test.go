package config

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A settings file loaded via LoadFromFile (the --settings-file path) must still
// end up with every derived runtime path populated. Regression test for #321/#322,
// where WorkflowsPath stayed empty and every workflow lookup resolved against the
// process working directory instead of base_folder.
func TestResolvePaths_PopulatesDerivedPaths(t *testing.T) {
	base := t.TempDir()
	cfg := &Config{
		BaseFolder: base,
		Environments: EnvironmentConfig{
			Workflows:  "{{base_folder}}/workflows",
			Workspaces: "{{base_folder}}/workspaces",
		},
	}

	cfg.ResolvePaths()

	assert.Equal(t, filepath.Join(base, "workflows"), cfg.WorkflowsPath)
	assert.Equal(t, filepath.Join(base, "workspaces"), cfg.WorkspacesPath)
}

// A partial settings file that omits environment keys must not leave a derived
// path empty -- an empty WorkflowsPath makes filepath.Join fall through to a
// relative path rooted at the process cwd.
func TestResolvePaths_FillsMissingEnvironmentDefaults(t *testing.T) {
	base := t.TempDir()
	cfg := &Config{BaseFolder: base} // no Environments block at all

	cfg.ResolvePaths()

	assert.Equal(t, filepath.Join(base, "workflows"), cfg.WorkflowsPath)
	assert.Equal(t, filepath.Join(base, "workspaces"), cfg.WorkspacesPath)
	assert.Equal(t, filepath.Join(base, "external-binaries"), cfg.BinariesPath)
	assert.Equal(t, filepath.Join(base, "external-data"), cfg.DataPath)
	assert.Equal(t, filepath.Join(base, "external-configs"), cfg.ConfigsPath)
	assert.Equal(t, filepath.Join(base, "snapshot"), cfg.SnapshotPath)
	assert.Equal(t, filepath.Join(base, "external-scripts"), cfg.ExternalScriptsPath)
}

// An explicit path in the settings file always wins over the fallback.
func TestResolvePaths_ExplicitPathWinsOverDefault(t *testing.T) {
	cfg := &Config{
		BaseFolder:   "/srv/osm",
		Environments: EnvironmentConfig{Workflows: "/custom/workflows"},
	}

	cfg.ResolvePaths()

	assert.Equal(t, "/custom/workflows", cfg.WorkflowsPath)
	assert.Equal(t, "/srv/osm/workspaces", cfg.WorkspacesPath)
}

// ResolvePaths must be idempotent -- root.go and hotreload both call it on
// configs that may already be resolved.
func TestResolvePaths_Idempotent(t *testing.T) {
	base := t.TempDir()
	cfg := &Config{BaseFolder: base}

	cfg.ResolvePaths()
	first := cfg.WorkflowsPath
	cfg.ResolvePaths()

	assert.Equal(t, first, cfg.WorkflowsPath)
}

// LoadFromFile itself stays a plain unmarshal; callers resolve. This pins the
// contract so the root.go fix is not silently undone.
func TestLoadFromFile_LeavesDerivedPathsUnresolved(t *testing.T) {
	base := t.TempDir()
	body := "base_folder: " + base + "\nenvironments:\n  workflows: \"{{base_folder}}/workflows\"\n"
	settings := writeSettings(t, base, body)

	cfg, err := LoadFromFile(settings)
	require.NoError(t, err)
	assert.Empty(t, cfg.WorkflowsPath, "LoadFromFile should not resolve; Load and callers do")

	cfg.ResolvePaths()
	assert.Equal(t, filepath.Join(base, "workflows"), cfg.WorkflowsPath)
}

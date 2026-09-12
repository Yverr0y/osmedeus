package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeSettings writes an osm-settings.yaml into base and returns its path.
func writeSettings(t *testing.T, base, body string) string {
	t.Helper()
	path := filepath.Join(base, "osm-settings.yaml")
	require.NoError(t, os.WriteFile(path, []byte(body), 0644))
	return path
}

// The two mappings named in the feature request (#320).
func TestApplyEnvOverrides_RequestedMappings(t *testing.T) {
	t.Setenv("OSM_DATABASE_USERNAME", "from-env")
	t.Setenv("OSM_SERVER_SIMPLE_USER_MAP_KEY_USER1", "s3cret")

	cfg := &Config{
		Database: DatabaseConfig{Username: "from-file"},
	}

	applied := ApplyEnvOverrides(cfg)

	assert.Equal(t, "from-env", cfg.Database.Username)
	assert.Equal(t, "s3cret", cfg.Server.SimpleUserMapKey["user1"])
	assert.Contains(t, applied, "OSM_DATABASE_USERNAME")
	assert.Contains(t, applied, "OSM_SERVER_SIMPLE_USER_MAP_KEY_USER1")
}

func TestApplyEnvOverrides_ScalarTypes(t *testing.T) {
	t.Setenv("OSM_DATABASE_PORT", "6543")
	t.Setenv("OSM_SERVER_HOST", "127.0.0.1")
	t.Setenv("OSM_SERVER_ENABLED_AUTH_API", "false")

	cfg := &Config{
		Database: DatabaseConfig{Port: 5432},
		Server:   ServerConfig{Host: "0.0.0.0", EnabledAuthAPI: true},
	}

	ApplyEnvOverrides(cfg)

	assert.Equal(t, 6543, cfg.Database.Port)
	assert.Equal(t, "127.0.0.1", cfg.Server.Host)
	assert.False(t, cfg.Server.EnabledAuthAPI)
}

// Nested sections extend the prefix rather than needing their own registration.
func TestApplyEnvOverrides_NestedStruct(t *testing.T) {
	t.Setenv("OSM_SERVER_JWT_SECRET_SIGNING_KEY", "env-signing-key")
	t.Setenv("OSM_SERVER_JWT_EXPIRATION_MINUTES", "60")

	cfg := &Config{}
	ApplyEnvOverrides(cfg)

	assert.Equal(t, "env-signing-key", cfg.Server.JWT.SecretSigningKey)
	assert.Equal(t, 60, cfg.Server.JWT.ExpirationMinutes)
}

// Runtime-only (`yaml:"-"`) fields are derived by ResolvePaths and must not be
// settable from the environment, or they would be silently recomputed anyway.
func TestApplyEnvOverrides_SkipsRuntimeOnlyFields(t *testing.T) {
	t.Setenv("OSM_WORKFLOWSPATH", "/hijacked")

	cfg := &Config{BaseFolder: t.TempDir()}
	applied := ApplyEnvOverrides(cfg)

	assert.NotContains(t, applied, "OSM_WORKFLOWSPATH")
	assert.Empty(t, cfg.WorkflowsPath)
}

// An override on an environments.* path survives into the resolved runtime path.
func TestApplyEnvOverrides_FeedsPathResolution(t *testing.T) {
	t.Setenv("OSM_ENVIRONMENTS_WORKFLOWS", "/custom/wf")

	base := t.TempDir()
	cfg := &Config{BaseFolder: base}
	ApplyEnvOverrides(cfg)
	cfg.ResolvePaths()

	assert.Equal(t, "/custom/wf", cfg.WorkflowsPath)
	// Untouched keys still get their defaults.
	assert.Equal(t, filepath.Join(base, "workspaces"), cfg.WorkspacesPath)
}

// A malformed value must not be silently coerced -- the file value stands.
func TestApplyEnvOverrides_InvalidValueIsIgnored(t *testing.T) {
	t.Setenv("OSM_DATABASE_PORT", "not-a-number")

	cfg := &Config{Database: DatabaseConfig{Port: 5432}}
	applied := ApplyEnvOverrides(cfg)

	assert.Equal(t, 5432, cfg.Database.Port, "file value should stand")
	assert.NotContains(t, applied, "OSM_DATABASE_PORT")
}

// Absent env vars change nothing.
func TestApplyEnvOverrides_NoEnvIsNoop(t *testing.T) {
	cfg := &Config{
		BaseFolder: "/srv/osm",
		Database:   DatabaseConfig{Username: "osmedeus", Port: 5432},
	}

	applied := ApplyEnvOverrides(cfg)

	assert.Empty(t, applied)
	assert.Equal(t, "osmedeus", cfg.Database.Username)
	assert.Equal(t, 5432, cfg.Database.Port)
	assert.Equal(t, "/srv/osm", cfg.BaseFolder)
}

func TestApplyEnvOverrides_NilConfigIsSafe(t *testing.T) {
	assert.Nil(t, ApplyEnvOverrides(nil))
}

// End to end through the real loader: file on disk + env override.
func TestLoad_EnvOverridesSettingsFile(t *testing.T) {
	base := t.TempDir()
	body := "base_folder: " + base + "\n" +
		"database:\n  username: from-file\n  port: 5432\n" +
		"environments:\n  workflows: \"{{base_folder}}/workflows\"\n"
	writeSettings(t, base, body)

	t.Setenv("OSM_DATABASE_USERNAME", "from-env")

	cfg, err := Load(base)
	require.NoError(t, err)

	assert.Equal(t, "from-env", cfg.Database.Username, "env must win over the file")
	assert.Equal(t, 5432, cfg.Database.Port, "un-overridden file values survive")
	assert.Equal(t, filepath.Join(base, "workflows"), cfg.WorkflowsPath)
}

// LoadFromFile stays override-free: `osmedeus config` round-trips the settings
// file through it and writes it back, so an env secret must not reach the file.
func TestLoadFromFile_DoesNotApplyEnvOverrides(t *testing.T) {
	base := t.TempDir()
	body := "base_folder: " + base + "\ndatabase:\n  username: from-file\n"
	settings := writeSettings(t, base, body)

	t.Setenv("OSM_DATABASE_USERNAME", "from-env")

	cfg, err := LoadFromFile(settings)
	require.NoError(t, err)

	assert.Equal(t, "from-file", cfg.Database.Username,
		"LoadFromFile must return the file verbatim so config writes do not persist env secrets")
}

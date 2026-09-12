package handlers

import (
	"encoding/json"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/j3ssie/osmedeus/v5/internal/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func postRun(t *testing.T, cfg *config.Config, body string) (int, map[string]interface{}) {
	t.Helper()
	app := fiber.New()
	app.Post("/runs", CreateRun(cfg, nil))

	req := httptest.NewRequest("POST", "/runs", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := app.Test(req)
	require.NoError(t, err)

	raw, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	require.NoError(t, json.Unmarshal(raw, &result))
	return resp.StatusCode, result
}

// An unconfigured WorkflowsPath must surface as a server misconfiguration, not as
// a 404 that makes the caller think their workflow name was wrong. Regression test
// for #321, where an empty WorkflowsPath was reported as "Workflow not found".
// The check itself lives in the loader (parser.ErrWorkflowsDirNotConfigured), so
// every call site gets it; this pins the handler's mapping of it to a 500.
func TestCreateRun_EmptyWorkflowsPathIsNotA404(t *testing.T) {
	cfg := &config.Config{BaseFolder: t.TempDir()} // WorkflowsPath deliberately unset

	status, result := postRun(t, cfg, `{"flow":"domain-extensive","target":"example.com"}`)

	assert.Equal(t, fiber.StatusInternalServerError, status)
	assert.Equal(t, true, result["error"])
	assert.Contains(t, result["message"], "Workflows path is not configured")
}

// A genuinely missing workflow still 404s, and the message now carries the reason
// instead of a bare "Workflow not found".
func TestCreateRun_MissingWorkflowReportsReason(t *testing.T) {
	cfg, _ := setupTestWorkflowDir(t)

	status, result := postRun(t, cfg, `{"flow":"does-not-exist","target":"example.com"}`)

	assert.Equal(t, fiber.StatusNotFound, status)
	assert.Equal(t, true, result["error"])
	msg, _ := result["message"].(string)
	assert.Contains(t, msg, "Workflow not found")
	assert.Contains(t, msg, "does-not-exist", "error should name the workflow that failed to load")
}

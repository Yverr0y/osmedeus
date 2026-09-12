package config

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
)

// EnvOverridePrefix is the prefix for environment variables that override
// settings-file values, e.g. OSM_DATABASE_USERNAME overrides database.username.
const EnvOverridePrefix = "OSM_"

// envListSeparator splits slice values, so OSM_SERVER_CORS_ORIGINS="a,b" becomes
// a two-element list.
const envListSeparator = ","

// ApplyEnvOverrides overlays environment variables onto an already-parsed
// configuration and returns the names of the variables it applied.
//
// The mapping is derived from the YAML field names, so it covers every setting
// without a per-field registration: join the YAML path with underscores, upper
// case it, and prefix it with OSM_.
//
//	database.username                  -> OSM_DATABASE_USERNAME
//	server.jwt.secret_signing_key      -> OSM_SERVER_JWT_SECRET_SIGNING_KEY
//	server.simple_user_map_key.user1   -> OSM_SERVER_SIMPLE_USER_MAP_KEY_USER1
//
// Runtime-only fields (`yaml:"-"`) are not overridable -- those are derived by
// ResolvePaths rather than configured. Lists are comma-separated. Map entries
// are addressed by suffixing the map's path with the key.
//
// This is deliberately not part of ParseConfig: `osmedeus config` round-trips the
// settings file through LoadFromFile and writes it back, and baking an
// environment-supplied secret into that file is exactly what this feature exists
// to avoid.
func ApplyEnvOverrides(cfg *Config) []string {
	if cfg == nil {
		return nil
	}
	var applied []string
	applyEnvToStruct(reflect.ValueOf(cfg).Elem(), EnvOverridePrefix, &applied)
	return applied
}

// applyEnvToStruct walks a struct, extending the env var prefix as it descends.
func applyEnvToStruct(v reflect.Value, prefix string, applied *[]string) {
	t := v.Type()

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		name, ok := envFieldName(field)
		if !ok {
			continue
		}
		key := prefix + name

		applyEnvToValue(v.Field(i), key, applied)
	}
}

// applyEnvToValue assigns an override to a single field, recursing into nested
// structs and pointers.
func applyEnvToValue(fv reflect.Value, key string, applied *[]string) {
	switch fv.Kind() {
	case reflect.Struct:
		applyEnvToStruct(fv, key+"_", applied)
		return

	case reflect.Ptr:
		if fv.Type().Elem().Kind() != reflect.Struct {
			break
		}
		// Only descend into an existing struct; allocating here would
		// materialize empty optional sections that were deliberately absent.
		if fv.IsNil() {
			return
		}
		applyEnvToStruct(fv.Elem(), key+"_", applied)
		return

	case reflect.Map:
		applyEnvToMap(fv, key, applied)
		return
	}

	raw, found := os.LookupEnv(key)
	if !found {
		return
	}
	if err := setScalar(fv, raw); err != nil {
		// A malformed value should not silently fall back to the file value.
		fmt.Fprintf(os.Stderr, "warning: ignoring %s: %v\n", key, err)
		return
	}
	*applied = append(*applied, key)
}

// applyEnvToMap fills map entries from any env var sharing the map's prefix.
// OSM_SERVER_SIMPLE_USER_MAP_KEY_USER1=secret sets the "user1" entry.
func applyEnvToMap(fv reflect.Value, key string, applied *[]string) {
	mt := fv.Type()
	if mt.Key().Kind() != reflect.String || mt.Elem().Kind() != reflect.String {
		return // Only string->string maps are addressable this way
	}

	entryPrefix := key + "_"
	for _, kv := range os.Environ() {
		name, raw, ok := strings.Cut(kv, "=")
		if !ok || !strings.HasPrefix(name, entryPrefix) {
			continue
		}
		entry := strings.ToLower(strings.TrimPrefix(name, entryPrefix))
		if entry == "" {
			continue
		}
		if fv.IsNil() {
			fv.Set(reflect.MakeMap(mt))
		}
		fv.SetMapIndex(reflect.ValueOf(entry), reflect.ValueOf(raw))
		*applied = append(*applied, name)
	}
}

// setScalar parses raw into fv according to fv's kind.
func setScalar(fv reflect.Value, raw string) error {
	switch fv.Kind() {
	case reflect.String:
		fv.SetString(raw)

	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("expected a boolean, got %q", raw)
		}
		fv.SetBool(b)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("expected an integer, got %q", raw)
		}
		if fv.OverflowInt(n) {
			return fmt.Errorf("value %q overflows %s", raw, fv.Type())
		}
		fv.SetInt(n)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return fmt.Errorf("expected an unsigned integer, got %q", raw)
		}
		if fv.OverflowUint(n) {
			return fmt.Errorf("value %q overflows %s", raw, fv.Type())
		}
		fv.SetUint(n)

	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return fmt.Errorf("expected a number, got %q", raw)
		}
		fv.SetFloat(f)

	case reflect.Slice:
		if fv.Type().Elem().Kind() != reflect.String {
			return fmt.Errorf("only string lists are overridable")
		}
		if strings.TrimSpace(raw) == "" {
			fv.Set(reflect.MakeSlice(fv.Type(), 0, 0))
			return nil
		}
		parts := strings.Split(raw, envListSeparator)
		out := reflect.MakeSlice(fv.Type(), 0, len(parts))
		for _, p := range parts {
			out = reflect.Append(out, reflect.ValueOf(strings.TrimSpace(p)))
		}
		fv.Set(out)

	default:
		return fmt.Errorf("unsupported field type %s", fv.Type())
	}
	return nil
}

// envFieldName derives a field's env var segment from its YAML tag, reporting
// false for fields that are not settings-file backed.
//
// Only the YAML name is ever used. Falling back to the Go field name for an
// untagged field would invent a second, undocumented spelling for the same
// setting -- a field with no YAML name is not a settings-file value at all.
func envFieldName(field reflect.StructField) (string, bool) {
	tag := field.Tag.Get("yaml")
	name, _, _ := strings.Cut(tag, ",")

	// "-" is a runtime-only field: derived, not configured.
	if name == "" || name == "-" {
		return "", false
	}

	return strings.ToUpper(name), true
}

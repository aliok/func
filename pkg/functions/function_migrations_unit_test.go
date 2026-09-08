package functions

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coreos/go-semver/semver"
	"gopkg.in/yaml.v2"
)

// TestMigrated ensures that the .Migrated() method returns whether or not the
// migrations were applied based on its self-reported .SpecVersion member.
func TestMigrated(t *testing.T) {
	vNext := semver.New(LastSpecVersion())
	vNext.BumpMajor()

	tests := []struct {
		name     string
		f        Function
		migrated bool
	}{{
		name:     "no migration stamp",
		f:        Function{},
		migrated: false, // function with no specVersion stamp should be not migrated.
	}, {
		name:     "explicit small specVersion",
		f:        Function{SpecVersion: "0.0.1"},
		migrated: false,
	}, {
		name:     "latest specVersion",
		f:        Function{SpecVersion: LastSpecVersion()},
		migrated: true,
	}, {
		name:     "future specVersion",
		f:        Function{SpecVersion: vNext.String()},
		migrated: true,
	}}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.f.Migrated() != test.migrated {
				t.Errorf("Expected %q.Migrated() to be %t when latest is %q",
					test.f.SpecVersion, test.migrated, LastSpecVersion())
			}
		})
	}
}

// TestMigrate ensures that functions have migrations apply the specVersion
// stamp on instantiation indicating migrations have been applied.
func TestMigrate(t *testing.T) {
	// Load an old function, as it an earlier version it has registered migrations
	// that will need to be applied.
	root := "testdata/migrations/v0.19.0"

	// Instantiate the function with the antiquated structure, which should cause
	// migrations to be applied in order, and result in a function whose version
	// compatibility is equivalent to the latest registered migration.
	f, err := NewFunction(root)
	if err != nil {
		t.Fatal(err)
	}
	if f.SpecVersion != LastSpecVersion() {
		t.Fatalf("Function was not migrated to %v on instantiation: specVersion is %v",
			LastSpecVersion(), f.SpecVersion)
	}
}

// TestMigrateToCreationStamp ensures that the creation timestamp migration
// introduced for functions 0.19.0 and earlier is applied.
func TestMigrateToCreationStamp(t *testing.T) {
	// Load a function of version 0.19.0, which should have the migration applied
	root := "testdata/migrations/v0.19.0"

	now := time.Now()
	f, err := NewFunction(root)
	if err != nil {
		t.Fatal(err)
	}

	if f.Created.Before(now) {
		t.Fatalf("migration not applied: expected timestamp to be now, got %v.", f.Created)
	}
}

// TestMigrateToBuilderImages ensures that the migration which migrates
// from "builder" and "builders" to "builderImages" is applied.  This results
// in the attributes being removed and no errors on load of the function with
// old schema.
func TestMigrateToBuilderImagesDefault(t *testing.T) {
	// Load a function created prior to the adoption of the builder images map
	// (was created with 'builder' and 'builders' which does not support different
	// builder implementations.
	root := "testdata/migrations/v0.23.0"

	// Without the migration, instantiating the older function would error
	// because its strict unmarshalling would fail parsing the unexpected
	// 'builder' and 'builders' members.
	_, err := NewFunction(root)
	if err != nil {
		t.Fatal(err)
	}
}

// TestMigrateToBuilderImagesCustom ensures that the migration to builderImages
// correctly carries forward a customized value for 'builder'.
func TestMigrateToBuilderImagesCustom(t *testing.T) {
	// An early version of a function which includes a customized value for
	// the 'builder'.  This should be correctly carried forward to
	// the namespaced 'builderImages' map as image for the "pack" builder.
	root := "testdata/migrations/v0.23.0-customized"
	expected := "example.com/user/custom-builder" // set in testdata func.yaml

	f, err := NewFunction(root)
	if err != nil {
		t.Fatal(f)
	}
	i, ok := f.Build.BuilderImages["pack"]
	if !ok {
		t.Fatal("migrated function does not include the pack builder images")
	}
	if i != expected {
		t.Fatalf("migrated function expected builder image '%v', got '%v'", expected, i)
	}

}

// TestMigrateToSpecVersion ensures that a func.yaml file with a "version" field
// is migrated to use the field name "specVersion"
func TestMigrateToSpecVersion(t *testing.T) {
	root := "testdata/migrations/v0.25.0"
	f, err := NewFunction(root)
	if err != nil {
		t.Fatal(err)
	}
	if f.SpecVersion != LastSpecVersion() {
		t.Fatal("migrated function does not include the Migration field")
	}
}

// TestMigrateToSpecs ensures that the migration to the sub-specs format from
// the previous Function structure works
func TestMigrateToSpecs(t *testing.T) {

	root := "testdata/migrations/v0.34.0"
	expectedGit := Git{URL: "http://test-url", Revision: "test revision", ContextDir: "/test/context/dir"}
	expectedNamespace := "test-namespace"
	var expectedEnvs []Env
	var expectedVolumes []Volume

	f, err := NewFunction(root)
	if err != nil {
		t.Error(err)
		t.Fatal(f)
	}

	if f.Build.Git != expectedGit {
		t.Fatalf("migrated Function expected Git '%v', got '%v'", expectedGit, f.Build.Git)
	}

	if f.Deploy.Namespace != expectedNamespace {
		t.Fatalf("migrated Function expected Namespace '%v', got '%v'", expectedNamespace, f.Deploy.Namespace)
	}

	if len(f.Run.Envs) != len(expectedEnvs) {
		t.Fatalf("migrated Function expected Run Envs '%v', got '%v'", len(expectedEnvs), len(f.Run.Envs))
	}

	if len(f.Run.Volumes) != len(expectedVolumes) {
		t.Fatalf("migrated Function expected Run Volumes '%v', got '%v'", len(expectedEnvs), len(f.Run.Envs))
	}

}

// TestMigrateFromInvokeStructure tests that migration from f.Invocation.Format to
// f.Invoke works
func TestMigrateFromInvokeStructure(t *testing.T) {
	root0 := "testdata/migrations/v0.35.0"
	expectedInvoke := "" // empty because http is default and not written in yaml file

	f0, err := NewFunction(root0)
	if err != nil {
		t.Error(err)
		t.Fatal(f0)
	}
	if f0.Invoke != expectedInvoke {
		t.Fatalf("migrated Function expected Invoke '%v', got '%v'", expectedInvoke, f0.Invoke)
	}

	root1 := "testdata/migrations/v0.35.0-nondefault"
	expectedInvoke = "cloudevent"
	f1, err := NewFunction(root1)
	if err != nil {
		t.Error(err)
		t.Fatal(f1)
	}
	if f1.Invoke != expectedInvoke {
		t.Fatalf("migrated Function expected Invoke '%v', got '%v'", expectedInvoke, f0.Invoke)
	}
}

// TestUnknownFieldsWarning verifies that loading a func.yaml at the latest
// spec version with unknown fields prints a warning to stderr.
// Note we have to do this 'dynamically' because we need the latest spec,
// whatever it is.
func TestUnknownFieldsWarning(t *testing.T) {
	unknownFieldsOnce = sync.Once{}

	root := t.TempDir()
	if err := writeFunc(Function{
		SpecVersion: LastSpecVersion(),
		Name:        "test-func",
		Runtime:     "go",
	}, root); err != nil {
		t.Fatal(err)
	}
	funcYaml, _ := os.OpenFile(root+"/func.yaml", os.O_APPEND|os.O_WRONLY, 0644)
	if _, err := funcYaml.WriteString("junkField: bad\n"); err != nil {
		t.Fatal(err)
	}
	funcYaml.Close()

	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	_, err := NewFunction(root)
	if err != nil {
		t.Fatal(err)
	}

	w.Close()
	os.Stderr = old

	var buf [4096]byte
	n, _ := r.Read(buf[:])
	output := string(buf[:n])

	if !strings.Contains(output, "Warning") {
		t.Errorf("expected warning in stderr, got: %q", output)
	}
	if !strings.Contains(output, `unknown field "junkField"`) {
		t.Errorf("expected junkField in warning, got: %q", output)
	}
}

// TestUnknownFieldsNoWarningOnClean verifies that a valid func.yaml
// produces no warning.
func TestUnknownFieldsNoWarningOnClean(t *testing.T) {
	unknownFieldsOnce = sync.Once{}

	root := t.TempDir()
	f := Function{
		Runtime: "go",
		Root:    root,
	}
	f.SpecVersion = LastSpecVersion()
	f.Name = "clean-func"
	f.Created = f.Created.Add(1)
	if err := writeFunc(f, root); err != nil {
		t.Fatal(err)
	}

	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	_, err := NewFunction(root)
	if err != nil {
		t.Fatal(err)
	}

	w.Close()
	os.Stderr = old

	var buf [4096]byte
	n, _ := r.Read(buf[:])
	output := string(buf[:n])

	if strings.Contains(output, "Warning") {
		t.Errorf("expected no warning for clean func.yaml, got: %q", output)
	}
}

// TestUnknownFieldsNoWarningPreMigration verifies that old func.yaml files
// (not yet at the latest spec) do NOT trigger the unknown fields warning,
// since they may contain old keys that migration handles.
func TestUnknownFieldsNoWarningPreMigration(t *testing.T) {
	unknownFieldsOnce = sync.Once{}

	old := os.Stderr
	r, w, _ := os.Pipe()
	os.Stderr = w

	_, _ = NewFunction("testdata/migrations/v0.34.0")

	w.Close()
	os.Stderr = old

	var buf [4096]byte
	n, _ := r.Read(buf[:])
	output := string(buf[:n])

	if strings.Contains(output, "Warning") {
		t.Errorf("expected no warning for pre-migration func.yaml, got: %q", output)
	}
}

func writeFunc(f Function, root string) error {
	bb, err := yaml.Marshal(&f)
	if err != nil {
		return err
	}
	return os.WriteFile(root+"/func.yaml", bb, 0644)
}

func TestMigrateScaleToTopLevel(t *testing.T) {
	t.Run("flat fields move to top-level scale.kpa", func(t *testing.T) {
		root := t.TempDir()
		// Write an old-format func.yaml with flat KPA fields under deploy.options.scale
		funcYaml := `specVersion: "0.36.0"
name: testfn
runtime: go
deploy:
  options:
    scale:
      min: 1
      max: 10
      metric: concurrency
      target: 100.0
      utilization: 70.0
`
		if err := os.WriteFile(filepath.Join(root, FunctionFile), []byte(funcYaml), 0644); err != nil {
			t.Fatal(err)
		}

		f := Function{
			SpecVersion: "0.36.0",
			Root:        root,
		}
		migrated, err := migrateScaleToTopLevel(f, migration{version: "0.37.0"})
		if err != nil {
			t.Fatal(err)
		}

		if migrated.SpecVersion != "0.37.0" {
			t.Errorf("specVersion = %q, want 0.37.0", migrated.SpecVersion)
		}
		if migrated.Scale == nil {
			t.Fatal("expected top-level scale to be populated")
		}
		if migrated.Scale.Min == nil || *migrated.Scale.Min != 1 {
			t.Errorf("scale.min = %v, want 1", migrated.Scale.Min)
		}
		if migrated.Scale.Max == nil || *migrated.Scale.Max != 10 {
			t.Errorf("scale.max = %v, want 10", migrated.Scale.Max)
		}
		if migrated.Scale.KPA == nil {
			t.Fatal("expected scale.kpa to be populated from flat fields")
		}
		if *migrated.Scale.KPA.Metric != "concurrency" {
			t.Errorf("scale.kpa.metric = %q, want concurrency", *migrated.Scale.KPA.Metric)
		}
		if *migrated.Scale.KPA.Target != 100.0 {
			t.Errorf("scale.kpa.target = %f, want 100", *migrated.Scale.KPA.Target)
		}
		if *migrated.Scale.KPA.Utilization != 70.0 {
			t.Errorf("scale.kpa.utilization = %f, want 70", *migrated.Scale.KPA.Utilization)
		}
		if migrated.Deploy.Options.Scale != nil {
			t.Error("expected deploy.options.scale to be cleared")
		}
	})

	t.Run("no-op when no scale fields", func(t *testing.T) {
		root := t.TempDir()
		funcYaml := `specVersion: "0.36.0"
name: testfn
runtime: go
`
		if err := os.WriteFile(filepath.Join(root, FunctionFile), []byte(funcYaml), 0644); err != nil {
			t.Fatal(err)
		}

		f := Function{SpecVersion: "0.36.0", Root: root}
		migrated, err := migrateScaleToTopLevel(f, migration{version: "0.37.0"})
		if err != nil {
			t.Fatal(err)
		}
		if migrated.SpecVersion != "0.37.0" {
			t.Errorf("specVersion = %q, want 0.37.0", migrated.SpecVersion)
		}
		if migrated.Scale != nil {
			t.Errorf("expected nil scale, got %+v", migrated.Scale)
		}
	})

	t.Run("preserves kpa sub-key when already set", func(t *testing.T) {
		root := t.TempDir()
		funcYaml := `specVersion: "0.36.0"
name: testfn
runtime: go
deploy:
  options:
    scale:
      kpa:
        metric: rps
`
		if err := os.WriteFile(filepath.Join(root, FunctionFile), []byte(funcYaml), 0644); err != nil {
			t.Fatal(err)
		}

		f := Function{SpecVersion: "0.36.0", Root: root}
		migrated, err := migrateScaleToTopLevel(f, migration{version: "0.37.0"})
		if err != nil {
			t.Fatal(err)
		}
		if migrated.Scale == nil || migrated.Scale.KPA == nil {
			t.Fatal("expected scale.kpa to be preserved")
		}
		if *migrated.Scale.KPA.Metric != "rps" {
			t.Errorf("scale.kpa.metric = %q, want rps", *migrated.Scale.KPA.Metric)
		}
	})

	t.Run("keda deployer gets http trigger", func(t *testing.T) {
		root := t.TempDir()
		funcYaml := `specVersion: "0.36.0"
name: testfn
runtime: go
deployer: keda
`
		if err := os.WriteFile(filepath.Join(root, FunctionFile), []byte(funcYaml), 0644); err != nil {
			t.Fatal(err)
		}

		f := Function{SpecVersion: "0.36.0", Deployer: "keda", Root: root}
		migrated, err := migrateScaleToTopLevel(f, migration{version: "0.37.0"})
		if err != nil {
			t.Fatal(err)
		}
		if migrated.Scale == nil || migrated.Scale.KEDA == nil {
			t.Fatal("expected scale.keda to be populated")
		}
		triggers := migrated.Scale.KEDA.Triggers
		if len(triggers) != 1 || triggers[0].Type != "http" {
			t.Errorf("expected [{http}], got %v", triggers)
		}
	})

	t.Run("keda deployer with existing triggers unchanged", func(t *testing.T) {
		root := t.TempDir()
		funcYaml := `specVersion: "0.36.0"
name: testfn
runtime: go
deployer: keda
deploy:
  options:
    scale:
      keda:
        triggers:
          - type: kafka
`
		if err := os.WriteFile(filepath.Join(root, FunctionFile), []byte(funcYaml), 0644); err != nil {
			t.Fatal(err)
		}

		f := Function{
			SpecVersion: "0.36.0",
			Deployer:    "keda",
			Root:        root,
		}
		migrated, err := migrateScaleToTopLevel(f, migration{version: "0.37.0"})
		if err != nil {
			t.Fatal(err)
		}
		triggers := migrated.Scale.KEDA.Triggers
		if len(triggers) != 1 || triggers[0].Type != "kafka" {
			t.Errorf("expected [{kafka}], got %v", triggers)
		}
	})

	t.Run("non-keda deployer no triggers added", func(t *testing.T) {
		root := t.TempDir()
		funcYaml := `specVersion: "0.36.0"
name: testfn
runtime: go
deployer: raw
`
		if err := os.WriteFile(filepath.Join(root, FunctionFile), []byte(funcYaml), 0644); err != nil {
			t.Fatal(err)
		}

		f := Function{SpecVersion: "0.36.0", Deployer: "raw", Root: root}
		migrated, err := migrateScaleToTopLevel(f, migration{version: "0.37.0"})
		if err != nil {
			t.Fatal(err)
		}
		if migrated.Scale != nil {
			t.Errorf("expected no scale for raw deployer, got %+v", migrated.Scale)
		}
	})
}

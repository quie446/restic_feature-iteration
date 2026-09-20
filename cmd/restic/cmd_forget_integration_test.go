package main

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"

	"github.com/restic/restic/internal/data"
	"github.com/restic/restic/internal/global"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func testRunForgetMayFail(t testing.TB, gopts global.Options, opts ForgetOptions, args ...string) error {
	pruneOpts := PruneOptions{
		MaxUnused: "5%",
	}
	return withTermStatus(t, gopts, func(ctx context.Context, gopts global.Options) error {
		return runForget(context.TODO(), opts, pruneOpts, gopts, gopts.Term, args)
	})
}

func testRunForget(t testing.TB, gopts global.Options, opts ForgetOptions, args ...string) {
	rtest.OK(t, testRunForgetMayFail(t, gopts, opts, args...))
}

func TestRunForgetSafetyNet(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)

	opts := BackupOptions{
		Host: "example",
	}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testListSnapshots(t, env.gopts, 2)

	// --keep-tags invalid
	err := testRunForgetMayFail(t, env.gopts, ForgetOptions{
		KeepTags: data.TagLists{data.TagList{"invalid"}},
		GroupBy:  data.SnapshotGroupByOptions{Host: true, Path: true},
	})
	rtest.Assert(t, strings.Contains(err.Error(), `refusing to delete last snapshot of snapshot group "host example, path`), "wrong error message got %v", err)

	// disallow `forget --unsafe-allow-remove-all`
	err = testRunForgetMayFail(t, env.gopts, ForgetOptions{
		UnsafeAllowRemoveAll: true,
	})
	rtest.Assert(t, strings.Contains(err.Error(), `--unsafe-allow-remove-all is not allowed unless a snapshot filter option is specified`), "wrong error message got %v", err)

	// disallow `forget` without options
	err = testRunForgetMayFail(t, env.gopts, ForgetOptions{})
	rtest.Assert(t, strings.Contains(err.Error(), `no policy was specified, no snapshots will be removed`), "wrong error message got %v", err)

	// `forget --host example --unsafe-allow-remove-all` should work
	testRunForget(t, env.gopts, ForgetOptions{
		UnsafeAllowRemoveAll: true,
		GroupBy:              data.SnapshotGroupByOptions{Host: true, Path: true},
		SnapshotFilter: data.SnapshotFilter{
			Hosts: []string{opts.Host},
		},
	})
	testListSnapshots(t, env.gopts, 0)
}

// TestRunForgetDryRunConsistency pins that a forget dry-run reports exactly
// the snapshots that a real forget run with the same policy removes, and
// that the dry-run itself does not remove anything.
func TestRunForgetDryRunConsistency(t *testing.T) {
	env, cleanup := withTestEnvironment(t)
	defer cleanup()

	testSetupBackupData(t, env)

	opts := BackupOptions{
		Host: "example",
	}
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "2")}, opts, env.gopts)
	testRunBackup(t, "", []string{filepath.Join(env.testdata, "0", "0", "9", "3")}, opts, env.gopts)
	snapshotsBefore := testListSnapshots(t, env.gopts, 3)

	forgetOpts := func(dryRun bool) ForgetOptions {
		return ForgetOptions{
			Last:   1,
			DryRun: dryRun,
		}
	}

	// the dry-run reports the snapshots it would remove
	buf, err := withCaptureStdout(t, env.gopts, func(ctx context.Context, gopts global.Options) error {
		gopts.JSON = true
		pruneOpts := PruneOptions{
			MaxUnused: "5%",
		}
		return runForget(ctx, forgetOpts(true), pruneOpts, gopts, gopts.Term, nil)
	})
	rtest.OK(t, err)

	var forgets []*ForgetGroup
	rtest.OK(t, json.Unmarshal(buf.Bytes(), &forgets))
	rtest.Equals(t, 1, len(forgets))
	rtest.Equals(t, 1, len(forgets[0].Keep))
	rtest.Equals(t, 2, len(forgets[0].Remove))
	reported := restic.NewIDSet()
	for _, sn := range forgets[0].Remove {
		reported.Insert(*sn.ID)
	}

	// the dry-run must not remove anything
	testListSnapshots(t, env.gopts, 3)

	// the real run must remove exactly the snapshots reported by the dry-run
	testRunForget(t, env.gopts, forgetOpts(false))

	remaining := restic.NewIDSet(testListSnapshots(t, env.gopts, 1)...)
	removed := restic.NewIDSet(snapshotsBefore...).Sub(remaining)
	rtest.Assert(t, removed.Equals(reported), "forget removed snapshots %v, but dry-run reported %v", removed, reported)
}

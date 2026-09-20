package repository_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/repository"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

func testPrune(t *testing.T, opts repository.PruneOptions, errOnUnused bool) {
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	repo, _, be := repository.TestRepositoryWithVersion(t, 0)
	createRandomBlobs(t, random, repo, 4, 0.5, true)
	createRandomBlobs(t, random, repo, 5, 0.5, true)
	keep, _ := selectBlobs(t, random, repo, 0.5)

	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		// duplicate a few blobs to exercise those code paths
		for blob := range keep {
			buf, err := repo.LoadBlob(ctx, blob, nil)
			rtest.OK(t, err)
			_, _, _, err = uploader.SaveBlob(ctx, blob.Type, buf, blob.ID, true)
			rtest.OK(t, err)
		}
		return nil
	}))

	plan, err := repository.PlanPrune(context.TODO(), opts, repo, func(ctx context.Context, repo restic.Repository, usedBlobs restic.FindBlobSet) (restic.IDSet, error) {
		for blob := range keep {
			usedBlobs.Insert(blob)
		}
		return restic.NewIDSet(), nil
	}, restic.NewNoopPrinter())
	rtest.OK(t, err)

	rtest.OK(t, plan.Execute(context.TODO(), restic.NewNoopPrinter()))

	repo = repository.TestOpenBackend(t, be)
	repository.TestCheckRepo(t, repo)

	if errOnUnused {
		existing := listBlobs(repo)
		rtest.Assert(t, existing.Equals(keep), "unexpected blobs, wanted %v got %v", keep, existing)
	}
}

func TestPrune(t *testing.T) {
	for _, test := range []struct {
		name        string
		opts        repository.PruneOptions
		errOnUnused bool
	}{
		{
			name: "0",
			opts: repository.PruneOptions{
				MaxRepackBytes: math.MaxUint64,
				MaxUnusedBytes: func(used uint64) (unused uint64) { return 0 },
			},
			errOnUnused: true,
		},
		{
			name: "50",
			opts: repository.PruneOptions{
				MaxRepackBytes: math.MaxUint64,
				MaxUnusedBytes: func(used uint64) (unused uint64) { return used / 2 },
			},
		},
		{
			name: "unlimited",
			opts: repository.PruneOptions{
				MaxRepackBytes: math.MaxUint64,
				MaxUnusedBytes: func(used uint64) (unused uint64) { return math.MaxUint64 },
			},
		},
		{
			name: "cachableonly",
			opts: repository.PruneOptions{
				MaxRepackBytes:      math.MaxUint64,
				MaxUnusedBytes:      func(used uint64) (unused uint64) { return used / 20 },
				RepackCacheableOnly: true,
			},
		},
		{
			name: "small",
			opts: repository.PruneOptions{
				MaxRepackBytes: math.MaxUint64,
				MaxUnusedBytes: func(used uint64) (unused uint64) { return math.MaxUint64 },
			},
			errOnUnused: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testPrune(t, test.opts, test.errOnUnused)
		})
		t.Run(test.name+"-recovery", func(t *testing.T) {
			opts := test.opts
			opts.UnsafeRecovery = true
			// unsafeNoSpaceRecovery does not repack partially used pack files
			testPrune(t, opts, false)
		})
	}
}

/*
1.) create repository with packsize of 2M.
2.) create enough data for 11 packfiles (31 packs)
3.) run a repository.PlanPrune(...) with a packsize of 16M (current default).
4.) run plan.Execute(...), extract plan.Stats() and check.
5.) Check that all blobs are contained in the new packfiles.
6.) The result should be less packfiles than before
*/
func TestPruneSmall(t *testing.T) {
	t.Parallel()
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	be := repository.TestBackend(t)
	repo, _ := repository.TestRepositoryWithBackend(t, be, 0, repository.Options{PackSize: repository.MinPackSize, Compression: repository.CompressionOff})

	const blobSize = 1000 * 1000
	const numBlobsCreated = 55

	keep := restic.NewBlobSet()
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		// we need a minimum of 11 packfiles, each packfile will be about 5 Mb long
		for range numBlobsCreated {
			buf := make([]byte, blobSize)
			random.Read(buf)

			id, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
			rtest.OK(t, err)
			keep.Insert(restic.BlobHandle{Type: restic.DataBlob, ID: id})
		}
		return nil
	}))

	// gather number of packfiles
	repoPacks, err := pack.Size(context.TODO(), repo, false)
	rtest.OK(t, err)
	lenPackfilesBefore := len(repoPacks)
	rtest.OK(t, repo.Close())

	// and reopen repository with default packsize
	repo = repository.TestOpenBackend(t, be)
	rtest.OK(t, repo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))

	opts := repository.PruneOptions{
		MaxRepackBytes: math.MaxUint64,
		MaxUnusedBytes: func(used uint64) (unused uint64) { return blobSize / 4 },
		SmallPackBytes: 5 * 1024 * 1024,
	}
	plan, err := repository.PlanPrune(context.TODO(), opts, repo, func(ctx context.Context, repo restic.Repository, usedBlobs restic.FindBlobSet) (restic.IDSet, error) {
		for blob := range keep {
			usedBlobs.Insert(blob)
		}
		return restic.NewIDSet(), nil
	}, restic.NewNoopPrinter())
	rtest.OK(t, err)
	rtest.OK(t, plan.Execute(context.TODO(), restic.NewNoopPrinter()))

	stats := plan.Stats()
	rtest.Equals(t, stats.Size.Used/blobSize, uint64(numBlobsCreated), fmt.Sprintf("total size of blobs should be %d but is %d",
		numBlobsCreated, stats.Size.Used/blobSize))
	rtest.Equals(t, stats.Blobs.Used, stats.Blobs.Repack, "the number of blobs should be identical after a repack")

	// repopen repository
	repo = repository.TestOpenBackend(t, be)
	repository.TestCheckRepo(t, repo)

	// load all blobs
	for blob := range keep {
		_, err := repo.LoadBlob(context.TODO(), blob, nil)
		rtest.OK(t, err)
	}

	repoPacks, err = pack.Size(context.TODO(), repo, false)
	rtest.OK(t, err)
	lenPackfilesAfter := len(repoPacks)

	rtest.Equals(t, lenPackfilesBefore > lenPackfilesAfter, true,
		fmt.Sprintf("the number packfiles before %d and after repack %d", lenPackfilesBefore, lenPackfilesAfter))
}

// listPacksInBackend returns all pack files currently stored in the backend.
func listPacksInBackend(t *testing.T, be backend.Backend) restic.IDSet {
	t.Helper()
	packs := restic.NewIDSet()
	rtest.OK(t, be.List(context.TODO(), backend.PackFile, func(handle backend.FileInfo) error {
		id, err := restic.ParseID(handle.Name)
		rtest.OK(t, err)
		packs.Insert(id)
		return nil
	}))
	return packs
}

// planPruneWithBlobs builds a prune plan for a repository where exactly the
// blobs in keep are still referenced.
func planPruneWithBlobs(t *testing.T, repo *repository.Repository, keep restic.BlobSet, dryRun bool) *repository.PrunePlan {
	t.Helper()
	opts := repository.PruneOptions{
		DryRun:         dryRun,
		MaxRepackBytes: math.MaxUint64,
		MaxUnusedBytes: func(_ uint64) uint64 { return 0 },
	}
	plan, err := repository.PlanPrune(context.TODO(), opts, repo, func(ctx context.Context, repo restic.Repository, usedBlobs restic.FindBlobSet) (restic.IDSet, error) {
		for blob := range keep {
			usedBlobs.Insert(blob)
		}
		return restic.NewIDSet(), nil
	}, restic.NewNoopPrinter())
	rtest.OK(t, err)
	return plan
}

// setupConcurrentPruneRepo creates a repository with referenced and
// unreferenced blobs and returns two repository handles on the same backend
// (one for planning prune, one simulating a concurrent backup), the set of
// referenced blobs and the backend.
func setupConcurrentPruneRepo(t *testing.T) (*repository.Repository, *repository.Repository, restic.BlobSet, backend.Backend) {
	t.Helper()
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	repo, _, be := repository.TestRepositoryWithVersion(t, 0)
	createRandomBlobs(t, random, repo, 4, 0.5, true)
	keep, _ := selectBlobs(t, random, repo, 0.75)
	rtest.Assert(t, len(keep) > 0, "test setup requires referenced blobs")

	other := repository.TestOpenBackend(t, be)
	rtest.OK(t, other.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))

	return repo, other, keep, be
}

// TestPrunePlanRevalidatesNewIndex pins down the reference/index view used by
// prune: after the plan was computed, a backup finishing on the same
// repository adds new packs and index files. Executing the stale plan must
// fail instead of removing the freshly written data.
func TestPrunePlanRevalidatesNewIndex(t *testing.T) {
	repo, other, keep, be := setupConcurrentPruneRepo(t)

	plan := planPruneWithBlobs(t, repo, keep, false)

	// simulate a concurrent backup that finishes after the prune plan was
	// created: it writes new blobs/packs and publishes a new index file
	concurrentBlobs := restic.NewBlobSet()
	rtest.OK(t, other.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		for range 3 {
			buf := make([]byte, 1024*1024)
			rand.New(rand.NewSource(time.Now().UnixNano())).Read(buf)
			id, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
			rtest.OK(t, err)
			concurrentBlobs.Insert(restic.BlobHandle{Type: restic.DataBlob, ID: id})
		}
		return nil
	}))

	err := plan.Execute(context.TODO(), restic.NewNoopPrinter())
	rtest.Assert(t, errors.Is(err, repository.ErrRepositoryChanged), "expected ErrRepositoryChanged, got %v", err)

	// none of the concurrent backup's data must have been removed
	checkRepo := repository.TestOpenBackend(t, be)
	rtest.OK(t, checkRepo.LoadIndex(context.TODO(), restic.NoopTerminalCounterFactory))
	for blob := range concurrentBlobs {
		buf, err := checkRepo.LoadBlob(context.TODO(), blob, nil)
		rtest.OK(t, err)
		rtest.Assert(t, len(buf) > 0, "blob %v was removed despite being written by a concurrent backup", blob)
	}

	// a fresh plan based on the updated index view must succeed
	freshPlan := planPruneWithBlobs(t, checkRepo, keep, false)
	rtest.OK(t, freshPlan.Execute(context.TODO(), restic.NewNoopPrinter()))

	verified := repository.TestOpenBackend(t, be)
	repository.TestCheckRepo(t, verified)
}

// TestPrunePlanRevalidatesUnindexedPacks verifies that a pack uploaded after
// the plan was computed (but not yet covered by an index) is not deleted as an
// unreferenced pack by the stale plan's execution.
func TestPrunePlanRevalidatesUnindexedPacks(t *testing.T) {
	repo, other, keep, be := setupConcurrentPruneRepo(t)

	plan := planPruneWithBlobs(t, repo, keep, false)

	before := listPacksInBackend(t, be)

	// backup in progress: a new pack file exists, no index references it yet
	rtest.OK(t, other.SavePackWithoutIndex(context.TODO(), make([]byte, 1024*1024)))

	err := plan.Execute(context.TODO(), restic.NewNoopPrinter())
	rtest.Assert(t, errors.Is(err, repository.ErrRepositoryChanged), "expected ErrRepositoryChanged, got %v", err)

	// nothing must have been deleted and the in-flight pack must remain
	after := listPacksInBackend(t, be)
	rtest.Assert(t, after.Equals(before) || len(after) == len(before)+1,
		"unexpected pack set change: before %d after %d", len(before), len(after))
	rtest.Assert(t, len(after) > len(before), "in-flight pack was removed as unreferenced")
}

// TestPruneDryRunAndRealPlanAgree verifies the decision-set parity contract:
// for an unchanged repository, the packs selected by a dry-run plan and a
// real plan are identical and executing the real plan removes exactly the
// union reported by the dry-run.
func TestPruneDryRunAndRealPlanAgree(t *testing.T) {
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	repo, _, be := repository.TestRepositoryWithVersion(t, 0)
	createRandomBlobs(t, random, repo, 4, 0.5, true)
	keep, _ := selectBlobs(t, random, repo, 0.5)

	dryPlan := planPruneWithBlobs(t, repo, keep, true)
	realPlan := planPruneWithBlobs(t, repo, keep, false)

	rtest.Assert(t, dryPlan.RemovePacksFirst().Equals(realPlan.RemovePacksFirst()),
		"unreferenced pack sets differ: dry-run %v vs real %v", dryPlan.RemovePacksFirst(), realPlan.RemovePacksFirst())
	rtest.Assert(t, dryPlan.RemovePacks().Equals(realPlan.RemovePacks()),
		"unused pack sets differ: dry-run %v vs real %v", dryPlan.RemovePacks(), realPlan.RemovePacks())
	rtest.Assert(t, dryPlan.RepackPacks().Equals(realPlan.RepackPacks()),
		"repack pack sets differ: dry-run %v vs real %v", dryPlan.RepackPacks(), realPlan.RepackPacks())

	// executing the dry-run plan must not touch the repository
	rtest.OK(t, dryPlan.Execute(context.TODO(), restic.NewNoopPrinter()))

	// and the real plan must delete exactly the packs the dry-run reported
	packsBefore := listPacksInBackend(t, be)
	rtest.OK(t, realPlan.Execute(context.TODO(), restic.NewNoopPrinter()))
	packsAfter := listPacksInBackend(t, be)

	expectedRemoved := restic.NewIDSet()
	expectedRemoved.Merge(dryPlan.RemovePacksFirst())
	expectedRemoved.Merge(dryPlan.RemovePacks())
	expectedRemoved.Merge(dryPlan.RepackPacks())

	// repacking can create new packs, so compare only the removed subset
	gotRemoved := packsBefore.Sub(packsAfter)
	rtest.Assert(t, gotRemoved.Equals(expectedRemoved),
		"actual removed packs %v do not match dry-run set %v", gotRemoved, expectedRemoved)

	verified := repository.TestOpenBackend(t, be)
	repository.TestCheckRepo(t, verified)
}

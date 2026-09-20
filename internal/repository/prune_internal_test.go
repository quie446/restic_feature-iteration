package repository

import (
	"context"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/restic/restic/internal/backend"
	"github.com/restic/restic/internal/repository/pack"
	"github.com/restic/restic/internal/restic"
	rtest "github.com/restic/restic/internal/test"
)

// saveTestBlobs saves count random blobs within a single uploader session.
// Together with packerCount = 1 this assembles the blobs into a single pack.
func saveTestBlobs(t *testing.T, random *rand.Rand, repo *Repository, count int) restic.BlobSet {
	t.Helper()
	blobs := restic.NewBlobSet()
	rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
		for range count {
			buf := make([]byte, 100*1024)
			random.Read(buf)
			id, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, buf, restic.ID{}, false)
			rtest.OK(t, err)
			blobs.Insert(restic.BlobHandle{Type: restic.DataBlob, ID: id})
		}
		return nil
	}))
	return blobs
}

func listTestBackendPacks(t *testing.T, be backend.Backend) restic.IDSet {
	t.Helper()
	ids := restic.NewIDSet()
	rtest.OK(t, be.List(context.TODO(), backend.PackFile, func(fi backend.FileInfo) error {
		id, err := restic.ParseID(fi.Name)
		rtest.OK(t, err)
		ids.Insert(id)
		return nil
	}))
	return ids
}

func listTestBackendIndexFiles(t *testing.T, be backend.Backend) restic.IDSet {
	t.Helper()
	ids := restic.NewIDSet()
	rtest.OK(t, be.List(context.TODO(), backend.IndexFile, func(fi backend.FileInfo) error {
		id, err := restic.ParseID(fi.Name)
		rtest.OK(t, err)
		ids.Insert(id)
		return nil
	}))
	return ids
}

// saveUnreferencedTestBlobs saves blobs whose packs are not referenced by
// any index file in the backend, simulating packs uploaded by a backup that
// has not saved its index (and snapshot) yet.
func saveUnreferencedTestBlobs(t *testing.T, random *rand.Rand, repo *Repository, be backend.Backend, count int) restic.BlobSet {
	t.Helper()
	indexIDs := listTestBackendIndexFiles(t, be)
	blobs := saveTestBlobs(t, random, repo, count)
	// remove the index file that was written for the new packs
	for id := range listTestBackendIndexFiles(t, be).Sub(indexIDs) {
		rtest.OK(t, be.Remove(context.TODO(), backend.Handle{Type: backend.IndexFile, Name: id.String()}))
	}
	return blobs
}

func blobSetToFindBlobSet(set restic.BlobSet) func(ctx context.Context, repo restic.Repository, usedBlobs restic.FindBlobSet) error {
	return func(_ context.Context, _ restic.Repository, usedBlobs restic.FindBlobSet) error {
		for bh := range set {
			usedBlobs.Insert(bh)
		}
		return nil
	}
}

// TestPruneRecheckSparesConcurrentBackup ensures that prune does not delete
// packs that became referenced by a backup which finished while prune was
// planning and running, while still collecting packs that remain
// unreferenced. This pins the reference-count/index view used by prune: data
// referenced by a snapshot must never be considered dead, no matter whether
// the snapshot appeared before or after the prune plan was made.
func TestPruneRecheckSparesConcurrentBackup(t *testing.T) {
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	ctx := context.TODO()
	repo, _, be := TestRepositoryWithVersion(t, 0)
	// assemble blobs saved within one uploader session into a single pack
	repo.packerCount = 1

	// referenced by the snapshots existing at planning time
	keep := saveTestBlobs(t, random, repo, 3)
	// unreferenced garbage that must be collected
	garbage := saveTestBlobs(t, random, repo, 3)
	// mixed pack: one blob stays referenced, the other is garbage at
	// planning time but becomes referenced again while prune is running
	mixed := saveTestBlobs(t, random, repo, 2)
	var mixedKeep, mixedLateRef restic.BlobHandle
	for bh := range mixed {
		if mixedKeep == (restic.BlobHandle{}) {
			mixedKeep = bh
		} else {
			mixedLateRef = bh
		}
	}

	// packs uploaded by a concurrent backup whose index and snapshot only
	// arrive after prune has made its plan
	late := saveUnreferencedTestBlobs(t, random, repo, be, 2)
	latePackIDs := restic.NewIDSet()
	for bh := range late {
		for _, pb := range repo.idx.Lookup(bh) {
			latePackIDs.Insert(pb.Pack)
		}
	}
	latePackBlobs := make(map[restic.ID]pack.Blobs)
	for packBlobs := range repo.listPacksFromIndex(ctx, latePackIDs) {
		latePackBlobs[packBlobs.PackID] = packBlobs.Blobs
	}
	rtest.Assert(t, len(latePackBlobs) > 0, "late blobs must be stored in packs")

	// forget about the late packs: from the index perspective used for
	// planning they are unreferenced packs in the repository
	repo.clearIndex()
	rtest.OK(t, repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory))

	// used blobs as visible at planning time
	used := restic.NewBlobSet()
	for bh := range keep {
		used.Insert(bh)
	}
	used.Insert(mixedKeep)

	opts := PruneOptions{
		MaxRepackBytes: math.MaxUint64,
		MaxUnusedBytes: func(used uint64) (unused uint64) { return 0 },
	}
	plan, err := PlanPrune(ctx, opts, repo, blobSetToFindBlobSet(used), restic.NewNoopPrinter())
	rtest.OK(t, err)

	// sanity check the plan: late packs are unreferenced, garbage packs are
	// removed and the mixed pack is repacked
	rtest.Assert(t, plan.removePacksFirst.Equals(latePackIDs), "plan must remove late packs first, got %v", plan.removePacksFirst)
	rtest.Assert(t, len(plan.removePacks) > 0, "plan must remove garbage packs")
	rtest.Equals(t, 1, len(plan.repackPacks))
	var mixedPackID restic.ID
	for id := range plan.repackPacks {
		mixedPackID = id
	}
	garbagePacks := plan.removePacks

	// the concurrent backup finishes while prune is running: its index
	// arrives in the repository and its snapshot references the late blobs
	// and (via deduplication) the second blob of the mixed pack. Use a
	// second repository connection to faithfully simulate a separate
	// process whose in-memory index is not shared with the prune run.
	backupRepo := TestOpenBackend(t, be)
	for id, blobs := range latePackBlobs {
		rtest.OK(t, backupRepo.idx.StorePack(ctx, id, blobs, &internalRepository{backupRepo}))
	}
	rtest.OK(t, backupRepo.idx.Flush(ctx, &internalRepository{backupRepo}))
	for bh := range late {
		used.Insert(bh)
	}
	used.Insert(mixedLateRef)

	rtest.OK(t, plan.Execute(ctx, restic.NewNoopPrinter()))

	// the garbage packs are gone, everything referenced by the concurrent
	// backup survived
	remaining := listTestBackendPacks(t, be)
	for id := range garbagePacks {
		rtest.Assert(t, !remaining.Has(id), "garbage pack %v must be deleted", id)
	}
	for id := range latePackIDs {
		rtest.Assert(t, remaining.Has(id), "pack %v of the concurrent backup must survive", id)
	}
	rtest.Assert(t, remaining.Has(mixedPackID), "mixed pack %v must survive", mixedPackID)

	// the repository is consistent and all referenced blobs are readable
	repo = TestOpenBackend(t, be)
	TestCheckRepo(t, repo)
	rtest.OK(t, repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory))
	for bh := range used {
		_, err := repo.LoadBlob(ctx, bh, nil)
		rtest.OK(t, err)
	}
	for bh := range garbage {
		rtest.Equals(t, 0, len(repo.idx.Lookup(bh)))
	}
}

// TestPruneDryRunExecutionConsistency pins that a prune dry-run and a real
// prune run starting from the same repository state select exactly the same
// packs for repacking and deletion.
func TestPruneDryRunExecutionConsistency(t *testing.T) {
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	ctx := context.TODO()
	repo, _, be := TestRepositoryWithVersion(t, 0)
	repo.packerCount = 1

	keep := saveTestBlobs(t, random, repo, 3)
	garbage := saveTestBlobs(t, random, repo, 3)
	mixed := saveTestBlobs(t, random, repo, 2)
	var mixedKeep restic.BlobHandle
	for bh := range mixed {
		mixedKeep = bh
		break
	}

	// packs that are not referenced by the index at all
	saveUnreferencedTestBlobs(t, random, repo, be, 2)
	repo.clearIndex()
	rtest.OK(t, repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory))

	used := restic.NewBlobSet()
	for bh := range keep {
		used.Insert(bh)
	}
	used.Insert(mixedKeep)

	newOpts := func(dryRun bool) PruneOptions {
		return PruneOptions{
			DryRun:         dryRun,
			MaxRepackBytes: math.MaxUint64,
			MaxUnusedBytes: func(used uint64) (unused uint64) { return 0 },
		}
	}

	packsBefore := listTestBackendPacks(t, be)

	// a dry-run must not modify the repository
	dryPlan, err := PlanPrune(ctx, newOpts(true), repo, blobSetToFindBlobSet(used), restic.NewNoopPrinter())
	rtest.OK(t, err)
	rtest.OK(t, dryPlan.Execute(ctx, restic.NewNoopPrinter()))
	rtest.Assert(t, listTestBackendPacks(t, be).Equals(packsBefore), "dry-run must not delete packs")

	// a real run starting from the same repository state must select the
	// exact same packs
	rtest.Assert(t, len(dryPlan.removePacksFirst) > 0, "test must exercise unreferenced packs")
	rtest.Assert(t, len(dryPlan.removePacks) > 0, "test must exercise unused packs")
	rtest.Assert(t, len(dryPlan.repackPacks) > 0, "test must exercise repacked packs")

	realPlan, err := PlanPrune(ctx, newOpts(false), repo, blobSetToFindBlobSet(used), restic.NewNoopPrinter())
	rtest.OK(t, err)
	rtest.Assert(t, realPlan.removePacksFirst.Equals(dryPlan.removePacksFirst), "unreferenced packs differ: dry-run %v, real %v", dryPlan.removePacksFirst, realPlan.removePacksFirst)
	rtest.Assert(t, realPlan.repackPacks.Equals(dryPlan.repackPacks), "repacked packs differ: dry-run %v, real %v", dryPlan.repackPacks, realPlan.repackPacks)
	rtest.Assert(t, realPlan.removePacks.Equals(dryPlan.removePacks), "removed packs differ: dry-run %v, real %v", dryPlan.removePacks, realPlan.removePacks)

	rtest.OK(t, realPlan.Execute(ctx, restic.NewNoopPrinter()))

	// the packs deleted by the real run must be exactly the packs selected
	// by the dry-run
	deleted := packsBefore.Sub(listTestBackendPacks(t, be))
	want := restic.NewIDSet()
	want.Merge(dryPlan.removePacksFirst)
	want.Merge(dryPlan.repackPacks)
	want.Merge(dryPlan.removePacks)
	rtest.Assert(t, deleted.Equals(want), "deleted packs %v, want %v", deleted, want)

	repo = TestOpenBackend(t, be)
	TestCheckRepo(t, repo)

	// the garbage blobs are gone, the used blobs are still referenced
	rtest.OK(t, repo.LoadIndex(ctx, restic.NoopTerminalCounterFactory))
	for bh := range garbage {
		rtest.Equals(t, 0, len(repo.idx.Lookup(bh)))
	}
	for bh := range used {
		rtest.Assert(t, len(repo.idx.Lookup(bh)) > 0, "used blob %v must still be indexed", bh)
	}
}

// TestPruneMaxUnusedDuplicate checks that MaxUnused correctly accounts for duplicates.
//
// Create a repository containing blobs a to d that are stored in packs as follows:
// - a, d
// - b, d
// - c, d
// All blobs should be kept during prune, but the duplicates should be gone afterwards.
// The special construction ensures that each pack contains a used, non-duplicate blob.
// This ensures that special cases that delete completely duplicate packs files do not
// apply.
func TestPruneMaxUnusedDuplicate(t *testing.T) {
	seed := time.Now().UnixNano()
	random := rand.New(rand.NewSource(seed))
	t.Logf("rand initialized with seed %d", seed)

	repo, _, _ := TestRepositoryWithVersion(t, 0)
	// ensure blobs are assembled into packs as expected
	repo.packerCount = 1
	// large blobs to prevent repacking due to too small packsize
	const blobSize = 1024 * 1024

	bufs := [][]byte{}
	for range 4 {
		// use uniform length for simpler control via MaxUnusedBytes
		buf := make([]byte, blobSize)
		random.Read(buf)
		bufs = append(bufs, buf)
	}
	keep := restic.NewBlobSet()

	for _, blobs := range [][][]byte{
		{bufs[0], bufs[3]},
		{bufs[1], bufs[3]},
		{bufs[2], bufs[3]},
	} {
		rtest.OK(t, repo.WithBlobUploader(context.TODO(), func(ctx context.Context, uploader restic.BlobSaverWithAsync) error {
			for _, blob := range blobs {
				id, _, _, err := uploader.SaveBlob(ctx, restic.DataBlob, blob, restic.ID{}, true)
				keep.Insert(restic.BlobHandle{Type: restic.DataBlob, ID: id})
				rtest.OK(t, err)
			}
			return nil
		}))
	}

	opts := PruneOptions{
		MaxRepackBytes: math.MaxUint64,
		// non-zero number of unused bytes, that is nevertheless smaller than a single blob
		// setting this to zero would bypass the unused/duplicate size accounting that should
		// be tested here
		MaxUnusedBytes: func(used uint64) (unused uint64) { return blobSize / 2 },
	}

	plan, err := PlanPrune(context.TODO(), opts, repo, func(ctx context.Context, repo restic.Repository, usedBlobs restic.FindBlobSet) error {
		for blob := range keep {
			usedBlobs.Insert(blob)
		}
		return nil
	}, restic.NewNoopPrinter())
	rtest.OK(t, err)

	rtest.OK(t, plan.Execute(context.TODO(), restic.NewNoopPrinter()))

	rsize := plan.Stats().Size
	remainingUnusedSize := rsize.Duplicate + rsize.Unused - rsize.Remove - rsize.Repackrm
	maxUnusedSize := opts.MaxUnusedBytes(rsize.Used)
	rtest.Assert(t, remainingUnusedSize <= maxUnusedSize, "too much unused data remains got %v, expected less than %v", remainingUnusedSize, maxUnusedSize)

	// divide by blobSize to ignore pack file overhead
	rtest.Equals(t, rsize.Used/blobSize, uint64(4))
	rtest.Equals(t, rsize.Duplicate/blobSize, uint64(2))
	rtest.Equals(t, rsize.Unused, uint64(0))
	rtest.Equals(t, rsize.Remove, uint64(0))
	rtest.Equals(t, rsize.Repack/blobSize, uint64(4))
	rtest.Equals(t, rsize.Repackrm/blobSize, uint64(2))
	rtest.Equals(t, rsize.Unref, uint64(0))
	rtest.Equals(t, rsize.Uncompressed, uint64(0))
}

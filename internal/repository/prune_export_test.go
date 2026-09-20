package repository

import (
	"context"

	"github.com/restic/restic/internal/restic"

	"golang.org/x/sync/errgroup"
)

// SavePackWithoutIndex uploads a single data blob as a pack file without
// publishing an index file. It models the state of a backup that is still
// running and is only visible to tests in the repository_test package.
func (r *Repository) SavePackWithoutIndex(ctx context.Context, blob []byte) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg, wgCtx := errgroup.WithContext(ctx)
	r.mainWg = wg
	r.startPackUploader(wgCtx, wg)

	_, _, _, err := r.saveBlob(wgCtx, restic.DataBlob, blob, restic.ID{}, false)
	if err != nil {
		return err
	}

	if err := r.dataPM.Flush(wgCtx); err != nil {
		return err
	}
	r.uploader.TriggerShutdown()
	err = wg.Wait()
	r.mainWg = nil
	return err
}

package merkledb

import (
	"bytes"
	"context"
	"log"
	"runtime"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

func getBasicDB_disk(dir string) (*merkleDB, error) {
	temp, _ := newDatabase_disk(
		context.Background(),
		dir,
		newDefaultConfig(),
		&mockMetrics{},
	)
	log.Printf("type of merkle db %T", temp.disk)
	return newDatabase_disk(
		context.Background(),
		dir,
		newDefaultConfig(),
		&mockMetrics{},
	)
}

// New returns a new merkle database.
func New_disk(ctx context.Context, dir string, config Config) (MerkleDB, error) {
	metrics, err := newMetrics("merkledb", config.Reg)
	if err != nil {
		return nil, err
	}
	return newDatabase_disk(ctx, dir, config, metrics)
}

func newDatabase_disk(
	ctx context.Context,
	dir string,
	config Config,
	metrics metrics,
) (*merkleDB, error) {
	if err := config.BranchFactor.Valid(); err != nil {
		return nil, err
	}

	hasher := config.Hasher
	if hasher == nil {
		hasher = DefaultHasher
	}

	rootGenConcurrency := runtime.NumCPU()
	if config.RootGenConcurrency != 0 {
		rootGenConcurrency = int(config.RootGenConcurrency)
	}

	// disk := newDBDisk(db, hasher, config, metrics)
	//disk, err := newRawDisk(dir, "merkle.db", hasher)
	disk, err := newRawDisk(dir, "merkle.db")

	if err != nil {
		return nil, err
	}

	trieDB := &merkleDB{
		metrics:          metrics,
		disk:             disk,
		history:          newTrieHistory(int(config.HistoryLength)),
		debugTracer:      getTracerIfEnabled(config.TraceLevel, DebugTrace, config.Tracer),
		infoTracer:       getTracerIfEnabled(config.TraceLevel, InfoTrace, config.Tracer),
		childViews:       make([]*view, 0, defaultPreallocationSize),
		hashNodesKeyPool: newBytesPool(rootGenConcurrency),
		tokenSize:        BranchFactorToTokenSize[config.BranchFactor],
		hasher:           hasher,
	}

	shutdownType, err := trieDB.disk.getShutdownType()
	if err != nil {
		return nil, err
	}
	if bytes.Equal(shutdownType, didNotHaveCleanShutdown) {
		// if err := trieDB.rebuild(ctx, int(config.ValueNodeCacheSize)); err != nil {
		// 	return nil, err
		// }
	} else {
		if err := trieDB.initializeRoot(); err != nil {
			log.Println("here")
			return nil, err
		}
	}

	// add current root to history (has no changes)
	trieDB.history.record(&changeSummary{
		rootID: trieDB.rootID,
		rootChange: change[maybe.Maybe[*node]]{
			after: trieDB.root,
		},
		values: map[Key]*change[maybe.Maybe[[]byte]]{},
		nodes:  map[Key]*change[*node]{},
	})

	// mark that the db has not yet been cleanly closed
	//err = trieDB.disk.setShutdownType(didNotHaveCleanShutdown)
	log.Printf("disk type: %T", trieDB.disk)
	return trieDB, err
}

func Test_MerkleDB_Get_Safety_disk(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	db, err := getBasicDB_disk(dir)
	require.NoError(err)

	log.Printf("type of db %T", db.disk)
	defer db.disk.(*rawDisk).close()
	defer db.disk.(*rawDisk).dm.free.close(dir)

	keyBytes := []byte{0}
	require.NoError(db.Put(keyBytes, []byte{0, 1, 2}))

	log.Printf("getting key %x", keyBytes)
	val, err := db.Get(keyBytes)
	require.NoError(err)

	log.Printf("getting node")
	n, err := db.getNode(ToKey(keyBytes), true)
	require.NoError(err)

	// node's value shouldn't be affected by the edit
	originalVal := slices.Clone(val)
	val[0]++
	require.Equal(originalVal, n.value.Value())

	t.Cleanup(func() {
		runtime.GC()	
	})

	log.Printf("after everything")
}

func Test_MerkleDB_DB_Load_Root_From_DB_disk(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	db, err := New_disk(
		context.Background(),
		dir,
		newDefaultConfig(),
	)
	require.NoError(err)

	// Populate initial set of key-value pairs
	keyCount := 100
	ops := make([]database.BatchOp, 0, keyCount)
	require.NoError(err)
	for i := 0; i < keyCount; i++ {
		k := []byte(strconv.Itoa(i))
		ops = append(ops, database.BatchOp{
			Key:   k,
			Value: hashing.ComputeHash256(k),
		})
	}
	view, err := db.NewView(context.Background(), ViewChanges{BatchOps: ops})
	require.NoError(err)
	require.NoError(view.CommitToDB(context.Background()))

	root, err := db.GetMerkleRoot(context.Background())
	require.NoError(err)

	require.NoError(db.Close())

	// reloading the db should set the root back to the one that was saved to [baseDB]
	db, err = New_disk(
		context.Background(),
		dir,
		newDefaultConfig(),
	)
	require.NoError(err)

	reloadedRoot, err := db.GetMerkleRoot(context.Background())
	require.NoError(err)
	require.Equal(root, reloadedRoot)

	
	t.Cleanup(func() {
		runtime.GC()	
	})
}

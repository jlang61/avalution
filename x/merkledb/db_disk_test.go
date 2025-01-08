package merkledb

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ava-labs/avalanchego/database"
	"github.com/ava-labs/avalanchego/database/dbtest"
	"github.com/ava-labs/avalanchego/database/memdb"
	"github.com/ava-labs/avalanchego/utils/hashing"
	"github.com/ava-labs/avalanchego/utils/maybe"
)

func newDB_disk(ctx context.Context, dir string, config Config) (*merkleDB, error) {
	db, err := New_disk(ctx, dir, config)
	if err != nil {
		return nil, err
	}
	return db.(*merkleDB), nil
}

func getBasicDBWithBranchFactor_disk(bf BranchFactor, dir string) (*merkleDB, error) {
	config := newDefaultConfig()
	config.BranchFactor = bf
	return newDatabase_disk(
		context.Background(),
		dir,
		config,
		&mockMetrics{},
	)
}

func getBasicDB_disk(t testing.TB) (*merkleDB, error) {
	// temp, _ := newDatabase_disk(
	// 	context.Background(),
	// 	dir,
	// 	newDefaultConfig(),
	// 	&mockMetrics{},
	// )
	// log.Printf("type of merkle db %T", temp.disk)
	dir := t.TempDir()
	database, err := newDatabase_disk(
		context.Background(),
		dir,
		newDefaultConfig(),
		&mockMetrics{},
	)
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		err = database.disk.(*rawDisk).close()
		if err != nil {
			t.Errorf("error closing disk: %v", err)
		}
		err = database.disk.(*rawDisk).dm.free.close(dir)
		if err != nil {
			t.Errorf("error closing disk: %v", err)
		}

	})
	return database, nil
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
	// log.Printf("disk type: %T", trieDB.disk)
	return trieDB, err
}

func Test_MerkleDB_Get_Safety_disk(t *testing.T) {
	require := require.New(t)

	// generate file, meant to cleanup after itself 
	// doesnt work on windows 
	// manually delete tempdir 
	// file descriptor - handle that gives access to file
	// under the hood of file management
	// close file descriptor
	db, err := getBasicDB_disk(t)
	require.NoError(err)

	keyBytes := []byte{0}
	require.NoError(db.Put(keyBytes, []byte{0, 1, 2}))

	val, err := db.Get(keyBytes)
	require.NoError(err)

	n, err := db.getNode(ToKey(keyBytes), true)
	require.NoError(err)

	// node's value shouldn't be affected by the edit
	originalVal := slices.Clone(val)
	val[0]++
	require.Equal(originalVal, n.value.Value())
}

func Test_MerkleDB_GetValues_Safety_disk(t *testing.T) {
	require := require.New(t)

	db, err := getBasicDB_disk(t)
	require.NoError(err)

	keyBytes := []byte{0}
	value := []byte{0, 1, 2}
	require.NoError(db.Put(keyBytes, value))

	gotValues, errs := db.GetValues(context.Background(), [][]byte{keyBytes})
	require.Len(errs, 1)
	require.NoError(errs[0])
	require.Equal(value, gotValues[0])
	gotValues[0][0]++

	// editing the value array shouldn't affect the db
	gotValues, errs = db.GetValues(context.Background(), [][]byte{keyBytes})
	require.Len(errs, 1)
	require.NoError(errs[0])
	require.Equal(value, gotValues[0])

	t.Cleanup(func() {
		runtime.GC()
	})
}

func Test_MerkleDB_DB_Interface_disk(t *testing.T) {
	dir := t.TempDir()

	for _, bf := range validBranchFactors {
		for name, test := range dbtest.Tests {
			t.Run(fmt.Sprintf("%s_%d", name, bf), func(t *testing.T) {
				db, err := getBasicDBWithBranchFactor_disk(bf, dir)
				require.NoError(t, err)
				test(t, db)
			})
		}
	}

	t.Cleanup(func() {
		runtime.GC()
	})
}

func Benchmark_MerkleDB_DBInterface_disk(b *testing.B) {
	dir := b.TempDir()

	for _, size := range dbtest.BenchmarkSizes {
		keys, values := dbtest.SetupBenchmark(b, size[0], size[1], size[2])
		for _, bf := range validBranchFactors {
			for name, bench := range dbtest.Benchmarks {
				b.Run(fmt.Sprintf("merkledb_%d_%d_pairs_%d_keys_%d_values_%s", bf, size[0], size[1], size[2], name), func(b *testing.B) {
					db, err := getBasicDBWithBranchFactor_disk(bf, dir)
					require.NoError(b, err)
					bench(b, db, keys, values)
				})
			}
		}
	}

	b.Cleanup(func() {
		runtime.GC()
	})
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

func Test_MerkleDB_DB_Rebuild_disk(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	initialSize := 5_000

	config := newDefaultConfig()
	config.ValueNodeCacheSize = uint(initialSize)
	config.IntermediateNodeCacheSize = uint(initialSize)

	db, err := newDB_disk(
		context.Background(),
		dir,
		config,
	)
	require.NoError(err)

	// Populate initial set of keys
	ops := make([]database.BatchOp, 0, initialSize)
	require.NoError(err)
	for i := 0; i < initialSize; i++ {
		k := []byte(strconv.Itoa(i))
		ops = append(ops, database.BatchOp{
			Key:   k,
			Value: hashing.ComputeHash256(k),
		})
	}
	view, err := db.NewView(context.Background(), ViewChanges{BatchOps: ops})
	require.NoError(err)
	require.NoError(view.CommitToDB(context.Background()))

	// Get root
	root, err := db.GetMerkleRoot(context.Background())
	require.NoError(err)

	// Rebuild
	require.NoError(db.rebuild(context.Background(), initialSize))

	// Assert root is the same after rebuild
	rebuiltRoot, err := db.GetMerkleRoot(context.Background())
	require.NoError(err)
	require.Equal(root, rebuiltRoot)

	// add variation where root has a value
	require.NoError(db.Put(nil, []byte{}))

	root, err = db.GetMerkleRoot(context.Background())
	require.NoError(err)

	require.NoError(db.rebuild(context.Background(), initialSize))

	rebuiltRoot, err = db.GetMerkleRoot(context.Background())
	require.NoError(err)
	require.Equal(root, rebuiltRoot)

	t.Cleanup(func() {
		runtime.GC()
	})
}

func Test_MerkleDB_Failed_Batch_Commit_disk(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	memDB := memdb.New()
	db, err := New_disk(
		context.Background(),
		dir,
		newDefaultConfig(),
	)
	require.NoError(err)

	_ = memDB.Close()

	batch := db.NewBatch()
	require.NoError(batch.Put([]byte("key1"), []byte("1")))
	require.NoError(batch.Put([]byte("key2"), []byte("2")))
	require.NoError(batch.Put([]byte("key3"), []byte("3")))
	err = batch.Write()
	require.ErrorIs(err, database.ErrClosed)

	t.Cleanup(func() {
		runtime.GC()
	})
}

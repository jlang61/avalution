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
	return trieDB, err
}

func Test_MerkleDB_Get_Safety_disk(t *testing.T) {
	require := require.New(t)
	dir := t.TempDir()

	db, err := getBasicDB_disk(dir)
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
}

func TestDatabaseCommitChanges_disk(t *testing.T) {
	require := require.New(t)

	db, err := getBasicDB()
	require.NoError(err)
	dbRoot := db.getMerkleRoot()

	// Committing a nil view should be a no-op.
	require.NoError(db.CommitToDB(context.Background()))
	require.Equal(dbRoot, db.getMerkleRoot()) // Root didn't change

	// Committing an invalid view should fail.
	invalidView, err := db.NewView(context.Background(), ViewChanges{})
	require.NoError(err)
	invalidView.(*view).invalidate()
	err = invalidView.CommitToDB(context.Background())
	require.ErrorIs(err, ErrInvalid)

	// Add key-value pairs to the database
	key1, key2, key3 := []byte{1}, []byte{2}, []byte{3}
	value1, value2, value3 := []byte{1}, []byte{2}, []byte{3}
	require.NoError(db.Put(key1, value1))
	require.NoError(db.Put(key2, value2))

	// Make a view and insert/delete a key-value pair.
	view1Intf, err := db.NewView(
		context.Background(),
		ViewChanges{
			BatchOps: []database.BatchOp{
				{Key: key3, Value: value3}, // New k-v pair
				{Key: key1, Delete: true},  // Delete k-v pair
			},
		},
	)
	require.NoError(err)
	require.IsType(&view{}, view1Intf)
	view1 := view1Intf.(*view)
	view1Root, err := view1.GetMerkleRoot(context.Background())
	require.NoError(err)

	// Make a second view
	view2Intf, err := db.NewView(context.Background(), ViewChanges{})
	require.NoError(err)
	require.IsType(&view{}, view2Intf)
	view2 := view2Intf.(*view)

	// Make a view atop a view
	view3Intf, err := view1.NewView(context.Background(), ViewChanges{})
	require.NoError(err)
	require.IsType(&view{}, view3Intf)
	view3 := view3Intf.(*view)

	// view3
	//  |
	// view1   view2
	//     \  /
	//      db

	// Commit view1
	require.NoError(view1.commitToDB(context.Background()))

	// Make sure the key-value pairs are correct.
	_, err = db.Get(key1)
	require.ErrorIs(err, database.ErrNotFound)
	gotValue, err := db.Get(key2)
	require.NoError(err)
	require.Equal(value2, gotValue)
	gotValue, err = db.Get(key3)
	require.NoError(err)
	require.Equal(value3, gotValue)

	// Make sure the root is right
	require.Equal(view1Root, db.getMerkleRoot())

	// Make sure view2 is invalid and view1 and view3 is valid.
	require.False(view1.invalidated)
	require.True(view2.invalidated)
	require.False(view3.invalidated)

	// Make sure view2 isn't tracked by the database.
	require.NotContains(db.childViews, view2)

	// Make sure view1 and view3 is tracked by the database.
	require.Contains(db.childViews, view1)
	require.Contains(db.childViews, view3)

	// Make sure view3 is now a child of db.
	require.Equal(db, view3.parentTrie)
}

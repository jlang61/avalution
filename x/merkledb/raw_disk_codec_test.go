package merkledb

import (
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
	"github.com/stretchr/testify/require"
)

var (
	encodeDBNodeTests_disk = []struct {
		name          string
		n             *dbNode
		expectedBytes []byte
	}{
		{
			name: "empty node",
			n: &dbNode{
				children: make(map[byte]*child),
				//diskAddr: diskAddress{offset: 1, size: 2},
			},
			expectedBytes: []byte{
				0x00, // value.HasValue()
				0x00, // len(children)
			},
		},
		{
			name: "has value",
			n: &dbNode{
				value:    maybe.Some([]byte("value")),
				children: make(map[byte]*child),
				//diskAddr: diskAddress{offset: 1, size: 2},
			},
			expectedBytes: []byte{
				0x01,                    // value.HasValue()
				0x05,                    // len(value.Value())
				'v', 'a', 'l', 'u', 'e', // value.Value()
				0x00, // len(children)
			},
		},
		{
			name: "1 child",
			n: &dbNode{
				value: maybe.Some([]byte("value")),
				children: map[byte]*child{
					0: {
						compressedKey: ToKey([]byte{0}),
						id: ids.ID{
							0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
							0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
							0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
							0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
						},
						hasValue: true,
						diskAddr: diskAddress{offset: 1, size: 2},
					},
				},
			},
			expectedBytes: []byte{
				0x01,                    // value.HasValue()
				0x05,                    // len(value.Value())
				'v', 'a', 'l', 'u', 'e', // value.Value()
				0x01, // len(children)
				0x00, // children[0].index
				0x08, // len(children[0].compressedKey)
				0x00, // children[0].compressedKey
				// children[0].id
				0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
				0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
				0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
				0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
				// disk addr
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,
				0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02,
				0x01, // children[0].hasValue
			},
		},
	}
)

func TestEncodeDBNode_disk(t *testing.T) {
	for _, test := range encodeDBNodeTests_disk {
		t.Run(test.name, func(t *testing.T) {
			bytes := encodeDBNode_disk(test.n)
			require.Equal(t, test.expectedBytes, bytes)
		})
	}
}

func TestDecodeDBNode_disk(t *testing.T) {
	for _, test := range encodeDBNodeTests_disk {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var n dbNode
			require.NoError(decodeDBNode_disk(test.expectedBytes, &n))
			require.Equal(test.n, &n)
		})
	}
}

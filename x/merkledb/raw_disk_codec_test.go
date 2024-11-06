package merkledb

import (
	"testing"

	"github.com/ava-labs/avalanchego/ids"
	"github.com/ava-labs/avalanchego/utils/maybe"
	"github.com/stretchr/testify/require"
)

var (
	encodeDBNodeTests = []struct {
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
	encodeKeyTests = []struct {
		name          string
		key           Key
		expectedBytes []byte
	}{
		{
			name: "empty",
			key:  ToKey([]byte{}),
			expectedBytes: []byte{
				0x00, // length
			},
		},
		{
			name: "1 byte",
			key:  ToKey([]byte{0}),
			expectedBytes: []byte{
				0x08, // length
				0x00, // key
			},
		},
		{
			name: "2 bytes",
			key:  ToKey([]byte{0, 1}),
			expectedBytes: []byte{
				0x10,       // length
				0x00, 0x01, // key
			},
		},
		{
			name: "4 bytes",
			key:  ToKey([]byte{0, 1, 2, 3}),
			expectedBytes: []byte{
				0x20,                   // length
				0x00, 0x01, 0x02, 0x03, // key
			},
		},
		{
			name: "8 bytes",
			key:  ToKey([]byte{0, 1, 2, 3, 4, 5, 6, 7}),
			expectedBytes: []byte{
				0x40,                                           // length
				0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, // key
			},
		},
		{
			name: "32 bytes",
			key:  ToKey(make([]byte, 32)),
			expectedBytes: append(
				[]byte{
					0x80, 0x02, // length
				},
				make([]byte, 32)..., // key
			),
		},
		{
			name: "64 bytes",
			key:  ToKey(make([]byte, 64)),
			expectedBytes: append(
				[]byte{
					0x80, 0x04, // length
				},
				make([]byte, 64)..., // key
			),
		},
		{
			name: "1024 bytes",
			key:  ToKey(make([]byte, 1024)),
			expectedBytes: append(
				[]byte{
					0x80, 0x40, // length
				},
				make([]byte, 1024)..., // key
			),
		},
	}
)

func TestEncodeDBNode(t *testing.T) {
	for _, test := range encodeDBNodeTests {
		t.Run(test.name, func(t *testing.T) {
			bytes := encodeDBNode(test.n)
			require.Equal(t, test.expectedBytes, bytes)
		})
	}
}

func TestDecodeDBNode(t *testing.T) {
	for _, test := range encodeDBNodeTests {
		t.Run(test.name, func(t *testing.T) {
			require := require.New(t)

			var n dbNode
			require.NoError(decodeDBNode(test.expectedBytes, &n))
			require.Equal(test.n, &n)
		})
	}
}

func TestEncodeKey(t *testing.T) {
	for _, test := range encodeKeyTests {
		t.Run(test.name, func(t *testing.T) {
			bytes := encodeKey(test.key)
			require.Equal(t, test.expectedBytes, bytes)
		})
	}
}

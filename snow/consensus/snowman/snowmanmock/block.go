// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/ava-labs/avalanchego/snow/consensus/snowman (interfaces: Block)
//
// Generated by this command:
//
//	mockgen -package=snowmanmock -destination=snow/consensus/snowman/snowmanmock/block.go -mock_names=Block=Block github.com/ava-labs/avalanchego/snow/consensus/snowman Block
//

// Package snowmanmock is a generated GoMock package.
package snowmanmock

import (
	context "context"
	reflect "reflect"
	time "time"

	ids "github.com/ava-labs/avalanchego/ids"
	gomock "go.uber.org/mock/gomock"
)

// Block is a mock of Block interface.
type Block struct {
	ctrl     *gomock.Controller
	recorder *BlockMockRecorder
}

// BlockMockRecorder is the mock recorder for Block.
type BlockMockRecorder struct {
	mock *Block
}

// NewBlock creates a new mock instance.
func NewBlock(ctrl *gomock.Controller) *Block {
	mock := &Block{ctrl: ctrl}
	mock.recorder = &BlockMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *Block) EXPECT() *BlockMockRecorder {
	return m.recorder
}

// Accept mocks base method.
func (m *Block) Accept(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Accept", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Accept indicates an expected call of Accept.
func (mr *BlockMockRecorder) Accept(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Accept", reflect.TypeOf((*Block)(nil).Accept), arg0)
}

// Bytes mocks base method.
func (m *Block) Bytes() []byte {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Bytes")
	ret0, _ := ret[0].([]byte)
	return ret0
}

// Bytes indicates an expected call of Bytes.
func (mr *BlockMockRecorder) Bytes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Bytes", reflect.TypeOf((*Block)(nil).Bytes))
}

// Height mocks base method.
func (m *Block) Height() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Height")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// Height indicates an expected call of Height.
func (mr *BlockMockRecorder) Height() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Height", reflect.TypeOf((*Block)(nil).Height))
}

// ID mocks base method.
func (m *Block) ID() ids.ID {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ID")
	ret0, _ := ret[0].(ids.ID)
	return ret0
}

// ID indicates an expected call of ID.
func (mr *BlockMockRecorder) ID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ID", reflect.TypeOf((*Block)(nil).ID))
}

// Parent mocks base method.
func (m *Block) Parent() ids.ID {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Parent")
	ret0, _ := ret[0].(ids.ID)
	return ret0
}

// Parent indicates an expected call of Parent.
func (mr *BlockMockRecorder) Parent() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Parent", reflect.TypeOf((*Block)(nil).Parent))
}

// Reject mocks base method.
func (m *Block) Reject(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Reject", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Reject indicates an expected call of Reject.
func (mr *BlockMockRecorder) Reject(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Reject", reflect.TypeOf((*Block)(nil).Reject), arg0)
}

// Timestamp mocks base method.
func (m *Block) Timestamp() time.Time {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Timestamp")
	ret0, _ := ret[0].(time.Time)
	return ret0
}

// Timestamp indicates an expected call of Timestamp.
func (mr *BlockMockRecorder) Timestamp() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Timestamp", reflect.TypeOf((*Block)(nil).Timestamp))
}

// Verify mocks base method.
func (m *Block) Verify(arg0 context.Context) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Verify", arg0)
	ret0, _ := ret[0].(error)
	return ret0
}

// Verify indicates an expected call of Verify.
func (mr *BlockMockRecorder) Verify(arg0 any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Verify", reflect.TypeOf((*Block)(nil).Verify), arg0)
}

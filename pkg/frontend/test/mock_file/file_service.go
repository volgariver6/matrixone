// Code generated by MockGen. DO NOT EDIT.
// Source: pkg/fileservice/file_service.go
//
// Generated by this command:
//
//	mockgen -source pkg/fileservice/file_service.go --destination pkg/frontend/test/mock_file/file_service.go -package=mock_file
//

// Package mock_file is a generated GoMock package.
package mock_file

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	fileservice "github.com/matrixorigin/matrixone/pkg/fileservice"
	fscache "github.com/matrixorigin/matrixone/pkg/fileservice/fscache"
)

// MockFileService is a mock of FileService interface.
type MockFileService struct {
	ctrl     *gomock.Controller
	recorder *MockFileServiceMockRecorder
}

// MockFileServiceMockRecorder is the mock recorder for MockFileService.
type MockFileServiceMockRecorder struct {
	mock *MockFileService
}

// NewMockFileService creates a new mock instance.
func NewMockFileService(ctrl *gomock.Controller) *MockFileService {
	mock := &MockFileService{ctrl: ctrl}
	mock.recorder = &MockFileServiceMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockFileService) EXPECT() *MockFileServiceMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockFileService) Close() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "Close")
}

// Close indicates an expected call of Close.
func (mr *MockFileServiceMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockFileService)(nil).Close))
}

// Cost mocks base method.
func (m *MockFileService) Cost() *fileservice.CostAttr {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Cost")
	ret0, _ := ret[0].(*fileservice.CostAttr)
	return ret0
}

// Cost indicates an expected call of Cost.
func (mr *MockFileServiceMockRecorder) Cost() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Cost", reflect.TypeOf((*MockFileService)(nil).Cost))
}

// Delete mocks base method.
func (m *MockFileService) Delete(ctx context.Context, filePaths ...string) error {
	m.ctrl.T.Helper()
	varargs := []any{ctx}
	for _, a := range filePaths {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Delete", varargs...)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockFileServiceMockRecorder) Delete(ctx any, filePaths ...any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]any{ctx}, filePaths...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockFileService)(nil).Delete), varargs...)
}

// List mocks base method.
func (m *MockFileService) List(ctx context.Context, dirPath string) ([]fileservice.DirEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "List", ctx, dirPath)
	ret0, _ := ret[0].([]fileservice.DirEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// List indicates an expected call of List.
func (mr *MockFileServiceMockRecorder) List(ctx, dirPath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "List", reflect.TypeOf((*MockFileService)(nil).List), ctx, dirPath)
}

// Name mocks base method.
func (m *MockFileService) Name() string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Name")
	ret0, _ := ret[0].(string)
	return ret0
}

// Name indicates an expected call of Name.
func (mr *MockFileServiceMockRecorder) Name() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Name", reflect.TypeOf((*MockFileService)(nil).Name))
}

// PrefetchFile mocks base method.
func (m *MockFileService) PrefetchFile(ctx context.Context, filePath string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "PrefetchFile", ctx, filePath)
	ret0, _ := ret[0].(error)
	return ret0
}

// PrefetchFile indicates an expected call of PrefetchFile.
func (mr *MockFileServiceMockRecorder) PrefetchFile(ctx, filePath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PrefetchFile", reflect.TypeOf((*MockFileService)(nil).PrefetchFile), ctx, filePath)
}

// Read mocks base method.
func (m *MockFileService) Read(ctx context.Context, vector *fileservice.IOVector) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", ctx, vector)
	ret0, _ := ret[0].(error)
	return ret0
}

// Read indicates an expected call of Read.
func (mr *MockFileServiceMockRecorder) Read(ctx, vector any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockFileService)(nil).Read), ctx, vector)
}

// ReadCache mocks base method.
func (m *MockFileService) ReadCache(ctx context.Context, vector *fileservice.IOVector) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ReadCache", ctx, vector)
	ret0, _ := ret[0].(error)
	return ret0
}

// ReadCache indicates an expected call of ReadCache.
func (mr *MockFileServiceMockRecorder) ReadCache(ctx, vector any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ReadCache", reflect.TypeOf((*MockFileService)(nil).ReadCache), ctx, vector)
}

// StatFile mocks base method.
func (m *MockFileService) StatFile(ctx context.Context, filePath string) (*fileservice.DirEntry, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StatFile", ctx, filePath)
	ret0, _ := ret[0].(*fileservice.DirEntry)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// StatFile indicates an expected call of StatFile.
func (mr *MockFileServiceMockRecorder) StatFile(ctx, filePath any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StatFile", reflect.TypeOf((*MockFileService)(nil).StatFile), ctx, filePath)
}

// Write mocks base method.
func (m *MockFileService) Write(ctx context.Context, vector fileservice.IOVector) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", ctx, vector)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockFileServiceMockRecorder) Write(ctx, vector any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockFileService)(nil).Write), ctx, vector)
}

// MockCacheDataAllocator is a mock of CacheDataAllocator interface.
type MockCacheDataAllocator struct {
	ctrl     *gomock.Controller
	recorder *MockCacheDataAllocatorMockRecorder
}

// MockCacheDataAllocatorMockRecorder is the mock recorder for MockCacheDataAllocator.
type MockCacheDataAllocatorMockRecorder struct {
	mock *MockCacheDataAllocator
}

// NewMockCacheDataAllocator creates a new mock instance.
func NewMockCacheDataAllocator(ctrl *gomock.Controller) *MockCacheDataAllocator {
	mock := &MockCacheDataAllocator{ctrl: ctrl}
	mock.recorder = &MockCacheDataAllocatorMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCacheDataAllocator) EXPECT() *MockCacheDataAllocatorMockRecorder {
	return m.recorder
}

// Alloc mocks base method.
func (m *MockCacheDataAllocator) Alloc(size int) fscache.Data {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Alloc", size)
	ret0, _ := ret[0].(fscache.Data)
	return ret0
}

// Alloc indicates an expected call of Alloc.
func (mr *MockCacheDataAllocatorMockRecorder) Alloc(size any) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Alloc", reflect.TypeOf((*MockCacheDataAllocator)(nil).Alloc), size)
}
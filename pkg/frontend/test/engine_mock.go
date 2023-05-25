// Code generated by MockGen. DO NOT EDIT.
// Source: ../../../pkg/vm/engine/types.go

// Package mock_frontend is a generated GoMock package.
package mock_frontend

import (
	context "context"
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	mpool "github.com/matrixorigin/matrixone/pkg/common/mpool"
	batch "github.com/matrixorigin/matrixone/pkg/container/batch"
	types "github.com/matrixorigin/matrixone/pkg/container/types"
	vector "github.com/matrixorigin/matrixone/pkg/container/vector"
	plan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	timestamp "github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	client "github.com/matrixorigin/matrixone/pkg/txn/client"
	engine "github.com/matrixorigin/matrixone/pkg/vm/engine"
)

// MockStatistics is a mock of Statistics interface.
type MockStatistics struct {
	ctrl     *gomock.Controller
	recorder *MockStatisticsMockRecorder
}

// MockStatisticsMockRecorder is the mock recorder for MockStatistics.
type MockStatisticsMockRecorder struct {
	mock *MockStatistics
}

// NewMockStatistics creates a new mock instance.
func NewMockStatistics(ctrl *gomock.Controller) *MockStatistics {
	mock := &MockStatistics{ctrl: ctrl}
	mock.recorder = &MockStatisticsMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockStatistics) EXPECT() *MockStatisticsMockRecorder {
	return m.recorder
}

// Rows mocks base method.
func (m *MockStatistics) Rows(ctx context.Context) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rows", ctx)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Rows indicates an expected call of Rows.
func (mr *MockStatisticsMockRecorder) Rows(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rows", reflect.TypeOf((*MockStatistics)(nil).Rows), ctx)
}

// Size mocks base method.
func (m *MockStatistics) Size(ctx context.Context, columnName string) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Size", ctx, columnName)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Size indicates an expected call of Size.
func (mr *MockStatisticsMockRecorder) Size(ctx, columnName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Size", reflect.TypeOf((*MockStatistics)(nil).Size), ctx, columnName)
}

// Stats mocks base method.
func (m *MockStatistics) Stats(ctx context.Context, statsInfoMap any) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stats", ctx, statsInfoMap)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Stats indicates an expected call of Stats.
func (mr *MockStatisticsMockRecorder) Stats(ctx, statsInfoMap interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stats", reflect.TypeOf((*MockStatistics)(nil).Stats), ctx, statsInfoMap)
}

// MockTableDef is a mock of TableDef interface.
type MockTableDef struct {
	ctrl     *gomock.Controller
	recorder *MockTableDefMockRecorder
}

// MockTableDefMockRecorder is the mock recorder for MockTableDef.
type MockTableDefMockRecorder struct {
	mock *MockTableDef
}

// NewMockTableDef creates a new mock instance.
func NewMockTableDef(ctrl *gomock.Controller) *MockTableDef {
	mock := &MockTableDef{ctrl: ctrl}
	mock.recorder = &MockTableDefMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockTableDef) EXPECT() *MockTableDefMockRecorder {
	return m.recorder
}

// ToPBVersion mocks base method.
func (m *MockTableDef) ToPBVersion() engine.TableDefPB {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ToPBVersion")
	ret0, _ := ret[0].(engine.TableDefPB)
	return ret0
}

// ToPBVersion indicates an expected call of ToPBVersion.
func (mr *MockTableDefMockRecorder) ToPBVersion() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ToPBVersion", reflect.TypeOf((*MockTableDef)(nil).ToPBVersion))
}

// tableDef mocks base method.
func (m *MockTableDef) tableDef() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "tableDef")
}

// tableDef indicates an expected call of tableDef.
func (mr *MockTableDefMockRecorder) tableDef() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "tableDef", reflect.TypeOf((*MockTableDef)(nil).tableDef))
}

// MockConstraint is a mock of Constraint interface.
type MockConstraint struct {
	ctrl     *gomock.Controller
	recorder *MockConstraintMockRecorder
}

// MockConstraintMockRecorder is the mock recorder for MockConstraint.
type MockConstraintMockRecorder struct {
	mock *MockConstraint
}

// NewMockConstraint creates a new mock instance.
func NewMockConstraint(ctrl *gomock.Controller) *MockConstraint {
	mock := &MockConstraint{ctrl: ctrl}
	mock.recorder = &MockConstraintMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConstraint) EXPECT() *MockConstraintMockRecorder {
	return m.recorder
}

// ToPBVersion mocks base method.
func (m *MockConstraint) ToPBVersion() engine.ConstraintPB {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ToPBVersion")
	ret0, _ := ret[0].(engine.ConstraintPB)
	return ret0
}

// ToPBVersion indicates an expected call of ToPBVersion.
func (mr *MockConstraintMockRecorder) ToPBVersion() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ToPBVersion", reflect.TypeOf((*MockConstraint)(nil).ToPBVersion))
}

// constraint mocks base method.
func (m *MockConstraint) constraint() {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "constraint")
}

// constraint indicates an expected call of constraint.
func (mr *MockConstraintMockRecorder) constraint() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "constraint", reflect.TypeOf((*MockConstraint)(nil).constraint))
}

// MockRelation is a mock of Relation interface.
type MockRelation struct {
	ctrl     *gomock.Controller
	recorder *MockRelationMockRecorder
}

// MockRelationMockRecorder is the mock recorder for MockRelation.
type MockRelationMockRecorder struct {
	mock *MockRelation
}

// NewMockRelation creates a new mock instance.
func NewMockRelation(ctrl *gomock.Controller) *MockRelation {
	mock := &MockRelation{ctrl: ctrl}
	mock.recorder = &MockRelationMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockRelation) EXPECT() *MockRelationMockRecorder {
	return m.recorder
}

// AddTableDef mocks base method.
func (m *MockRelation) AddTableDef(arg0 context.Context, arg1 engine.TableDef) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AddTableDef", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// AddTableDef indicates an expected call of AddTableDef.
func (mr *MockRelationMockRecorder) AddTableDef(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AddTableDef", reflect.TypeOf((*MockRelation)(nil).AddTableDef), arg0, arg1)
}

// DelTableDef mocks base method.
func (m *MockRelation) DelTableDef(arg0 context.Context, arg1 engine.TableDef) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DelTableDef", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// DelTableDef indicates an expected call of DelTableDef.
func (mr *MockRelationMockRecorder) DelTableDef(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DelTableDef", reflect.TypeOf((*MockRelation)(nil).DelTableDef), arg0, arg1)
}

// Delete mocks base method.
func (m *MockRelation) Delete(arg0 context.Context, arg1 *batch.Batch, arg2 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockRelationMockRecorder) Delete(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockRelation)(nil).Delete), arg0, arg1, arg2)
}

// GetEngineType mocks base method.
func (m *MockRelation) GetEngineType() engine.EngineType {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetEngineType")
	ret0, _ := ret[0].(engine.EngineType)
	return ret0
}

// GetEngineType indicates an expected call of GetEngineType.
func (mr *MockRelationMockRecorder) GetEngineType() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetEngineType", reflect.TypeOf((*MockRelation)(nil).GetEngineType))
}

// GetHideKeys mocks base method.
func (m *MockRelation) GetHideKeys(arg0 context.Context) ([]*engine.Attribute, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetHideKeys", arg0)
	ret0, _ := ret[0].([]*engine.Attribute)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetHideKeys indicates an expected call of GetHideKeys.
func (mr *MockRelationMockRecorder) GetHideKeys(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetHideKeys", reflect.TypeOf((*MockRelation)(nil).GetHideKeys), arg0)
}

// GetMetadataScanInfoBytes mocks base method.
func (m *MockRelation) GetMetadataScanInfoBytes(ctx context.Context, name string) ([][]byte, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetMetadataScanInfoBytes", ctx, name)
	ret0, _ := ret[0].([][]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetMetadataScanInfoBytes indicates an expected call of GetMetadataScanInfoBytes.
func (mr *MockRelationMockRecorder) GetMetadataScanInfoBytes(ctx, name interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetMetadataScanInfoBytes", reflect.TypeOf((*MockRelation)(nil).GetMetadataScanInfoBytes), ctx, name)
}

// GetPrimaryKeys mocks base method.
func (m *MockRelation) GetPrimaryKeys(arg0 context.Context) ([]*engine.Attribute, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetPrimaryKeys", arg0)
	ret0, _ := ret[0].([]*engine.Attribute)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetPrimaryKeys indicates an expected call of GetPrimaryKeys.
func (mr *MockRelationMockRecorder) GetPrimaryKeys(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetPrimaryKeys", reflect.TypeOf((*MockRelation)(nil).GetPrimaryKeys), arg0)
}

// GetTableID mocks base method.
func (m *MockRelation) GetTableID(arg0 context.Context) uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetTableID", arg0)
	ret0, _ := ret[0].(uint64)
	return ret0
}

// GetTableID indicates an expected call of GetTableID.
func (mr *MockRelationMockRecorder) GetTableID(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetTableID", reflect.TypeOf((*MockRelation)(nil).GetTableID), arg0)
}

// MaxAndMinValues mocks base method.
func (m *MockRelation) MaxAndMinValues(ctx context.Context) ([][2]any, []uint8, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "MaxAndMinValues", ctx)
	ret0, _ := ret[0].([][2]any)
	ret1, _ := ret[1].([]uint8)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// MaxAndMinValues indicates an expected call of MaxAndMinValues.
func (mr *MockRelationMockRecorder) MaxAndMinValues(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "MaxAndMinValues", reflect.TypeOf((*MockRelation)(nil).MaxAndMinValues), ctx)
}

// NewReader mocks base method.
func (m *MockRelation) NewReader(arg0 context.Context, arg1 int, arg2 *plan.Expr, arg3 [][]byte) ([]engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewReader", arg0, arg1, arg2, arg3)
	ret0, _ := ret[0].([]engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewReader indicates an expected call of NewReader.
func (mr *MockRelationMockRecorder) NewReader(arg0, arg1, arg2, arg3 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewReader", reflect.TypeOf((*MockRelation)(nil).NewReader), arg0, arg1, arg2, arg3)
}

// Ranges mocks base method.
func (m *MockRelation) Ranges(arg0 context.Context, arg1 ...*plan.Expr) ([][]byte, error) {
	m.ctrl.T.Helper()
	varargs := []interface{}{arg0}
	for _, a := range arg1 {
		varargs = append(varargs, a)
	}
	ret := m.ctrl.Call(m, "Ranges", varargs...)
	ret0, _ := ret[0].([][]byte)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Ranges indicates an expected call of Ranges.
func (mr *MockRelationMockRecorder) Ranges(arg0 interface{}, arg1 ...interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	varargs := append([]interface{}{arg0}, arg1...)
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Ranges", reflect.TypeOf((*MockRelation)(nil).Ranges), varargs...)
}

// Rows mocks base method.
func (m *MockRelation) Rows(ctx context.Context) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rows", ctx)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Rows indicates an expected call of Rows.
func (mr *MockRelationMockRecorder) Rows(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rows", reflect.TypeOf((*MockRelation)(nil).Rows), ctx)
}

// Size mocks base method.
func (m *MockRelation) Size(ctx context.Context, columnName string) (int64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Size", ctx, columnName)
	ret0, _ := ret[0].(int64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Size indicates an expected call of Size.
func (mr *MockRelationMockRecorder) Size(ctx, columnName interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Size", reflect.TypeOf((*MockRelation)(nil).Size), ctx, columnName)
}

// Stats mocks base method.
func (m *MockRelation) Stats(ctx context.Context, statsInfoMap any) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Stats", ctx, statsInfoMap)
	ret0, _ := ret[0].(bool)
	return ret0
}

// Stats indicates an expected call of Stats.
func (mr *MockRelationMockRecorder) Stats(ctx, statsInfoMap interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Stats", reflect.TypeOf((*MockRelation)(nil).Stats), ctx, statsInfoMap)
}

// TableColumns mocks base method.
func (m *MockRelation) TableColumns(ctx context.Context) ([]*engine.Attribute, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TableColumns", ctx)
	ret0, _ := ret[0].([]*engine.Attribute)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TableColumns indicates an expected call of TableColumns.
func (mr *MockRelationMockRecorder) TableColumns(ctx interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TableColumns", reflect.TypeOf((*MockRelation)(nil).TableColumns), ctx)
}

// TableDefs mocks base method.
func (m *MockRelation) TableDefs(arg0 context.Context) ([]engine.TableDef, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TableDefs", arg0)
	ret0, _ := ret[0].([]engine.TableDef)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TableDefs indicates an expected call of TableDefs.
func (mr *MockRelationMockRecorder) TableDefs(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TableDefs", reflect.TypeOf((*MockRelation)(nil).TableDefs), arg0)
}

// Update mocks base method.
func (m *MockRelation) Update(arg0 context.Context, arg1 *batch.Batch) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Update", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Update indicates an expected call of Update.
func (mr *MockRelationMockRecorder) Update(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Update", reflect.TypeOf((*MockRelation)(nil).Update), arg0, arg1)
}

// UpdateConstraint mocks base method.
func (m *MockRelation) UpdateConstraint(arg0 context.Context, arg1 *engine.ConstraintDef) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateConstraint", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// UpdateConstraint indicates an expected call of UpdateConstraint.
func (mr *MockRelationMockRecorder) UpdateConstraint(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateConstraint", reflect.TypeOf((*MockRelation)(nil).UpdateConstraint), arg0, arg1)
}

// Write mocks base method.
func (m *MockRelation) Write(arg0 context.Context, arg1 *batch.Batch) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Write", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Write indicates an expected call of Write.
func (mr *MockRelationMockRecorder) Write(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Write", reflect.TypeOf((*MockRelation)(nil).Write), arg0, arg1)
}

// MockReader is a mock of Reader interface.
type MockReader struct {
	ctrl     *gomock.Controller
	recorder *MockReaderMockRecorder
}

// MockReaderMockRecorder is the mock recorder for MockReader.
type MockReaderMockRecorder struct {
	mock *MockReader
}

// NewMockReader creates a new mock instance.
func NewMockReader(ctrl *gomock.Controller) *MockReader {
	mock := &MockReader{ctrl: ctrl}
	mock.recorder = &MockReaderMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockReader) EXPECT() *MockReaderMockRecorder {
	return m.recorder
}

// Close mocks base method.
func (m *MockReader) Close() error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Close")
	ret0, _ := ret[0].(error)
	return ret0
}

// Close indicates an expected call of Close.
func (mr *MockReaderMockRecorder) Close() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Close", reflect.TypeOf((*MockReader)(nil).Close))
}

// Read mocks base method.
func (m *MockReader) Read(arg0 context.Context, arg1 []string, arg2 *plan.Expr, arg3 *mpool.MPool, arg4 engine.VectorPool) (*batch.Batch, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Read", arg0, arg1, arg2, arg3, arg4)
	ret0, _ := ret[0].(*batch.Batch)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Read indicates an expected call of Read.
func (mr *MockReaderMockRecorder) Read(arg0, arg1, arg2, arg3, arg4 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Read", reflect.TypeOf((*MockReader)(nil).Read), arg0, arg1, arg2, arg3, arg4)
}

// MockDatabase is a mock of Database interface.
type MockDatabase struct {
	ctrl     *gomock.Controller
	recorder *MockDatabaseMockRecorder
}

// MockDatabaseMockRecorder is the mock recorder for MockDatabase.
type MockDatabaseMockRecorder struct {
	mock *MockDatabase
}

// NewMockDatabase creates a new mock instance.
func NewMockDatabase(ctrl *gomock.Controller) *MockDatabase {
	mock := &MockDatabase{ctrl: ctrl}
	mock.recorder = &MockDatabaseMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockDatabase) EXPECT() *MockDatabaseMockRecorder {
	return m.recorder
}

// Create mocks base method.
func (m *MockDatabase) Create(arg0 context.Context, arg1 string, arg2 []engine.TableDef) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", arg0, arg1, arg2)
	ret0, _ := ret[0].(error)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *MockDatabaseMockRecorder) Create(arg0, arg1, arg2 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockDatabase)(nil).Create), arg0, arg1, arg2)
}

// Delete mocks base method.
func (m *MockDatabase) Delete(arg0 context.Context, arg1 string) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", arg0, arg1)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockDatabaseMockRecorder) Delete(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockDatabase)(nil).Delete), arg0, arg1)
}

// GetCreateSql mocks base method.
func (m *MockDatabase) GetCreateSql(arg0 context.Context) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCreateSql", arg0)
	ret0, _ := ret[0].(string)
	return ret0
}

// GetCreateSql indicates an expected call of GetCreateSql.
func (mr *MockDatabaseMockRecorder) GetCreateSql(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCreateSql", reflect.TypeOf((*MockDatabase)(nil).GetCreateSql), arg0)
}

// GetDatabaseId mocks base method.
func (m *MockDatabase) GetDatabaseId(arg0 context.Context) string {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDatabaseId", arg0)
	ret0, _ := ret[0].(string)
	return ret0
}

// GetDatabaseId indicates an expected call of GetDatabaseId.
func (mr *MockDatabaseMockRecorder) GetDatabaseId(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDatabaseId", reflect.TypeOf((*MockDatabase)(nil).GetDatabaseId), arg0)
}

// IsSubscription mocks base method.
func (m *MockDatabase) IsSubscription(arg0 context.Context) bool {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "IsSubscription", arg0)
	ret0, _ := ret[0].(bool)
	return ret0
}

// IsSubscription indicates an expected call of IsSubscription.
func (mr *MockDatabaseMockRecorder) IsSubscription(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "IsSubscription", reflect.TypeOf((*MockDatabase)(nil).IsSubscription), arg0)
}

// Relation mocks base method.
func (m *MockDatabase) Relation(arg0 context.Context, arg1 string) (engine.Relation, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Relation", arg0, arg1)
	ret0, _ := ret[0].(engine.Relation)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Relation indicates an expected call of Relation.
func (mr *MockDatabaseMockRecorder) Relation(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Relation", reflect.TypeOf((*MockDatabase)(nil).Relation), arg0, arg1)
}

// Relations mocks base method.
func (m *MockDatabase) Relations(arg0 context.Context) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Relations", arg0)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Relations indicates an expected call of Relations.
func (mr *MockDatabaseMockRecorder) Relations(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Relations", reflect.TypeOf((*MockDatabase)(nil).Relations), arg0)
}

// Truncate mocks base method.
func (m *MockDatabase) Truncate(arg0 context.Context, arg1 string) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Truncate", arg0, arg1)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Truncate indicates an expected call of Truncate.
func (mr *MockDatabaseMockRecorder) Truncate(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Truncate", reflect.TypeOf((*MockDatabase)(nil).Truncate), arg0, arg1)
}

// MockEngine is a mock of Engine interface.
type MockEngine struct {
	ctrl     *gomock.Controller
	recorder *MockEngineMockRecorder
}

// MockEngineMockRecorder is the mock recorder for MockEngine.
type MockEngineMockRecorder struct {
	mock *MockEngine
}

// NewMockEngine creates a new mock instance.
func NewMockEngine(ctrl *gomock.Controller) *MockEngine {
	mock := &MockEngine{ctrl: ctrl}
	mock.recorder = &MockEngineMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockEngine) EXPECT() *MockEngineMockRecorder {
	return m.recorder
}

// AllocateIDByKey mocks base method.
func (m *MockEngine) AllocateIDByKey(ctx context.Context, key string) (uint64, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "AllocateIDByKey", ctx, key)
	ret0, _ := ret[0].(uint64)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// AllocateIDByKey indicates an expected call of AllocateIDByKey.
func (mr *MockEngineMockRecorder) AllocateIDByKey(ctx, key interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "AllocateIDByKey", reflect.TypeOf((*MockEngine)(nil).AllocateIDByKey), ctx, key)
}

// Commit mocks base method.
func (m *MockEngine) Commit(ctx context.Context, op client.TxnOperator) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Commit", ctx, op)
	ret0, _ := ret[0].(error)
	return ret0
}

// Commit indicates an expected call of Commit.
func (mr *MockEngineMockRecorder) Commit(ctx, op interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Commit", reflect.TypeOf((*MockEngine)(nil).Commit), ctx, op)
}

// Create mocks base method.
func (m *MockEngine) Create(ctx context.Context, databaseName string, op client.TxnOperator) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Create", ctx, databaseName, op)
	ret0, _ := ret[0].(error)
	return ret0
}

// Create indicates an expected call of Create.
func (mr *MockEngineMockRecorder) Create(ctx, databaseName, op interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Create", reflect.TypeOf((*MockEngine)(nil).Create), ctx, databaseName, op)
}

// Database mocks base method.
func (m *MockEngine) Database(ctx context.Context, databaseName string, op client.TxnOperator) (engine.Database, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Database", ctx, databaseName, op)
	ret0, _ := ret[0].(engine.Database)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Database indicates an expected call of Database.
func (mr *MockEngineMockRecorder) Database(ctx, databaseName, op interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Database", reflect.TypeOf((*MockEngine)(nil).Database), ctx, databaseName, op)
}

// Databases mocks base method.
func (m *MockEngine) Databases(ctx context.Context, op client.TxnOperator) ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Databases", ctx, op)
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Databases indicates an expected call of Databases.
func (mr *MockEngineMockRecorder) Databases(ctx, op interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Databases", reflect.TypeOf((*MockEngine)(nil).Databases), ctx, op)
}

// Delete mocks base method.
func (m *MockEngine) Delete(ctx context.Context, databaseName string, op client.TxnOperator) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Delete", ctx, databaseName, op)
	ret0, _ := ret[0].(error)
	return ret0
}

// Delete indicates an expected call of Delete.
func (mr *MockEngineMockRecorder) Delete(ctx, databaseName, op interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Delete", reflect.TypeOf((*MockEngine)(nil).Delete), ctx, databaseName, op)
}

// GetNameById mocks base method.
func (m *MockEngine) GetNameById(ctx context.Context, op client.TxnOperator, tableId uint64) (string, string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetNameById", ctx, op, tableId)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(error)
	return ret0, ret1, ret2
}

// GetNameById indicates an expected call of GetNameById.
func (mr *MockEngineMockRecorder) GetNameById(ctx, op, tableId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetNameById", reflect.TypeOf((*MockEngine)(nil).GetNameById), ctx, op, tableId)
}

// GetRelationById mocks base method.
func (m *MockEngine) GetRelationById(ctx context.Context, op client.TxnOperator, tableId uint64) (string, string, engine.Relation, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetRelationById", ctx, op, tableId)
	ret0, _ := ret[0].(string)
	ret1, _ := ret[1].(string)
	ret2, _ := ret[2].(engine.Relation)
	ret3, _ := ret[3].(error)
	return ret0, ret1, ret2, ret3
}

// GetRelationById indicates an expected call of GetRelationById.
func (mr *MockEngineMockRecorder) GetRelationById(ctx, op, tableId interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetRelationById", reflect.TypeOf((*MockEngine)(nil).GetRelationById), ctx, op, tableId)
}

// Hints mocks base method.
func (m *MockEngine) Hints() engine.Hints {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Hints")
	ret0, _ := ret[0].(engine.Hints)
	return ret0
}

// Hints indicates an expected call of Hints.
func (mr *MockEngineMockRecorder) Hints() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Hints", reflect.TypeOf((*MockEngine)(nil).Hints))
}

// New mocks base method.
func (m *MockEngine) New(ctx context.Context, op client.TxnOperator) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "New", ctx, op)
	ret0, _ := ret[0].(error)
	return ret0
}

// New indicates an expected call of New.
func (mr *MockEngineMockRecorder) New(ctx, op interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "New", reflect.TypeOf((*MockEngine)(nil).New), ctx, op)
}

// NewBlockReader mocks base method.
func (m *MockEngine) NewBlockReader(ctx context.Context, num int, ts timestamp.Timestamp, expr *plan.Expr, ranges [][]byte, tblDef *plan.TableDef) ([]engine.Reader, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "NewBlockReader", ctx, num, ts, expr, ranges, tblDef)
	ret0, _ := ret[0].([]engine.Reader)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// NewBlockReader indicates an expected call of NewBlockReader.
func (mr *MockEngineMockRecorder) NewBlockReader(ctx, num, ts, expr, ranges, tblDef interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "NewBlockReader", reflect.TypeOf((*MockEngine)(nil).NewBlockReader), ctx, num, ts, expr, ranges, tblDef)
}

// Nodes mocks base method.
func (m *MockEngine) Nodes(isInternal bool, tenant, username string, cnLabel map[string]string) (engine.Nodes, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Nodes", isInternal, tenant, username, cnLabel)
	ret0, _ := ret[0].(engine.Nodes)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Nodes indicates an expected call of Nodes.
func (mr *MockEngineMockRecorder) Nodes(isInternal, tenant, username, cnLabel interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nodes", reflect.TypeOf((*MockEngine)(nil).Nodes), isInternal, tenant, username, cnLabel)
}

// Rollback mocks base method.
func (m *MockEngine) Rollback(ctx context.Context, op client.TxnOperator) error {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Rollback", ctx, op)
	ret0, _ := ret[0].(error)
	return ret0
}

// Rollback indicates an expected call of Rollback.
func (mr *MockEngineMockRecorder) Rollback(ctx, op interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Rollback", reflect.TypeOf((*MockEngine)(nil).Rollback), ctx, op)
}

// MockVectorPool is a mock of VectorPool interface.
type MockVectorPool struct {
	ctrl     *gomock.Controller
	recorder *MockVectorPoolMockRecorder
}

// MockVectorPoolMockRecorder is the mock recorder for MockVectorPool.
type MockVectorPoolMockRecorder struct {
	mock *MockVectorPool
}

// NewMockVectorPool creates a new mock instance.
func NewMockVectorPool(ctrl *gomock.Controller) *MockVectorPool {
	mock := &MockVectorPool{ctrl: ctrl}
	mock.recorder = &MockVectorPoolMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockVectorPool) EXPECT() *MockVectorPoolMockRecorder {
	return m.recorder
}

// GetVector mocks base method.
func (m *MockVectorPool) GetVector(typ types.Type) *vector.Vector {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVector", typ)
	ret0, _ := ret[0].(*vector.Vector)
	return ret0
}

// GetVector indicates an expected call of GetVector.
func (mr *MockVectorPoolMockRecorder) GetVector(typ interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVector", reflect.TypeOf((*MockVectorPool)(nil).GetVector), typ)
}

// PutBatch mocks base method.
func (m *MockVectorPool) PutBatch(bat *batch.Batch) {
	m.ctrl.T.Helper()
	m.ctrl.Call(m, "PutBatch", bat)
}

// PutBatch indicates an expected call of PutBatch.
func (mr *MockVectorPoolMockRecorder) PutBatch(bat interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "PutBatch", reflect.TypeOf((*MockVectorPool)(nil).PutBatch), bat)
}

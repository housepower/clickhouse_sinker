// Code generated by MockGen. DO NOT EDIT.
// Source: ../vendor/github.com/confluentinc/confluent-kafka-go/schemaregistry/schemaregistry_client.go

// Package parser is a generated GoMock package.
package parser

import (
	reflect "reflect"

	schemaregistry "github.com/confluentinc/confluent-kafka-go/schemaregistry"
	gomock "github.com/golang/mock/gomock"
)

// MockSchemaRegistryClient is a mock of Client interface.
type MockSchemaRegistryClient struct {
	ctrl     *gomock.Controller
	recorder *MockSchemaRegistryClientMockRecorder
}

// MockSchemaRegistryClientMockRecorder is the mock recorder for MockSchemaRegistryClient.
type MockSchemaRegistryClientMockRecorder struct {
	mock *MockSchemaRegistryClient
}

// NewMockSchemaRegistryClient creates a new mock instance.
func NewMockSchemaRegistryClient(ctrl *gomock.Controller) *MockSchemaRegistryClient {
	mock := &MockSchemaRegistryClient{ctrl: ctrl}
	mock.recorder = &MockSchemaRegistryClientMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockSchemaRegistryClient) EXPECT() *MockSchemaRegistryClientMockRecorder {
	return m.recorder
}

// DeleteSubject mocks base method.
func (m *MockSchemaRegistryClient) DeleteSubject(subject string, permanent bool) ([]int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteSubject", subject, permanent)
	ret0, _ := ret[0].([]int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteSubject indicates an expected call of DeleteSubject.
func (mr *MockSchemaRegistryClientMockRecorder) DeleteSubject(subject, permanent interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteSubject", reflect.TypeOf((*MockSchemaRegistryClient)(nil).DeleteSubject), subject, permanent)
}

// DeleteSubjectVersion mocks base method.
func (m *MockSchemaRegistryClient) DeleteSubjectVersion(subject string, version int, permanent bool) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "DeleteSubjectVersion", subject, version, permanent)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// DeleteSubjectVersion indicates an expected call of DeleteSubjectVersion.
func (mr *MockSchemaRegistryClientMockRecorder) DeleteSubjectVersion(subject, version, permanent interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "DeleteSubjectVersion", reflect.TypeOf((*MockSchemaRegistryClient)(nil).DeleteSubjectVersion), subject, version, permanent)
}

// GetAllSubjects mocks base method.
func (m *MockSchemaRegistryClient) GetAllSubjects() ([]string, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllSubjects")
	ret0, _ := ret[0].([]string)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllSubjects indicates an expected call of GetAllSubjects.
func (mr *MockSchemaRegistryClientMockRecorder) GetAllSubjects() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllSubjects", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetAllSubjects))
}

// GetAllVersions mocks base method.
func (m *MockSchemaRegistryClient) GetAllVersions(subject string) ([]int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetAllVersions", subject)
	ret0, _ := ret[0].([]int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetAllVersions indicates an expected call of GetAllVersions.
func (mr *MockSchemaRegistryClientMockRecorder) GetAllVersions(subject interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetAllVersions", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetAllVersions), subject)
}

// GetBySubjectAndID mocks base method.
func (m *MockSchemaRegistryClient) GetBySubjectAndID(subject string, id int) (schemaregistry.SchemaInfo, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetBySubjectAndID", subject, id)
	ret0, _ := ret[0].(schemaregistry.SchemaInfo)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetBySubjectAndID indicates an expected call of GetBySubjectAndID.
func (mr *MockSchemaRegistryClientMockRecorder) GetBySubjectAndID(subject, id interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetBySubjectAndID", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetBySubjectAndID), subject, id)
}

// GetCompatibility mocks base method.
func (m *MockSchemaRegistryClient) GetCompatibility(subject string) (schemaregistry.Compatibility, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetCompatibility", subject)
	ret0, _ := ret[0].(schemaregistry.Compatibility)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetCompatibility indicates an expected call of GetCompatibility.
func (mr *MockSchemaRegistryClientMockRecorder) GetCompatibility(subject interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetCompatibility", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetCompatibility), subject)
}

// GetDefaultCompatibility mocks base method.
func (m *MockSchemaRegistryClient) GetDefaultCompatibility() (schemaregistry.Compatibility, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetDefaultCompatibility")
	ret0, _ := ret[0].(schemaregistry.Compatibility)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetDefaultCompatibility indicates an expected call of GetDefaultCompatibility.
func (mr *MockSchemaRegistryClientMockRecorder) GetDefaultCompatibility() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetDefaultCompatibility", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetDefaultCompatibility))
}

// GetID mocks base method.
func (m *MockSchemaRegistryClient) GetID(subject string, schema schemaregistry.SchemaInfo, normalize bool) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetID", subject, schema, normalize)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetID indicates an expected call of GetID.
func (mr *MockSchemaRegistryClientMockRecorder) GetID(subject, schema, normalize interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetID", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetID), subject, schema, normalize)
}

// GetLatestSchemaMetadata mocks base method.
func (m *MockSchemaRegistryClient) GetLatestSchemaMetadata(subject string) (schemaregistry.SchemaMetadata, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetLatestSchemaMetadata", subject)
	ret0, _ := ret[0].(schemaregistry.SchemaMetadata)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetLatestSchemaMetadata indicates an expected call of GetLatestSchemaMetadata.
func (mr *MockSchemaRegistryClientMockRecorder) GetLatestSchemaMetadata(subject interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetLatestSchemaMetadata", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetLatestSchemaMetadata), subject)
}

// GetSchemaMetadata mocks base method.
func (m *MockSchemaRegistryClient) GetSchemaMetadata(subject string, version int) (schemaregistry.SchemaMetadata, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetSchemaMetadata", subject, version)
	ret0, _ := ret[0].(schemaregistry.SchemaMetadata)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetSchemaMetadata indicates an expected call of GetSchemaMetadata.
func (mr *MockSchemaRegistryClientMockRecorder) GetSchemaMetadata(subject, version interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetSchemaMetadata", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetSchemaMetadata), subject, version)
}

// GetVersion mocks base method.
func (m *MockSchemaRegistryClient) GetVersion(subject string, schema schemaregistry.SchemaInfo, normalize bool) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "GetVersion", subject, schema, normalize)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// GetVersion indicates an expected call of GetVersion.
func (mr *MockSchemaRegistryClientMockRecorder) GetVersion(subject, schema, normalize interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "GetVersion", reflect.TypeOf((*MockSchemaRegistryClient)(nil).GetVersion), subject, schema, normalize)
}

// Register mocks base method.
func (m *MockSchemaRegistryClient) Register(subject string, schema schemaregistry.SchemaInfo, normalize bool) (int, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Register", subject, schema, normalize)
	ret0, _ := ret[0].(int)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Register indicates an expected call of Register.
func (mr *MockSchemaRegistryClientMockRecorder) Register(subject, schema, normalize interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Register", reflect.TypeOf((*MockSchemaRegistryClient)(nil).Register), subject, schema, normalize)
}

// TestCompatibility mocks base method.
func (m *MockSchemaRegistryClient) TestCompatibility(subject string, version int, schema schemaregistry.SchemaInfo) (bool, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "TestCompatibility", subject, version, schema)
	ret0, _ := ret[0].(bool)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// TestCompatibility indicates an expected call of TestCompatibility.
func (mr *MockSchemaRegistryClientMockRecorder) TestCompatibility(subject, version, schema interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "TestCompatibility", reflect.TypeOf((*MockSchemaRegistryClient)(nil).TestCompatibility), subject, version, schema)
}

// UpdateCompatibility mocks base method.
func (m *MockSchemaRegistryClient) UpdateCompatibility(subject string, update schemaregistry.Compatibility) (schemaregistry.Compatibility, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateCompatibility", subject, update)
	ret0, _ := ret[0].(schemaregistry.Compatibility)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateCompatibility indicates an expected call of UpdateCompatibility.
func (mr *MockSchemaRegistryClientMockRecorder) UpdateCompatibility(subject, update interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateCompatibility", reflect.TypeOf((*MockSchemaRegistryClient)(nil).UpdateCompatibility), subject, update)
}

// UpdateDefaultCompatibility mocks base method.
func (m *MockSchemaRegistryClient) UpdateDefaultCompatibility(update schemaregistry.Compatibility) (schemaregistry.Compatibility, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "UpdateDefaultCompatibility", update)
	ret0, _ := ret[0].(schemaregistry.Compatibility)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// UpdateDefaultCompatibility indicates an expected call of UpdateDefaultCompatibility.
func (mr *MockSchemaRegistryClientMockRecorder) UpdateDefaultCompatibility(update interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "UpdateDefaultCompatibility", reflect.TypeOf((*MockSchemaRegistryClient)(nil).UpdateDefaultCompatibility), update)
}

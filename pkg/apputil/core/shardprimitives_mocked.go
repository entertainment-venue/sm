package core

import (
	"github.com/entertainment-venue/sm/pkg/apputil/storage"
	"github.com/stretchr/testify/mock"
)

var (
	_ ShardPrimitives = new(MockedShardPrimitives)
)

type MockedShardPrimitives struct {
	mock.Mock
}

func (m *MockedShardPrimitives) Add(id string, spec *storage.ShardSpec) error {
	args := m.Called(id, spec)
	return args.Error(0)
}

func (m *MockedShardPrimitives) Drop(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

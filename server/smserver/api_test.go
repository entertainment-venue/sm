package smserver

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/entertainment-venue/sm/pkg/apputil"
	"github.com/entertainment-venue/sm/pkg/commonutil"
	"github.com/entertainment-venue/sm/pkg/etcdutil"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestApi(t *testing.T) {
	suite.Run(t, new(ApiTestSuite))
}

type ApiTestSuite struct {
	suite.Suite

	container  *smContainer
	testRouter *gin.Engine
	testServer *Server
}

func (suite *ApiTestSuite) SetupTest() {
	suite.testRouter = gin.Default()
	suite.testServer = &Server{}

	suite.container = &smContainer{
		lg:        lg,
		Container: &apputil.Container{},
		stopper:   &commonutil.GoroutineStopper{},
		shards:    make(map[string]Shard),

		nodeManager: &nodeManager{"foo"},
	}
	suite.container.SetService("foo")

	handlers := suite.container.getHttpHandlers()
	for path, handler := range handlers {
		suite.testRouter.Any(path, handler)
	}
}

func (suite *ApiTestSuite) TestGinAddSpec_requestError() {
	req := httptest.NewRequest(http.MethodPost, "/sm/server/add-spec", bytes.NewBuffer([]byte("foo")))
	req.Header.Add("Content-Type", "application/json")

	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinAddSpec_sameService() {
	spec := smAppSpec{
		Service:    "foo",
		CreateTime: time.Now().Unix(),
	}

	req := httptest.NewRequest(http.MethodPost, "/sm/server/add-spec", bytes.NewBuffer([]byte(spec.String())))
	req.Header.Add("Content-Type", "application/json")

	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)
	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinAddSpec_success() {
	// mock
	var nodes []string
	nodes = append(nodes, "/sm/app/foo/service/serviceA/spec")
	// TODO apputil.Container 中的service没有exported，所这块不能赋值
	nodes = append(nodes, "/sm/app/serviceA/lease/guard")
	nodes = append(nodes, "/sm/app/serviceA/containerhb/")
	nodes = append(nodes, "/sm/app/foo/service/foo/shard/serviceA")

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On(
		"CreateAndGet",
		mock.Anything,
		nodes,
		mock.Anything,
		clientv3.NoLease,
	).Return(nil)
	suite.container.Client = mockedEtcdWrapper

	spec := smAppSpec{
		Service:    "serviceA",
		CreateTime: time.Now().Unix(),
	}
	req := httptest.NewRequest(http.MethodPost, "/sm/server/add-spec", bytes.NewBuffer([]byte(spec.String())))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Equal(suite.T(), w.Code, http.StatusOK)
}

func (suite *ApiTestSuite) TestGinDelSpec_emptyService() {
	req := httptest.NewRequest(http.MethodGet, "/sm/server/del-spec", nil)
	w := httptest.NewRecorder()

	suite.testRouter.ServeHTTP(w, req)
	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinDelSpec_notFound() {
	req := httptest.NewRequest(http.MethodGet, "/sm/server/del-spec?service=foo", nil)
	w := httptest.NewRecorder()

	suite.testRouter.ServeHTTP(w, req)
	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinDelSpec_success() {
	service := "serviceA"
	pfxList := []string{
		"/sm/app/" + service + "/",
		"/sm/app/foo/service/" + service + "/spec",
		"/sm/app/foo/service/foo/shard/" + service,
	}

	// mock
	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("DelKVs", mock.Anything, pfxList).Return(nil)
	suite.container.Client = mockedEtcdWrapper

	req := httptest.NewRequest(http.MethodGet, "/sm/server/del-spec?service="+service, nil)
	w := httptest.NewRecorder()

	suite.testRouter.ServeHTTP(w, req)
	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Equal(suite.T(), w.Code, http.StatusOK)
}

func (suite *ApiTestSuite) TestGinGetSpec_success() {
	pfx := "/sm/app/foo/service/foo/shard/"

	// mock
	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("GetKVs", mock.Anything, pfx).Return(
		map[string]string{
			"1": "2",
		},
		nil,
	)
	suite.container.Client = mockedEtcdWrapper

	req := httptest.NewRequest(http.MethodGet, "/sm/server/get-spec", nil)
	w := httptest.NewRecorder()

	suite.testRouter.ServeHTTP(w, req)
	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Equal(suite.T(), w.Code, http.StatusOK)
}

func (suite *ApiTestSuite) TestGinAddShard_bindError() {
	req := httptest.NewRequest(http.MethodPost, "/sm/server/add-shard", bytes.NewBuffer([]byte("foo")))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinAddShard_sameService() {
	shardReq := addShardRequest{Service: "foo", ShardId: "shardA"}
	req := httptest.NewRequest(http.MethodPost, "/sm/server/add-shard", bytes.NewBuffer([]byte(shardReq.String())))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinAddShard_notFound() {
	shardReq := addShardRequest{Service: "serviceA", ShardId: "shardA"}

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("GetKV", mock.Anything, "/sm/app/foo/service/serviceA/spec", mock.Anything).Return(&clientv3.GetResponse{}, nil)
	suite.container.Client = mockedEtcdWrapper

	req := httptest.NewRequest(http.MethodPost, "/sm/server/add-shard", bytes.NewBuffer([]byte(shardReq.String())))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinAddShard_success() {
	shardReq := addShardRequest{Service: "serviceA", ShardId: "shardA"}
	pfx := fmt.Sprintf("/sm/app/foo/service/%s/shard/%s", shardReq.Service, shardReq.ShardId)
	suite.container.shards[shardReq.Service] = new(smShard)

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("GetKV", mock.Anything, "/sm/app/foo/service/serviceA/spec", mock.Anything).Return(&clientv3.GetResponse{Count: 1}, nil)
	mockedEtcdWrapper.On("CreateAndGet", mock.Anything, []string{pfx}, mock.Anything, clientv3.NoLease).Return(nil)
	suite.container.Client = mockedEtcdWrapper

	req := httptest.NewRequest(http.MethodPost, "/sm/server/add-shard", bytes.NewBuffer([]byte(shardReq.String())))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Equal(suite.T(), w.Code, http.StatusOK)
}

func (suite *ApiTestSuite) TestGinDelShard_bindError() {
	req := httptest.NewRequest(http.MethodPost, "/sm/server/del-shard", bytes.NewBuffer([]byte("foo")))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinDelShard_notFound() {
	service := "serviceA"
	shard := "shardA"
	pfx := fmt.Sprintf("/sm/app/foo/service/%s/shard/%s", service, shard)

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	delResp := clientv3.DeleteResponse{Deleted: 0}
	mockedEtcdWrapper.On("Delete", mock.Anything, pfx, mock.Anything).Return(&delResp, nil)
	suite.container.Client = mockedEtcdWrapper

	shardReq := addShardRequest{Service: "serviceA", ShardId: "shardA"}
	req := httptest.NewRequest(http.MethodPost, "/sm/server/del-shard", bytes.NewBuffer([]byte(shardReq.String())))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Equal(suite.T(), w.Code, http.StatusOK)
}

func (suite *ApiTestSuite) TestGinDelShard_success() {
	service := "serviceA"
	shard := "shardA"
	pfx := fmt.Sprintf("/sm/app/foo/service/%s/shard/%s", service, shard)
	suite.container.shards[service] = new(smShard)

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	delResp := clientv3.DeleteResponse{Deleted: 1}
	mockedEtcdWrapper.On("Delete", mock.Anything, pfx, mock.Anything).Return(&delResp, nil)
	suite.container.Client = mockedEtcdWrapper

	shardReq := addShardRequest{Service: service, ShardId: shard}
	req := httptest.NewRequest(http.MethodPost, "/sm/server/del-shard", bytes.NewBuffer([]byte(shardReq.String())))
	req.Header.Add("Content-Type", "application/json")
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)

	mockedEtcdWrapper.AssertExpectations(suite.T())
	assert.Equal(suite.T(), w.Code, http.StatusOK)
}

func (suite *ApiTestSuite) TestGinGetShard_emptyService() {
	req := httptest.NewRequest(http.MethodGet, "/sm/server/get-shard", nil)
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)
	assert.Equal(suite.T(), w.Code, http.StatusBadRequest)
}

func (suite *ApiTestSuite) TestGinGetShard_success() {
	service := "serviceA"
	pfx := fmt.Sprintf("/sm/app/foo/service/%s/shard/", service)

	mockedEtcdWrapper := new(etcdutil.MockedEtcdWrapper)
	mockedEtcdWrapper.On("GetKVs", mock.Anything, pfx).Return(map[string]string{"1": "2"}, nil)
	suite.container.Client = mockedEtcdWrapper

	req := httptest.NewRequest(http.MethodGet, "/sm/server/get-shard?service="+service, nil)
	w := httptest.NewRecorder()
	suite.testRouter.ServeHTTP(w, req)
	assert.Equal(suite.T(), w.Code, http.StatusOK)
}

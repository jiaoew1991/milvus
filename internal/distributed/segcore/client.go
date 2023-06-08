package segcore

import (
	"context"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/segcorepb"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"google.golang.org/grpc"
)

// Client is the grpc client of QueryNode.
type Client struct {
	grpcClient grpcclient.GrpcClient[segcorepb.SegcoreClient]
	addr       string
}

// NewClient creates a new QueryNode client.
func NewClient(addr string) *Client {
	clientParams := paramtable.Get().ProxyGrpcClientCfg
	client := &Client{
		addr: addr,
		grpcClient: &grpcclient.ClientBase[segcorepb.SegcoreClient]{
			ClientMaxRecvSize:      clientParams.ClientMaxRecvSize.GetAsInt(),
			ClientMaxSendSize:      clientParams.ClientMaxSendSize.GetAsInt(),
			DialTimeout:            clientParams.DialTimeout.GetAsDuration(time.Millisecond),
			KeepAliveTime:          clientParams.KeepAliveTime.GetAsDuration(time.Millisecond),
			KeepAliveTimeout:       clientParams.KeepAliveTimeout.GetAsDuration(time.Millisecond),
			RetryServiceNameConfig: "milvus.proto.query.QueryNode",
			MaxAttempts:            clientParams.MaxAttempts.GetAsInt(),
			InitialBackoff:         float32(clientParams.InitialBackoff.GetAsFloat()),
			MaxBackoff:             float32(clientParams.MaxBackoff.GetAsFloat()),
			BackoffMultiplier:      float32(clientParams.BackoffMultiplier.GetAsFloat()),
			CompressionEnabled:     clientParams.CompressionEnabled.GetAsBool(),
		},
	}
	client.grpcClient.SetRole("segcore")
	client.grpcClient.SetGetAddrFunc(client.getAddr)
	client.grpcClient.SetNewGrpcClientFunc(client.newGrpcClient)

	return client
}

func (c *Client) newGrpcClient(cc *grpc.ClientConn) segcorepb.SegcoreClient {
	return segcorepb.NewSegcoreClient(cc)
}

func (c *Client) getAddr() (string, error) {
	return c.addr, nil
}

func wrapGrpcCall[T any](ctx context.Context, c *Client, call func(grpcClient segcorepb.SegcoreClient) (*T, error)) (*T, error) {
	ret, err := c.grpcClient.ReCall(ctx, func(client segcorepb.SegcoreClient) (any, error) {
		if !funcutil.CheckCtxValid(ctx) {
			return nil, ctx.Err()
		}
		return call(client)
	})
	if err != nil || ret == nil {
		return nil, err
	}
	return ret.(*T), err
}

// GetComponentStates gets the component states of QueryNode.
func (c *Client) GetComponentStates(ctx context.Context) (*milvuspb.ComponentStates, error) {
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*milvuspb.ComponentStates, error) {
		return client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	})
}

func (c *Client) NewCollection(ctx context.Context, request *segcorepb.NewCollectionRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*commonpb.Status, error) {
		return client.NewCollection(ctx, request)
	})
}

func (c *Client) DeleteCollection(ctx context.Context, req *segcorepb.DeleteCollectionRequest) (*commonpb.Status, error) {
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*commonpb.Status, error) {
		return client.DeleteCollection(ctx, req)
	})
}

// LoadSegments loads the segments to search.
func (c *Client) LoadSegments(ctx context.Context, req *segcorepb.LoadSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*commonpb.Status, error) {
		return client.LoadSegments(ctx, req)
	})
}

// ReleaseSegments releases the data of the specified segments in QueryNode.
func (c *Client) ReleaseSegments(ctx context.Context, req *segcorepb.ReleaseSegmentsRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()))
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*commonpb.Status, error) {
		return client.ReleaseSegments(ctx, req)
	})
}

func (c *Client) SearchSegments(ctx context.Context, req *segcorepb.SearchRequest) (*internalpb.SearchResults, error) {
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*internalpb.SearchResults, error) {
		return client.SearchSegments(ctx, req)
	})
}

func (c *Client) QuerySegments(ctx context.Context, req *segcorepb.QueryRequest) (*internalpb.RetrieveResults, error) {
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*internalpb.RetrieveResults, error) {
		return client.QuerySegments(ctx, req)
	})
}

// Delete is used to forward delete message between delegator and workers.
func (c *Client) Delete(ctx context.Context, req *segcorepb.DeleteRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()),
	)
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*commonpb.Status, error) {
		return client.Delete(ctx, req)
	})
}

func (c *Client) Insert(ctx context.Context, req *segcorepb.InsertRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()),
	)
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*commonpb.Status, error) {
		return client.Insert(ctx, req)
	})
}

func (c *Client) UpdateSegmentIndex(ctx context.Context, req *segcorepb.UpdateIndexRequest) (*commonpb.Status, error) {
	req = typeutil.Clone(req)
	commonpbutil.UpdateMsgBase(
		req.GetBase(),
		commonpbutil.FillMsgBaseFromClient(paramtable.GetNodeID()),
	)
	return wrapGrpcCall(ctx, c, func(client segcorepb.SegcoreClient) (*commonpb.Status, error) {
		return client.UpdateSegmentIndex(ctx, req)
	})
}

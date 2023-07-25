package dependency

import (
	"context"

	smsgstream "github.com/milvus-io/milvus/internal/mq/msgstream"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// DefaultFactory is a factory that produces instances of storage.ChunkManager and message queue.
type DefaultFactory struct {
	standAlone          bool
	chunkManagerFactory storage.Factory
	msgStreamFactory    msgstream.Factory
}

// Only for test
func NewDefaultFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{
		standAlone:       standAlone,
		msgStreamFactory: smsgstream.NewRocksmqFactory("/tmp/milvus/rocksmq/", &paramtable.Get().ServiceParam),
		chunkManagerFactory: storage.NewChunkManagerFactory("local",
			storage.RootPath("/tmp/milvus")),
	}
}

// Only for test
func MockDefaultFactory(standAlone bool, params *paramtable.ComponentParam) *DefaultFactory {
	return &DefaultFactory{
		standAlone:          standAlone,
		msgStreamFactory:    smsgstream.NewRocksmqFactory("/tmp/milvus/rocksmq/", &paramtable.Get().ServiceParam),
		chunkManagerFactory: storage.NewChunkManagerFactoryWithParam(params),
	}
}

// NewFactory creates a new instance of the DefaultFactory type.
// If standAlone is true, the factory will operate in standalone mode.
func NewFactory(standAlone bool) *DefaultFactory {
	return &DefaultFactory{standAlone: standAlone}
}

// Init create a msg factory(TODO only support one mq at the same time.)
// In order to guarantee backward compatibility of config file, we still support multiple mq configs.
// The initialization of MQ follows the following rules, if the mq.type is default.
// 1. standalone(local) mode: rocksmq(default) > natsmq > Pulsar > Kafka
// 2. cluster mode:  Pulsar(default) > Kafka (rocksmq and natsmq is unsupported in cluster mode)
func (f *DefaultFactory) Init(params *paramtable.ComponentParam) {
	// skip if using default factory
	if f.msgStreamFactory != nil {
		return
	}

	f.chunkManagerFactory = storage.NewChunkManagerFactoryWithParam(params)

	// initialize mq client or embedded mq.
	if err := f.initMQ(f.standAlone, params); err != nil {
		panic(err)
	}
}

type Factory interface {
	msgstream.Factory
	Init(p *paramtable.ComponentParam)
	NewPersistentStorageChunkManager(ctx context.Context) (storage.ChunkManager, error)
}

/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package sync

import (
	"go.mongodb.org/mongo-driver/mongo/options"

	"mongo-sync/internal/config"
	"mongo-sync/internal/mongo"
)

type SyncMongoWrapper struct {
	syncMongoFactories []*SyncMongoFactory
}

func InitWrapper(config *config.Config) *SyncMongoWrapper {
	copClientOptions := options.Client().ApplyURI(config.CheckOutPointDbUrl)

	// Set client options
	mongo.CheckConnection(copClientOptions)

	syncMongoFactories := make([]*SyncMongoFactory, 0, len(config.SyncConfigs))
	for _, syncConfig := range config.SyncConfigs {
		// oplog 链接参数
		opLogClientOptions := options.Client().ApplyURI(syncConfig.OpLogRsUrl)
		mongo.CheckConnection(opLogClientOptions)

		syncMongoFactories = append(syncMongoFactories, initFactory(copClientOptions, opLogClientOptions, syncConfig))
	}
	return &SyncMongoWrapper{syncMongoFactories: syncMongoFactories}
}

func (s *SyncMongoWrapper) Wrapper() {
	for _, wrapper := range s.syncMongoFactories {
		go wrapper.sync()
	}
}

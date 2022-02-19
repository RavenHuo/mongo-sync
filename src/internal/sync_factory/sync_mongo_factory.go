/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package sync_factory

import (
	"mongo-sync/internal/config"
)

type syncMongo interface {
	sync()
}

type SyncMongoFactory struct {
	engineList []syncMongo
}

func InitFactory(config *config.Config) *SyncMongoFactory {
	return &SyncMongoFactory{}
}

func (s *SyncMongoFactory) DoSync() {
	for _, engine := range s.engineList {
		engine.sync()
	}
}

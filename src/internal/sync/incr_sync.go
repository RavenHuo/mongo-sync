/**
 * @Author raven
 * @Description
 * @Date 2022/2/25
 **/
package sync

import (
	"github.com/sirupsen/logrus"

	"mongo-sync/internal/mongo/model"
)

func (s *SyncMongoFactory) IncrSyncDataConsumer(incrSyncMongoDocChan chan model.MongoDoc) {
	logrus.Infof("start sync increment data...")

	for {
		select {
		case mongoDoc := <-incrSyncMongoDocChan:
			s.consumerFactory.IncrDataConsumer(&mongoDoc)
		}
	}

}

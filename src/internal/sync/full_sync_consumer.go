/**
 * @Author raven
 * @Description
 * @Date 2022/2/22
 **/
package sync

import (
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

func (s *SyncMongoFactory) FullSyncDataConsumer(fullSyncMongoDocChan chan map[string]interface{}, fullSyncGoroutineWg *sync.WaitGroup, goRoNum int) {
	_now := time.Now()
	defer fullSyncGoroutineWg.Done()
	defer logrus.Infof("sync full data goroutine number: %d exit,  cost:%d", goRoNum, time.Now().Sub(_now).Nanoseconds())
	logrus.Infof("sync full data goroutine number: %d start", goRoNum)

	for {
		select {
		case mongoDoc, ok := <-fullSyncMongoDocChan:
			if ok {
				s.consumerFactory.FullDataConsumer(mongoDoc)
			} else {
				return
			}
		}

	}
}

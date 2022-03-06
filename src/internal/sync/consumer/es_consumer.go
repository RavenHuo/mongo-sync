/**
 * @Author raven
 * @Description
 * @Date 2022/2/27
 **/
package consumer

import (
	"context"
	"sync"
	"time"

	"github.com/olivere/elastic/v7"
	"github.com/sirupsen/logrus"

	"mongo-sync/internal/config"
	"mongo-sync/internal/mongo/model"
)

const (
	ElasticMaxRetryOnConflict  = 5    // elastic出现版本冲突时（乐观锁）的最大重试次数
	ElasticBatchInsertMaxCount = 2000 // elastic批量插入最大条数
	ElasticTimeout             = "1s" // ES操作超时时间
)

type esSyncMongoConsumer struct {
	Url   string
	Index string
}

func initEsSyncMongoConsumer(config *config.SyncEs) *esSyncMongoConsumer {
	return &esSyncMongoConsumer{
		Url:   config.Url,
		Index: config.Index,
	}
}

func (s *esSyncMongoConsumer) getName() string {
	return EsSyncMongoConsumer
}
func (s *esSyncMongoConsumer) isFullData() bool {
	return true
}
func (s *esSyncMongoConsumer) consumer(mongoDoc *model.MongoDoc) {
	esClient, err := elastic.NewClient(elastic.SetSniff(false),
		elastic.SetURL(s.Url))
	if err != nil {
		logrus.Errorf("elastic.NewClient err:%v,urls:%+v", err, s.Url)
		return
	}
	bulk := esClient.Bulk()
	bulks := make([]elastic.BulkableRequest, 0)
	bulksLock := sync.Mutex{}
	go func() {
		for {
			select {
			// 每3秒同步一次
			case <-time.After(time.Second * 3):
				if len(bulks) == 0 {
					continue
				}
				bulksLock.Lock()
				bulk.Add(bulks...)
				bulkResponse, err := bulk.Do(context.Background())
				if err != nil {
					logrus.Infof("batch processing, bulk do err:%v count:%d\n", err, len(bulks))
					bulksLock.Unlock()
					continue
				}
				for _, v := range bulkResponse.Failed() {
					logrus.Errorf("index: %s, type: %s, _id: %s, error: %+v", v.Index, v.Type, v.Id, *v.Error)
				}
				logrus.Infof("sync incr data successCount:%d, faildCount:%d", len(bulks)-len(bulkResponse.Failed()), len(bulkResponse.Failed()))
				bulk.Reset()
				bulks = make([]elastic.BulkableRequest, 0)
				bulksLock.Unlock()
			}
		}
	}()

	switch mongoDoc.Op {
	case model.OperationInsert:
		for {
			if len(bulks) >= ElasticBatchInsertMaxCount {
				logrus.Warnf("listenSyncIncrData batch insert count too much,bulks count:%d", len(bulks))
				time.Sleep(time.Second)
			} else {
				break
			}
		}
		bulksLock.Lock()
		doc := elastic.NewBulkIndexRequest().Index(s.Url).Id(mongoDoc.ID).Doc(mongoDoc.Doc).RetryOnConflict(ElasticMaxRetryOnConflict)
		bulks = append(bulks, doc)
		bulksLock.Unlock()
	case model.OperationUpdate:
		bulksLock.Lock()
		doc := elastic.NewBulkUpdateRequest().Index(s.Url).Id(mongoDoc.ID).Doc(mongoDoc.Doc).RetryOnConflict(ElasticMaxRetryOnConflict)
		bulks = append(bulks, doc)
		bulksLock.Unlock()
	case model.OperationDelete:
		bulksLock.Lock()
		doc := elastic.NewBulkDeleteRequest().Index(s.Url).Id(mongoDoc.ID)
		bulks = append(bulks, doc)
		bulksLock.Unlock()
	}
}

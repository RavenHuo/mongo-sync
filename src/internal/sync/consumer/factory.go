/**
 * @Author raven
 * @Description
 * @Date 2022/2/27
 **/
package consumer

import (
	"mongo-sync/internal/config"
	"mongo-sync/internal/mongo/model"
)

const (
	EsSyncMongoConsumer    = "EsSyncMongoConsumer"
	KafkaSyncMongoConsumer = "KafkaSyncMongoConsumer"
)

type syncMongoConsumer interface {
	consumer(*model.MongoDoc)
	getName() string
	isFullData() bool
}

type MongoConsumerFactory struct {
	consumerList []syncMongoConsumer
}

func BuildConsumerFactory(option *config.SyncOption) *MongoConsumerFactory {
	consumerList := make([]syncMongoConsumer, 0)
	if len(option.ElasticSearch.Url) > 0 && len(option.ElasticSearch.Index) > 0 {
		consumerList = append(consumerList, initEsSyncMongoConsumer(&option.ElasticSearch))
	}
	return &MongoConsumerFactory{
		consumerList: consumerList,
	}
}

func (m *MongoConsumerFactory) FullDataConsumer(doc map[string]interface{}) {
	mongoDoc := &model.MongoDoc{
		Op:  model.OperationInsert,
		Doc: doc,
	}
	for _, consumer := range m.consumerList {
		if consumer.isFullData() {
			consumer.consumer(mongoDoc)
		}
	}
}

func (m *MongoConsumerFactory) IncrDataConsumer(doc *model.MongoDoc) {
	for _, consumer := range m.consumerList {
		consumer.consumer(doc)
	}
}

/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package sync

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"mongo-sync/internal/config"
	"mongo-sync/internal/constant"
	mongo2 "mongo-sync/internal/mongo"
	"mongo-sync/internal/mongo/model"
	"mongo-sync/internal/sync/consumer"
)

type SyncMongoFactory struct {
	copClientOptions   *options.ClientOptions
	opLogClientOptions *options.ClientOptions
	config             config.MongoSyncConfig
	consumerFactory    *consumer.MongoConsumerFactory
}

func initFactory(copClientOptions, opLogClientOptions *options.ClientOptions, config config.MongoSyncConfig) *SyncMongoFactory {
	return &SyncMongoFactory{
		copClientOptions:   copClientOptions,
		opLogClientOptions: opLogClientOptions,
		config:             config,
		consumerFactory:    consumer.BuildConsumerFactory(&config.Option),
	}
}

func (s *SyncMongoFactory) sync() {
	logrus.Infof("start Sync es copClientOptions:%+v, opLogClientOptions:%+v, config: %+v", s.copClientOptions, s.opLogClientOptions, s.config)

	// Connect to MongoDB
	client, err := mongo.Connect(context.Background(), s.copClientOptions)
	defer client.Disconnect(context.Background())
	if err != nil {
		logrus.Error("check_out_point db mongo.Connect err:%s", err.Error())
		return
	}
	// 数据库，集合
	collection := client.Database(constant.CheckOutPointDbName).Collection(constant.CheckOutPointColName)

	var checkOutPointModel model.CheckoutPoint
	filter := bson.M{"op_log_rs_url": s.config.OpLogRsUrl, "option": s.config.Option, "need_sync_db_col": s.config.DbCol}
	err = collection.FindOne(context.Background(), filter).Decode(&checkOutPointModel)
	if err != nil && err != mongo.ErrNoDocuments {
		logrus.Errorf("check_out_point query err :%s", err.Error())
		return
	}
	// 全量同步
	if err == mongo.ErrNoDocuments {
		// 2.1 在全量同步前获取mongodb最新的oplog.rs记录
		opLogRs, err := mongo2.GetNewestOpLogRs(s.opLogClientOptions)
		if err != nil {
			logrus.Errorf("GetNewestOpLogRs  err :%s", err.Error())
			return
		}
		// 全量同步数据
		err = s.startFullSync()
		if err != nil {
			logrus.Errorf("startFullSync  err:%s, config:%+v", err.Error(), s)
			return
		}
		if err = mongo2.InsertCheckOutPoint(s.copClientOptions, opLogRs.Timestamp, s.config.DbName, s.config.DbCol, s.config.OpLogRsUrl, &s.config.Option); err != nil {
			logrus.Errorf("full sync InsertCheckOutPoint %+v, err :%s", s, err.Error())
			return
		}
	}

	// 开始增量同步
	logrus.Infof("start sync increment dat congig:+%v", s)
	checkOutPoint, err := mongo2.FindOneCheckOutPoint(s.copClientOptions, s.config.DbName, s.config.DbCol, s.config.OpLogRsUrl, &s.config.Option)
	if err != nil {
		logrus.Errorf("find checkOutPoint dbName:%s, dbCol:%s, config:%+v, err:%s", s.config.DbName, s.config.DbCol, s.config.Option, err.Error())
		return
	}

	currentOpLogTimestamp := checkOutPoint.CheckoutPointTs
	incrSyncMongoDocChan := make(chan model.MongoDoc, constant.IncrSyncBufferedChannelMaxSize)
	defer close(incrSyncMongoDocChan)
	// 增量消费
	go s.IncrSyncDataConsumer(incrSyncMongoDocChan)

	//  记录断点
	go s.tickerUpdateCheckOutPoint(&currentOpLogTimestamp)

	opName := s.config.DbName + "." + s.config.DbCol

	for {
		query := bson.M{
			"ts":          bson.M{"$gt": currentOpLogTimestamp},
			"op":          bson.M{"$in": model.OpCodes},
			"ns":          opName,
			"fromMigrate": bson.M{"$exists": false},
		}
		// 3.3 流式拉取oplog.rs表，进行日志重放
		oplogCurr, err := mongo2.GetLogRsIterWithFilter(s.opLogClientOptions, query)
		if err != nil {
			logrus.Errorf("GetNewestOpLogRs clientOption:%+v err :%s", s.opLogClientOptions, err.Error())
		}
		oplogCurr.Close(context.Background())
		for {
			var opLogRs model.OpLogRsModel
			if ok := oplogCurr.Next(context.Background()); ok {
				doc := getMongoDocByOpLogRs(opLogRs)
				logrus.Infof("incr sync change doc from oplog,id:%s,option:%+v,opLogRs:%+v", doc.ID, s.config.Option, opLogRs)
				incrSyncMongoDocChan <- doc
				currentOpLogTimestamp = opLogRs.Timestamp
			} else {
				break
			}
		}
	}
}

func (s *SyncMongoFactory) startFullSync() error {
	startTime := time.Now()
	cur, err := mongo2.GetFullDataIter(s.opLogClientOptions, s.config.DbName, s.config.DbCol)
	if err != nil {
		logrus.Errorf("startFullSync err : %s", err.Error())
		return err
	}
	defer cur.Close(context.Background())

	// 创建带缓存channel，由mongo生产数据,工厂消费
	fullSyncMongoDocChan := make(chan map[string]interface{}, constant.FullSyncBufferedChannelMaxSize)
	fullSyncGoroutineWG := sync.WaitGroup{}

	for goRoNum := 0; goRoNum < config.ApplicationConfig.FullSyncGoroutineCount; goRoNum++ {
		fullSyncGoroutineWG.Add(1)
		// 启动协程消费
		go s.FullSyncDataConsumer(fullSyncMongoDocChan, &fullSyncGoroutineWG, goRoNum)
	}

	for cur.Next(context.Background()) {
		// mongodb文档
		mongoDoc := make(map[string]interface{})
		if err := cur.Decode(&mongoDoc); err != nil {
			break
		}
		fullSyncMongoDocChan <- mongoDoc
	}

	close(fullSyncMongoDocChan)
	err = cur.Err()
	if err != nil {
		logrus.Errorf("sync full data fail,err:%v ", err)
		return err
	}
	fullSyncGoroutineWG.Wait()
	logrus.Infof("full sync data success cost:%dms, dbName:%s", time.Now().Sub(startTime).Nanoseconds()/1e6, s.config.DbName)
	return nil
}

// 记录同步位点到mongodb，用于断点继传
func (s *SyncMongoFactory) tickerUpdateCheckOutPoint(currentOpLogTimestamp *primitive.Timestamp) {
	for {
		select {
		// 30s更新一次最新的同步位点到mongo
		case <-time.After(time.Second * 30):
			err := mongo2.UpdateCheckOutPoint(s.copClientOptions, *currentOpLogTimestamp, s.config.DbName, s.config.OpLogRsUrl, s.config.DbCol, &s.config.Option)
			if err != nil {
				logrus.Errorf("checkoutPoint update fail, config:%+v, checkoutpoint:%+v", s, currentOpLogTimestamp)
			} else {
				logrus.Infof("checkoutPoint update success, config:%+v, checkoutpoint:%+v", s, currentOpLogTimestamp)
			}
		}
	}
}

func getMongoDocByOpLogRs(oplog model.OpLogRsModel) model.MongoDoc {
	id := getDocIdByOpLogRs(oplog)
	mongoDoc := model.MongoDoc{
		ID: id,
	}
	switch oplog.Operation {
	case model.OperationInsert:
		delete(oplog.Doc, "_id")
		mongoDoc.Op = model.OperationInsert
		mongoDoc.Doc = oplog.Doc
	case model.OperationDelete:
		mongoDoc.Op = model.OperationDelete
	case model.OperationUpdate:
		delete(oplog.Doc, "_id")
		mongoDoc.Op = model.OperationUpdate
		mongoDoc.Doc = oplog.Doc
	default:
		logrus.Errorf("getMongoDocByOpLogRs err,oplog.Operation:%s, oplog:%+v", oplog.Operation, oplog)
	}
	return mongoDoc
}

func getDocIdByOpLogRs(oplog model.OpLogRsModel) string {
	var id string
	switch oplog.Operation {
	case model.OperationInsert, model.OperationDelete:
		idTypeOf := reflect.TypeOf(oplog.Doc["_id"])
		switch idTypeOf.String() {
		case "bson.ObjectId":
			id = oplog.Doc["_id"].(primitive.ObjectID).Hex()
		case "string":
			id = oplog.Doc["_id"].(string)
		default:
			logrus.Errorf("getDocIdByTypeOf err,typeOf illegal:%v,oplog:%+v", idTypeOf, oplog)
		}
	case model.OperationUpdate:
		idTypeOf := reflect.TypeOf(oplog.Update["_id"])
		switch idTypeOf.String() {
		case "bson.ObjectId":
			id = oplog.Update["_id"].(primitive.ObjectID).Hex()
		case "string":
			id = oplog.Update["_id"].(string)
		default:
			logrus.Errorf("getDocIdByTypeOf err,typeOf illegal:%v,oplog:%+v", idTypeOf, oplog)
		}
	}
	return id
}

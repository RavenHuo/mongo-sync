/**
 * @Author raven
 * @Description
 * @Date 2022/2/22
 **/
package mongo

import (
	"context"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"mongo-sync/internal/config"
	"mongo-sync/internal/constant"
	"mongo-sync/internal/mongo/model"
)

// GetFullDataIter 查询dbName.dbCol 的所有数据
func GetFullDataIter(opLogRsOption *options.ClientOptions, dbName, dbCol string) (*mongo.Cursor, error) {
	client, err := mongo.Connect(context.Background(), opLogRsOption)
	if err != nil {
		return nil, err
	}
	collection := client.Database(dbName, nil).Collection(dbCol, nil)
	return collection.Find(context.Background(), bson.M{})
}

func GetLogRsIterWithFilter(opLogRsOption *options.ClientOptions, filter bson.M) (*mongo.Cursor, error) {
	opLogClient, err := mongo.Connect(context.Background(), opLogRsOption)
	if err != nil {
		logrus.Errorf("getClient  clientOption:%+v  connect err :=%s", opLogClient, err)
		return nil, err
	}
	connection := opLogClient.Database(constant.OpLogRsDbName, nil).Collection(constant.OpLogRsColName, nil)
	opt := options.Find().SetSort(bson.D{{"$natural", -1}})
	return connection.Find(context.Background(), filter, opt)
}

func CheckConnection(clientOption *options.ClientOptions) {
	client, err := mongo.Connect(context.Background(), clientOption)
	// Check the connection
	defer client.Disconnect(context.Background())
	if err != nil {
		logrus.Errorf("getClient  clientOption:%+v  connect err :=%s", clientOption, err)
		panic("getClient err" + err.Error())
	}
	timeOutCtx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	err = client.Ping(timeOutCtx, nil)

	if err != nil {
		fmt.Println("connection ping error ---------" + err.Error())
		panic("connection ping err " + err.Error())
	}
}

// 获取最新的oplog
func GetNewestOpLogRs(clientOption *options.ClientOptions) (*model.OpLogRsModel, error) {
	client, err := mongo.Connect(context.Background(), clientOption)
	if err != nil {
		logrus.Errorf("getClient  clientOption:%+v  connect err :=%s", clientOption, err)
		return nil, err
	}
	var opLogRs model.OpLogRsModel
	connection := client.Database(constant.OpLogRsDbName, nil).Collection(constant.OpLogRsColName, nil)
	opt := options.FindOne().SetSort(bson.D{{"$natural", -1}})
	err = connection.FindOne(context.Background(), ValidOps(), opt).Decode(&opLogRs)
	if err != nil {
		logrus.Errorf("GetNewestOpLogRs clientOption:%+v err :%s", clientOption, err.Error())
	}
	return &opLogRs, err
}

// mongodb对文档的增删改操作
func ValidOps() bson.M {
	return bson.M{"op": bson.M{"$in": model.OpCodes}}
}

func InsertCheckOutPoint(clientOption *options.ClientOptions, ts primitive.Timestamp, dbName, dbCol, opLogUrl string, option *config.SyncOption) error {
	client, err := mongo.Connect(context.Background(), clientOption)
	defer client.Disconnect(context.Background())
	if err != nil {
		logrus.Errorf("getClient  clientOption:%+v  connect err :=%s", clientOption, err)
		return err
	}
	connection := client.Database(constant.CheckOutPointDbName, nil).Collection(constant.CheckOutPointColName, nil)
	checkOutPointModel := model.CheckoutPoint{
		NeedSyncDbName:  dbName,
		NeedSyncDbCol:   dbCol,
		Option:          option,
		CheckoutPointTs: ts,
		OpLogRsUrl:      opLogUrl,
	}
	_, err = connection.InsertOne(context.Background(), &checkOutPointModel)
	return err
}

func FindOneCheckOutPoint(clientOption *options.ClientOptions, dbName, dbCol, opLogUrl string, option *config.SyncOption) (*model.CheckoutPoint, error) {
	client, err := mongo.Connect(context.Background(), clientOption)
	defer client.Disconnect(context.Background())
	if err != nil {
		logrus.Errorf("getClient  clientOption:%+v  connect err :=%s", clientOption, err)
		return nil, err
	}
	connection := client.Database(constant.CheckOutPointDbName, nil).Collection(constant.CheckOutPointColName, nil)
	filter := bson.M{
		"option":            option,
		"need_sync_db_col":  dbCol,
		"need_sync_db_name": dbName,
		"op_log_rs_url":     opLogUrl,
	}

	var checkOutPointModel model.CheckoutPoint

	err = connection.FindOne(context.Background(), filter).Decode(&checkOutPointModel)
	return &checkOutPointModel, err
}

func UpdateCheckOutPoint(clientOption *options.ClientOptions, ts primitive.Timestamp, dbName, dbCol, opLogUrl string, option *config.SyncOption) error {
	client, err := mongo.Connect(context.Background(), clientOption)
	defer client.Disconnect(context.Background())
	if err != nil {
		logrus.Errorf("getClient  clientOption:%+v  connect err :=%s", clientOption, err)
		return err
	}
	connection := client.Database(constant.CheckOutPointDbName, nil).Collection(constant.CheckOutPointColName, nil)
	filter := bson.M{
		"option":            option,
		"need_sync_db_col":  dbCol,
		"need_sync_db_name": dbName,
		"op_log_rs_url":     opLogUrl,
	}

	update := bson.M{"$set": bson.M{"checkout_point_ts": ts}}
	_, err = connection.UpdateOne(context.Background(), filter, update)
	if err != nil {
		logrus.Errorf("update check_out_point err:%v,where:%+v,update:%+v", err, filter, update)
	}
	return err
}

package model

import (
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"mongo-sync/internal/constant"
)

var OpCodes = [...]string{"c", "i", "u", "d"}

const (
	OperationInsert = "i"
	OperationDelete = "d"
	OperationUpdate = "u"
)

type MongoDoc struct {
	Op  string
	ID  string
	Doc map[string]interface{}
}

type OpLogRsModel struct {
	// 操作时间戳
	Timestamp    primitive.Timestamp `bson:"ts"`
	HistoryID    float64             `bson:"h"`
	MongoVersion int                 `bson:"v"`
	// 操作类型 i,d,u
	Operation string `bson:"op"`
	// 库名
	Namespace string `bson:"ns"`
	// 操作内容
	Doc bson.M `bson:"o"`
	// 更新时的id
	Update bson.M `bson:"o2"`
}

func (c OpLogRsModel) GetInfo() MongoInfo {
	return MongoInfo{
		DBName:  constant.OpLogRsDbName,
		ColName: constant.OpLogRsColName,
	}
}

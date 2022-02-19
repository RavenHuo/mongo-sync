/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package model

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
)

const CheckOutPointCollectionName = "check_out_point"

//mongodb同步到es同步状态标记
type CheckoutPoint struct {
	MongoUrl        string             `json:"mongo_url" bson:"mongo_url"`
	ElasticUrl      string             `json:"elastic_url" bson:"elastic_url"`
	NeedSyncDbCol   string             `json:"need_sync_db_col" bson:"need_sync_db_col"`
	CheckoutPointTs primitive.DateTime `json:"checkout_point_ts" bson:"checkout_point_ts"`
}

func (c CheckoutPoint) GetInfo() ModelInfo {
	return ModelInfo{
		DBName:  CheckOutPointDbName,
		ColName: CheckOutPointColName,
	}
}

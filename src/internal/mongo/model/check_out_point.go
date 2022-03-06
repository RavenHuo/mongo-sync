/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package model

import (
	"go.mongodb.org/mongo-driver/bson/primitive"

	"mongo-sync/internal/config"
	"mongo-sync/internal/constant"
)

//mongodb同步到es同步状态标记
type CheckoutPoint struct {
	OpLogRsUrl      string              `bson:"op_log_rs_url"`
	NeedSyncDbCol   string              `bson:"need_sync_db_col"`
	NeedSyncDbName  string              `bson:"need_sync_db_name"`
	CheckoutPointTs primitive.Timestamp `bson:"checkout_point_ts"`
	Option          *config.SyncOption  `bson:"option"`
}

func (c CheckoutPoint) GetInfo() MongoInfo {
	return MongoInfo{
		DBName:  constant.CheckOutPointDbName,
		ColName: constant.CheckOutPointColName,
	}
}

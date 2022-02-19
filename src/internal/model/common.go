/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package model

const CheckOutPointDbName = "checkOutPoint"
const CheckOutPointColName = "check_out_point"
const OpLogRsDbName = "local"
const OpLogRsColName = "oplog.rs"

type ModelInfo struct {
	DBName  string
	ColName string
}
type MongoModel interface {
	GetInfo() ModelInfo
}

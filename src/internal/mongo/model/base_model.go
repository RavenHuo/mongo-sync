/**
 * @Author raven
 * @Description
 * @Date 2022/3/5
 **/
package model

type MongoInfo struct {
	DBName  string
	ColName string
}
type MongoModel interface {
	GetInfo() MongoInfo
}

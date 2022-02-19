/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package config

type MongoSyncEsConfig struct {
	// ES url
	EsUrl string
	// mongo 文档同步到es的索引
	EsIndex string
	// mongo文档名称
	DbCol string
	// 是否需要包装bsonId
	WrapBsonId bool
	// OpLogRs的地址
	OpLogRsUrl string
}

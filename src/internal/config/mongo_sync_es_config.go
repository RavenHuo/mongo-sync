/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package config

type MongoSyncEsConfig struct {
	EsUrl      string
	EsIndex    string
	DbCol      string
	WrapBsonId bool
}

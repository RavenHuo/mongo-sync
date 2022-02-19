/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package config

type Config struct {
	Mongo        MongoConfig
	EsSyncConfig []MongoSyncEsConfig
}

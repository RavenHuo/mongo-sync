/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package config

var ApplicationConfig Config

type Config struct {
	Port                   int               `yaml:"port"`                      // 端口
	CheckOutPointDbUrl     string            `yaml:"check_out_point_db_url"`    // check_out_point db地址
	FullSyncGoroutineCount int               `yaml:"full_sync_goroutine_count"` // 全量数据同步时 协程数控制
	SyncConfigs            []MongoSyncConfig `yaml:"sync_configs"`              // 同步es配置
}

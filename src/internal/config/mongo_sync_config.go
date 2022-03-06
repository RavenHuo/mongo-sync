/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package config

type MongoSyncConfig struct {
	DbCol      string     `yaml:"db_col"`        // mongo文档名称
	DbName     string     `yaml:"db_name"`       // mongo 数据库
	WrapBsonId bool       `yaml:"wrap_bson_id"`  // 是否需要包装bsonId
	OpLogRsUrl string     `yaml:"op_log_rs_url"` // OpLogRs的地址
	Option     SyncOption `yaml:"option"`
}

type SyncOption struct {
	ElasticSearch SyncEs    `bson:"elastic_search,omitempty" yaml:"elastic_search"` // 同步ES
	Kafka         SyncKafka `bson:"kafka,omitempty" yaml:"kafka"`                   // 同步kafka
}

type SyncEs struct {
	Url   string `bson:"url,omitempty" yaml:"url"`     // ES url
	Index string `bson:"index,omitempty" yaml:"index"` // mongo 文档同步到es的索引
}

type SyncKafka struct {
	Url   string `bson:"url,omitempty"`
	Topic string `bson:"topic,omitempty"`
}

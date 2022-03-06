/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package main

import (
	"net/http"
	"strconv"

	"github.com/jinzhu/configor"
	"github.com/sirupsen/logrus"

	"mongo-sync/internal/config"
	"mongo-sync/internal/sync"
)

func main() {
	if err := configor.Load(&config.ApplicationConfig, "etc/config.yml"); err != nil {
		panic("load mongo config panic err:" + err.Error())
	}
	logrus.Infof("config %+v", config.ApplicationConfig)

	sync.InitWrapper(&config.ApplicationConfig).Wrapper()

	err := http.ListenAndServe("localhost:"+strconv.Itoa(config.ApplicationConfig.Port), nil)

	if err != nil {
		logrus.Infof("ListenAndServe: %s", err.Error())
		panic("ListenAndServe: " + err.Error())
	}

}

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
	"mongo-sync/internal/sync_factory"
)

var applicationConfig config.Config

func main() {
	if err := configor.Load(&applicationConfig, "etc/config.yml"); err != nil {
		panic("load mongo config panic err:" + err.Error())
	}
	logrus.Infof("config %+v", applicationConfig)

	err := http.ListenAndServe(":"+strconv.Itoa(applicationConfig.Port), nil)

	if err != nil {
		logrus.Infof("ListenAndServe: %s", err.Error())
		panic("ListenAndServe: " + err.Error())
	}

	sync_factory.InitFactory(&applicationConfig).DoSync()
}

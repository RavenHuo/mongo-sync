/**
 * @Author raven
 * @Description
 * @Date 2022/2/19
 **/
package main

import (
	"fmt"

	"github.com/jinzhu/configor"

	"mongo-sync/internal/config"
)

var applicationConfig config.Config

func main() {
	if err := configor.Load(&applicationConfig, "etc/config.yml"); err != nil {
		panic("load mongo config panic err:" + err.Error())
	}
	fmt.Print(applicationConfig)
}

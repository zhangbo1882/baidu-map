package main

import (
	controller "github.com/zhangbo1882/baidu-map/pkg"
)

func main() {
	m := controller.NewMapController()
	m.Run()
}

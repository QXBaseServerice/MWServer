package main

import (
	"fmt"
	"os"
	"runtime"

	"github.com/arsgo/ars/servers/config"
	"github.com/arsgo/lib4go/forever"
)

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	conf, err := config.Get()
	if err != nil {
		fmt.Println("获取配置文件失败:", err)
	}
	//fmt.Println("conf", conf)
	rcServer, err := NewMWServer(conf)
	if err != nil {
		os.Exit(100)
		return
	}
	f := forever.NewForever(rcServer, rcServer.Log, "mwserver", "mwserver")
	f.Start()
}

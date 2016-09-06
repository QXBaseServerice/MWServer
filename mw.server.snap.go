package main

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/arsgo/ars/snap"
	"github.com/arsgo/lib4go/sysinfo"
)

//MWSnap RC server快照信息
type MWSnap struct {
	mwServer *MWServer
	Domain   string `json:"domain"`
	path     string
	Address  string      `json:"address"`
	Server   string      `json:"server"`
	Refresh  int         `json:"refresh"`
	Version  string      `json:"version"`
	CPU      string      `json:"cpu"`
	Mem      string      `json:"mem"`
	Disk     string      `json:"disk"`
	Last     string      `json:"last"`
	Snap     interface{} `json:"snap"`
	Cache    interface{} `json:"cache"`
}

//GetServicesSnap 获取RC服务的快照信息
func (rs MWSnap) GetServicesSnap(services map[string]interface{}) string {
	snap := rs
	snap.Last = time.Now().Format("20060102150405")
	snap.CPU = sysinfo.GetAvaliabeCPU().Used
	snap.Mem = sysinfo.GetAvaliabeMem().Used
	snap.Disk = sysinfo.GetAvaliabeDisk().Used

	/*rpcs := rs.mwServer.rpcServerCollector.Get()
	if len(rpcs) > 0 {
		services["rpc"] = rpcs
	}*/
	/*schedulers := rs.mwServer.schedulerCollector.Get()
	if len(schedulers) > 0 {
		services["jobs"] = schedulers
	}*/
	cache := make(map[string]interface{})
	//cache["rpc"] = rs.mwServer.spRPCClient.GetSnap()
	snap.Cache = cache
	snap.Snap = services

	buffer, _ := json.Marshal(&snap)
	return string(buffer)
}

//startRefreshSnap 启动定时刷新
func (rc *MWServer) startRefreshSnap() {
	defer rc.recover()

	fmt.Println("---->startRefreshSnap:", time.Second, time.Duration(rc.snap.Refresh))

	snap.Bind(time.Second*time.Duration(rc.snap.Refresh), rc.updateSnap)
}

func (rc *MWServer) setDefSnap() {
	rc.updateSnap(snap.GetData())
}

func (rc *MWServer) updateSnap(services map[string]interface{}) {
	rc.snapLogger.Info(" -> 更新 rc server快照信息")

	fmt.Println("rc.clusterClient.domain:", rc.clusterClient.domain)

	fmt.Println("rc.snap.path:", rc.snap.path)

	fmt.Println("rc.snap.GetServicesSnap(services):", rc.snap.GetServicesSnap(services))

	rc.clusterClient.SetNode(rc.snap.path, rc.snap.GetServicesSnap(services))
}

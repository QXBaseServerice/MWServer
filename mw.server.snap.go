package main

import (
	"encoding/json"
	"time"

	"github.com/arsgo/ars/snap"
	"github.com/arsgo/lib4go/sysinfo"
)

//MWSnap RC server快照信息
type MWSnap struct {
	mwServer   *MWServer
	Domain     string `json:"domain"`
	path       string
	Address    string      `json:"address"`
	Server     string      `json:"server"`
	Refresh    int         `json:"refresh"`
	Version    string      `json:"version"`
	CPU        string      `json:"cpu"`
	Mem        string      `json:"mem"`
	Disk       string      `json:"disk"`
	Last       string      `json:"last"`
	Monitoring interface{} `json:"monitoring"`
}

//GetServicesSnap 获取RC服务的快照信息
func (rs MWSnap) GetServicesSnap(services map[string]interface{}) string {
	snap := rs
	snap.Last = time.Now().Format("20060102150405")
	snap.CPU = sysinfo.GetAvaliabeCPU().Used
	snap.Mem = sysinfo.GetAvaliabeMem().Used
	snap.Disk = sysinfo.GetAvaliabeDisk().Used

	if rs.mwServer.monitorSystems.GetLength() > 0 {
		services["systems"] = rs.mwServer.monitorSystems.GetAll()
	}
	if rs.mwServer.monitorConfigs.GetLength() > 0 {
		services["paths"] = getAllKeys(rs.mwServer.monitorConfigs)
	}
	if rs.mwServer.monitorChildrenValue.GetLength() > 0 {
		services["children"] = rs.mwServer.monitorChildrenValue.GetAll()
	}

	/*rpcs := rs.mwServer.rpcServerCollector.Get()
	if len(rpcs) > 0 {
		services["rpc"] = rpcs
	}*/
	/*schedulers := rs.mwServer.schedulerCollector.Get()
	if len(schedulers) > 0 {
		services["jobs"] = schedulers
	}*/
	snap.Monitoring = services

	buffer, _ := json.Marshal(&snap)
	return string(buffer)
}

//startRefreshSnap 启动定时刷新
func (rc *MWServer) startRefreshSnap(p ...interface{}) {
	defer rc.recover()
	snap.Bind(time.Second*time.Duration(rc.snap.Refresh), rc.updateSnap)
}

func (rc *MWServer) setDefSnap() {
	rc.updateSnap(snap.GetData())
}

func (rc *MWServer) updateSnap(services map[string]interface{}) {
	rc.snapLogger.Info(" -> 更新 mw server快照信息")
	rc.clusterClient.SetNode(rc.snap.path, rc.snap.GetServicesSnap(services))
}

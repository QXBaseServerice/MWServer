package main

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/arsgo/ars/cluster"
	"github.com/arsgo/ars/snap"
	"github.com/arsgo/lib4go/concurrent"
	"github.com/arsgo/lib4go/influxdb"
	"github.com/arsgo/lib4go/mem"
)

//MWServerItem 的框架房东
type MWServerItem struct {
	Domain  string
	Address string
	Server  string
	Path    string
}

//BindMWServer 绑定服务
func (rc *MWServer) BindMWServer() (err error) {

	rc.snap.Address = fmt.Sprint(rc.conf.IP, getLocalRandomAddress())
	rc.snap.path, err = rc.clusterClient.CreateMWServer(rc.snap.GetServicesSnap(snap.GetData()))
	if err != nil {
		return err
	}

	rc.clusterClient.WatchServerChange(func(items []*MWServerItem, err error) {

		defer rc.startSync.Done("INIT.SERVER")

		isMaster := rc.IsMasterServer(items)
		if isMaster && !rc.IsMaster {
			rc.IsMaster = true
			rc.snap.Server = cluster.SERVER_MASTER
			rc.setDefSnap()
			rc.Log.Info(" -> 当前服务是 [", rc.snap.Server, "]")

			go rc.watchSystemNodesChange()
			go rc.watchWarningConfigChange()

		} else if !isMaster {
			rc.IsMaster = false
			rc.snap.Server = cluster.SERVER_SLAVE
			rc.setDefSnap()
			rc.Log.Info(" -> 当前服务是 [", rc.snap.Server, "]")
		}
	})

	return nil
}

func (rc *MWServer) watchWarningConfigChange() {
	path := rc.clusterClient.warningConfigPath
	rc.clusterClient.WatchValueChangedAndNotifyNow(path, func(configStr string, err error) {
		rc.Log.Infof(":【%s】的值发生了变化", path)
		if err != nil {
			rc.Log.Errorf("节点%s的监控异常：%v", path, err)
			return
		}
		var c WarningConfig
		if err := json.Unmarshal([]byte(configStr), &c); err != nil {
			rc.Log.Errorf("%s的值无法解析为WarningConfig(非法JSON)", path)
			rc.Log.Errorf("configStr:%s", configStr)
			panic(fmt.Errorf("%s的值无法解析为WarningConfig(非法JSON)", path))
		}
		rc.warningConfig = c
	})
}

func (rc *MWServer) watchSystemNodesChange() {
	path := rc.clusterClient.monitorConfigPath
	rc.clusterClient.WatchChildrenChangedAndNotifyNow(path, func(systemNameList []string, err error) {
		rc.Log.Infof(":【%s】的子节点发生了变化:%v", path, systemNameList)
		if err != nil {
			rc.Log.Errorf("系统%s的监控异常：%v", path, err)
			return
		}

		added, delted := getArrDifferentFromMap(systemNameList, rc.monitorSystems.GetAll())
		rc.Log.Infof("增加系统：%v", added)
		rc.Log.Infof("移除系统：%v", delted)

		for _, systemName := range added {
			go rc.watchSystemConfigChange(systemName, path+"/"+systemName)
		}

		for _, systemName := range delted {
			go rc.removeSystemConfigChangeListner(systemName)
		}
	})
}

func (rc *MWServer) watchSystemConfigChange(systemName, path string) {
	rc.clusterClient.WatchValueChangedAndNotifyNow(path, func(jsonstr string, err error) {
		rc.Log.Infof(":【%s】的值发生了变化", path)
		if err != nil {
			rc.Log.Errorf("节点%s的监控异常：%v", path, err)
			return
		}

		var m []ZKMonitorConfig
		if err := json.Unmarshal([]byte(jsonstr), &m); err != nil {
			rc.Log.Errorf("%s的值无法解析为ZKMonitorConfig(非法JSON)", path)
			rc.Log.Errorf("jsonstr:%s", jsonstr)
			return
		}

		// 处理含有统配符“*”的path
		newPathConfigs := concurrent.NewConcurrentMap()
		for i, len := 0, len(m); i < len; i++ {
			for _, item := range m[i].Paths {
				if !strings.HasSuffix(item, "/*") {
					// path+watchtype 作为索引
					c := &MonitorConfig{
						Path: item, SystemName: systemName, SystemFullName: "未设置", WatchType: m[i].WatchType,
						Properties: m[i].Properties, TplMsgs: m[i].Msgs}
					newPathConfigs.Set(c.getKey(), c)
					continue
				}

				childrens, err := rc.clusterClient.handler.GetChildren(strings.TrimRight(item, "/*"))
				if err != nil {
					continue
				}
				for _, child := range childrens {
					// path+watchtype 作为索引
					c := &MonitorConfig{
						Path: strings.TrimRight(item, "*") + child, SystemName: systemName, SystemFullName: "未设置", WatchType: m[i].WatchType,
						Properties: m[i].Properties, TplMsgs: m[i].Msgs}
					newPathConfigs.Set(c.getKey(), c)
				}
			}
		}

		addedConfigs, deletedConfigs := getMapDifferentFromMap(rc.monitorConfigs.GetAll(), newPathConfigs.GetAll())
		for _, item := range addedConfigs {
			config := newPathConfigs.Get(item).(*MonitorConfig)
			switch config.WatchType {
			case "children_count":
				rc.monitoringNodes(config)
			case "node_value":
				rc.monitoringData(config)
			case "children_value":
				rc.monitoringNodesData(config)
			default:
				rc.Log.Errorf("配置错误,不支持的监控类型:%s(%s)", config.WatchType, config.Path)
			}
		}

		for _, item := range deletedConfigs {
			rc.removeMonitorConfigChangeListner(rc.monitorConfigs.Get(item).(*MonitorConfig))
		}
	})
}

func (rc *MWServer) removeSystemConfigChangeListner(systemName string) {
	if rc.monitorSystems.Get(systemName) == nil {
		return
	}

	rc.Log.Infof("TODO:移除对【%s】系统配置的监控", systemName)

	configs := rc.monitorConfigs.GetAll()
	for _, item := range configs {
		c := item.(*MonitorConfig)
		if c.SystemName == systemName {
			rc.removeMonitorConfigChangeListner(c)
		}
	}
}

func (rc *MWServer) removeMonitorConfigChangeListner(config *MonitorConfig) {
	rc.Log.Infof("TODO:移除对【%s】系统下的路径【%s】【%s】的监控", config.SystemName, config.Path, config.WatchType)
	rc.monitorConfigs.Delete(config.getKey())
}

func (rc *MWServer) getMemcachedClient() (memcacheClient *mem.MemcacheClient, err error) {
	memcacheConfig, err := rc.clusterClient.GetDBConfig("memcached")
	if err != nil {
		return nil, err
	}
	memcacheClient, err = mem.New(memcacheConfig)
	if err != nil {
		return nil, err
	}
	return memcacheClient, nil
}

func (rc *MWServer) sendToWarningSystem(systemName, property, level, msg string) (err error) {

	subscribeGroups := []string{}

	// 解析出需要被提醒的订阅部门
	for rule, groupName := range rc.warningConfig.SubscribeGroups {
		reg := regexp.MustCompile("^" + strings.Replace(rule, "*", `(\w+)`, -1) + "$")  // 替换统配符"*" 为 单词字符"\w"(相当于 [0-9A-Za-z_])
		if reg.FindString(fmt.Sprintf("%s-%s-%s", systemName, property, level)) != "" { // 利用正则表达式匹配规则
			for _, val := range strings.Split(groupName, ",") { // groupName可能是"cyo2o,weixin"
				subscribeGroups = append(subscribeGroups, val)
			}
		}
	}

	// 遍历这些组
	//   遍历组下面的成员
	//     根据成员的配置方式，发送内容给他们
	memcacheClient, err := rc.getMemcachedClient()
	if err != nil {
		rc.Log.Errorf("Memcached初始化异常，但不退出，err:%s", err)
	}

	for _, groupName := range subscribeGroups {
		group := rc.warningConfig.Groups[groupName]
		if !group.IsRev {
			continue
		}
		for memberName, member := range group.Members {

			cachekey := fmt.Sprintf("%s_%s_%s_%s", systemName, property, level, memberName) // 同一个系统，同一个属性，同一个级别，同一个人，在指定的频率内只发送一次

			if memcacheClient != nil {
				if memcacheClient.Get(cachekey) != "" {
					rc.Log.Infof("发送太频繁，本次跳过！%s", cachekey)
					continue
				} else {
					rc.Log.Infof("缓存起来：%s", cachekey)
					memcacheClient.Add(cachekey, "Have Sent!", int32(member.Frequency))
				}
			}

			switch {
			case member.RecvSMS:
				way := rc.warningConfig.Ways["sms"]
				params, err := rc.clusterClient.GetSMSConfig(way.Channel)
				if err != nil {
					rc.Log.Errorf("无法获取渠道%s的配置参数", way.Channel)
				} else {
					if way.Disabled {
						rc.Log.Infof("短信发送渠道被暂停了")
					} else if err := sendWarningMessageBySMS(msg, member.Mobile, way.Channel, params); err != nil {
						rc.Log.Errorf("通过短信发送报警消息异常,err:%s", err)
					}
				}
				fallthrough // 贯穿
			case member.RecvWeixin:
				fmt.Println("TODO 发送微信消息")
				fallthrough
			case member.RecvEmail:
				fmt.Println("TODO 发送邮件")
				fallthrough
			default:
				break
			}

		}
	}

	return nil
}

//IsMasterServer 检查当前RC Server是否是Master
func (rc *MWServer) IsMasterServer(items []*MWServerItem) bool {
	var servers []string
	for _, v := range items {
		servers = append(servers, v.Path)
	}
	sort.Sort(sort.StringSlice(servers))
	return len(servers) == 0 || strings.EqualFold(rc.snap.path, servers[0])
}

func (rc *MWServer) monitoringNodesData(config *MonitorConfig) {
	p := config.Path
	rc.clusterClient.WatchChildrenChangedAndNotifyNow(p, func(children []string, err error) {
		if err != nil {
			rc.Log.Errorf("监控节点%s异常：%v", p, err)
			return
		}
		for _, nodeName := range children {
			childPath := p + "/" + nodeName
			if rc.currentListenPaths.Exists(childPath) {
				continue
			}
			rc.clusterClient.WatchValueChangedAndNotifyNow(childPath, func(value string, err error) {
				if err != nil {
					rc.Log.Errorf("监控节点%s的值异常:%v", p, err)
					return
				}
				go rc.monitoringCore(value, config)
			})
			rc.currentListenPaths.Set(childPath, true)
		}
	})
}

func (rc *MWServer) monitoringNodes(config *MonitorConfig) {
	rc.clusterClient.WatchChildrenChangedAndNotifyNow(config.Path, func(children []string, err error) {
		if err != nil {
			rc.Log.Errorf("监控节点%s异常：%v", config.Path, err)
			return
		}
		go rc.monitoringCore(fmt.Sprint(len(children)), config)
	})
}

func (rc *MWServer) monitoringData(config *MonitorConfig) {
	rc.clusterClient.WatchValueChangedAndNotifyNow(config.Path, func(value string, err error) {
		if err != nil {
			rc.Log.Errorf("监控节点%s异常：%v", config.Path, err)
			return
		}
		go rc.monitoringCore(value, config)
	})
}

func (rc *MWServer) monitoringCore(raw string, _pathConfig *MonitorConfig) {
	for prop, rules := range _pathConfig.Properties {
		level, currval, limitval, ip := validate(prop, raw, rules)
		if level == "" {
			fmt.Println("TODO：这里要考虑是否发送恢复消息")
			continue
		}

		msg := getWarningMessage(&WarningMessageEntity{level: level,
			property: prop, path: _pathConfig.Path, sysname: _pathConfig.SystemName, currval: currval,
			limitval: limitval, messagetpl: _pathConfig.TplMsgs[prop],
			ip: ip})

		// 避免同一台机器的cpu,memory,disk的重复报警
		if isContain, err := regexp.MatchString("^cpu|memory|disk$", prop); err == nil && isContain {
			key := ip + "_" + prop
			saveDate := rc.sameIPWarningMessages.Get(key)
			diff, _ := timeSubNowSeconds(saveDate.(string))
			rc.sameIPWarningMessages.Set(key, getNowTimeStamp()) // 设置到map
			if 0-diff <= 5 {
				fmt.Println("5秒内，相同IP的相同属性报警，忽略:", key)
				continue
			}
		}

		if err := rc.sendToWarningSystem(_pathConfig.SystemName, prop, level, msg); err != nil {
			fmt.Println(err)
		}
		if err := rc.sendToInfluxDB(prop, _pathConfig.SystemName, ip, _pathConfig.Path, currval); err != nil {
			fmt.Println("保存到InfluxDB异常：", err)
		}

	}
}

type MonitorData struct {
	Property string  `json:"property"`
	Sys      string  `json:"sys"`
	Robot    string  `json:"robot"`
	Script   string  `json:"script"`
	Data     float64 `json:"data"`
	Time     string  `json:"time"`
}

func (rc *MWServer) sendToInfluxDB(prop, sysName, ip, path, currval string) (err error) {
	val, err := strconv.ParseFloat(currval, 64)
	if err != nil {
		return fmt.Errorf("监控的数据[%s],转成float64时异常,err:%s", currval, err)
	}
	datas := []MonitorData{MonitorData{Property: prop, Sys: sysName, Robot: ip, Script: path, Data: val, Time: fmt.Sprint(time.Now().UnixNano())}}
	b, err := json.Marshal(datas)
	if err != nil {
		return fmt.Errorf("解析监控实体异常,err:%s", err)
	}
	influxClient, err := rc.getInfluxClient()
	if err != nil {
		return fmt.Errorf("创建Influx客户端异常,err:%s", err)
	}
	err = influxClient.Save(string(b))
	if err != nil {
		return fmt.Errorf("保存数据到InfluxDB异常,err:%s", err)
	}
	return nil
}

func (rc *MWServer) getInfluxClient() (influxClient *influxdb.InfluxDB, err error) {
	influxConfig, err := rc.clusterClient.GetDBConfig("influx")
	if err != nil {
		return nil, err
	}
	influxClient, err = influxdb.New(influxConfig)
	if err != nil {
		return nil, err
	}
	return influxClient, nil
}

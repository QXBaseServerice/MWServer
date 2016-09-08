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
		isMaster := rc.IsMasterServer(items)
		if isMaster && !rc.IsMaster {
			rc.IsMaster = true
			rc.snap.Server = cluster.SERVER_MASTER
			rc.setDefSnap()
			rc.Log.Info(" -> 当前服务是 [", rc.snap.Server, "]")

			go rc.watchSystemInfoChange()
			go rc.watchWarningConfigChange()
			go rc.watchSystemNodesChange()
		} else if !isMaster {
			rc.IsMaster = false
			rc.snap.Server = cluster.SERVER_SLAVE
			rc.setDefSnap()
			rc.Log.Info(" -> 当前服务是 [", rc.snap.Server, "]")
		}
	})

	return nil
}

func (rc *MWServer) watchSystemNodesChange() {
	path := rc.clusterClient.monitorConfigPath
	rc.clusterClient.WatchChildrenChangedAndNotifyNow(path, func(systemNameList []string, err error) {
		rc.startSync.Done("LOADING.EverySystemConfig")
		if err != nil {
			rc.Log.Errorf("系统%s的监控异常：%v", path, err)
			return
		}

		added, delted := getArrDifferentFromMap(rc.monitorSystems.GetAll(), systemNameList)

		for _, systemName := range added {
			p := fmt.Sprintf("%s/%s", path, systemName)
			go rc.watchSystemConfigChange(systemName, p)
		}

		for _, systemName := range delted {
			go rc.removeSystemConfigChangeListner(systemName)
		}
	})
}

func (rc *MWServer) watchSystemConfigChange(systemName, path string) {
	// 添加到已监控系统列表
	rc.monitorSystems.Set(systemName, path)

	rc.clusterClient.WatchValueChangedAndNotifyNow(path, func(jsonstr string, err error) {
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

				c := &MonitorConfig{
					Path: item, SystemName: systemName, SystemFullName: rc.systemsInfo[systemName].FullName, WatchType: m[i].WatchType,
					Properties: m[i].Properties, TplMsgs: m[i].Msgs}

				if !strings.HasSuffix(item, "/*") {
					newPathConfigs.Set(c.getKey(), c)
					continue
				} else {
					// TODO:暂时不考虑路径中配置“/*”的支持
					// 因为没有监控/*之前的节点变化，后续/*下面增加了子节点，将无法被监听(带有一定复杂性)
					panic(fmt.Errorf("请不要在路径【%s】中配置：“/*”", item))
				}
				/*childrens, err := rc.clusterClient.handler.GetChildren(strings.TrimRight(item, "/*"))
				if err != nil {
					rc.Log.Errorf("%s没有子节点", strings.TrimRight(item, "/*"))
					continue
				}
				for _, child := range childrens {
					c.Path = strings.TrimRight(item, "*") + child
					newPathConfigs.Set(c.getKey(), c)
				}*/
			}
		}

		addedConfigs, deletedConfigs := getMapDifferentFromMap(rc.monitorConfigs.GetAll(), newPathConfigs.GetAll())

		for _, item := range addedConfigs {
			c, _ := newPathConfigs.Get(item)
			config := c.(*MonitorConfig)

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
			c, _ := rc.monitorConfigs.Get(item)
			rc.removeMonitorConfigChangeListner(c.(*MonitorConfig))
		}
	})
}

func (rc *MWServer) monitoringNodesData(config *MonitorConfig) {
	defer rc.recover()
	// 添加到已监控路径列表
	if !config.outOfConfigMap {
		rc.monitorConfigs.Set(config.getKey(), config)
	}

	p := config.Path
	rc.clusterClient.WatchChildrenChangedAndNotifyNow(p, func(children []string, err error) {
		//rc.Log.Infof("——> PATH监控(s)：【%s】的子节点发生了改变:%v", p, children)
		if err != nil {
			rc.Log.Errorf("监控节点%s异常：%v", p, err)
			return
		}

		var oldChildren []string
		o, isExists := rc.monitorChildrenValue.Get(config.getKey())
		if !isExists {
			oldChildren = []string{}
		} else {
			oldChildren = o.([]string)
		}
		rc.monitorChildrenValue.Set(config.getKey(), children) // 更新此节点的已监控子节点集合

		added, deleted := getArrDifferentFromArr(oldChildren, children)

		getChildMonitorConfig := func(realpath string, parentConfig *MonitorConfig) *MonitorConfig {
			child := parentConfig.getCopier()
			child.parentKey = parentConfig.getKey()
			child.outOfConfigMap = true // 子节点并未配置到zk，为了计算配置路径的差异化，将不会被包含到rc.monitorConfigs
			child.Path = realpath
			child.WatchType = "node_value"
			return child
		}

		for _, item := range added {
			go rc.monitoringData(getChildMonitorConfig(p+"/"+item, config))
		}

		for _, item := range deleted {
			go rc.removeMonitorConfigChangeListner(getChildMonitorConfig(p+"/"+item, config))
		}
	})
}

func (rc *MWServer) monitoringNodes(config *MonitorConfig) {
	defer rc.recover()
	// 添加到已监控路径列表
	if !config.outOfConfigMap {
		rc.monitorConfigs.Set(config.getKey(), config)
	}

	rc.clusterClient.WatchChildrenChangedAndNotifyNow(config.Path, func(children []string, err error) {
		//rc.Log.Infof("——> PATH监控：【%s】的子节点发生变化:%v", config.Path, children)
		if err != nil {
			rc.Log.Errorf("监控节点%s异常：%v", config.Path, err)
			return
		}
		go rc.monitoringCore(fmt.Sprint(len(children)), config)
	})
}

func (rc *MWServer) monitoringData(config *MonitorConfig) {
	// 添加到已监控路径列表
	if !config.outOfConfigMap {
		rc.monitorConfigs.Set(config.getKey(), config)
	}

	rc.clusterClient.WatchValueChangedAndNotifyNow(config.Path, func(value string, err error) {
		//rc.Log.Infof("——> PATH监控：【%s】的值发生变化", config.Path)
		if err != nil {
			rc.Log.Errorf("监控节点%s异常：%v", config.Path, err)
			return
		}
		go rc.monitoringCore(value, config)
	})
}

func (rc *MWServer) monitoringCore(raw string, _pathConfig *MonitorConfig) {
	defer rc.recover()

	for prop, rules := range _pathConfig.Properties {
		level, currval, limitval, ip := validate(prop, raw, rules)

		if err := rc.sendToInfluxDB(prop, _pathConfig.SystemName, ip, _pathConfig.Path, currval); err != nil {
			fmt.Println("保存到InfluxDB异常：", err)
		}

		if level == "" {
			rc.Log.Info("TODO：这里要考虑是否发送恢复消息")
			continue
		}

		msg := getWarningMessage(&WarningMessageEntity{level: level,
			property: prop, path: _pathConfig.Path, sysname: _pathConfig.SystemName, currval: currval,
			limitval: limitval, messagetpl: _pathConfig.TplMsgs[prop],
			ip: ip})

		// 避免同一台机器的cpu,memory,disk的重复报警
		isContain, err := regexp.MatchString("^cpu|memory|disk$", prop)
		if err == nil && isContain {
			key := ip + "_" + prop
			saveDate, isExists := rc.sameIPWarningMessages.Get(key)
			if isExists {
				diff, _ := timeSubNowSeconds(saveDate.(string))
				if 0-diff <= 5 {
					rc.Log.Infof("5秒内，相同IP的相同属性报警，被忽略:%s", key)
					rc.sameIPWarningMessages.Set(key, getNowTimeStamp()) // 更新时间戳
					continue
				}
			} else {
				rc.sameIPWarningMessages.Set(key, getNowTimeStamp()) // 设置到map
			}
		}

		if err := rc.sendToWarningSystem(_pathConfig.SystemName, prop, level, msg, ip); err != nil {
			fmt.Println(err)
		}

	}
}

func (rc *MWServer) removeSystemConfigChangeListner(systemName string) {
	d, isExists := rc.monitorSystems.Get(systemName)
	if !isExists {
		rc.Log.Errorf("系统【%s】的配置不在MAP[monitorSystems]中", systemName)
		return
	}

	// 移除系统级别的节点的监控
	rc.Log.Infof("移除对【%s】系统本身配置的监控", systemName)
	rc.monitorSystems.Delete(systemName)
	rc.clusterClient.handler.RemoveWatchValue(d.(string))

	// 移除系统下的所有已注册监控
	configs := rc.monitorConfigs.GetAll()
	for _, item := range configs {
		c := item.(*MonitorConfig)
		if c.SystemName == systemName {
			rc.removeMonitorConfigChangeListner(c)
		}
	}
}

func (rc *MWServer) removeMonitorConfigChangeListner(config *MonitorConfig) {
	defer rc.recover()
	if config.outOfConfigMap {
		return
	}

	rc.Log.Infof("移除对【%s】系统下的路径【%s】【%s】的监控", config.SystemName, config.Path, config.WatchType)
	rc.monitorConfigs.Delete(config.getKey())

	switch config.WatchType {
	case "children_count":
		rc.clusterClient.handler.RemoveWatchChildren(config.Path)
	case "children_value":
		// 移除对该节点的子节点的监控
		rc.clusterClient.handler.RemoveWatchChildren(config.Path)
		// 遍历移除对其子节点的值的监控
		if c, isExists := rc.monitorChildrenValue.GetAndDel(config.getKey()); isExists {
			if children, isOk := c.([]string); isOk {
				for _, child := range children {
					rc.Log.Infof("移除对【%s】的ChildrenValue：【%s】的监控", config.Path, child)
					rc.clusterClient.handler.RemoveWatchValue(config.Path + "/" + child)
				}
			}
		}
	case "node_value":
		rc.clusterClient.handler.RemoveWatchValue(config.Path)
	default:
		panic(fmt.Errorf("UNREACHABLE:未处理的监控类型:%s", config.WatchType))
	}
}

func (rc *MWServer) watchWarningConfigChange() {
	path := rc.clusterClient.warningConfigPath
	rc.clusterClient.WatchValueChangedAndNotifyNow(path, func(configStr string, err error) {
		defer rc.startSync.Done("LOADED.WarningConfig")
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

func (rc *MWServer) watchSystemInfoChange() {
	path := rc.clusterClient.systemInfoConfigPath
	rc.clusterClient.WatchValueChangedAndNotifyNow(path, func(configStr string, err error) {
		defer rc.startSync.Done("LOADED.SystemInfo")
		if err != nil {
			rc.Log.Errorf("节点%s的监控异常：%v", path, err)
			return
		}
		var c map[string]SystemInfo
		if err := json.Unmarshal([]byte(configStr), &c); err != nil {
			panic(fmt.Errorf("%s的值无法解析为map[string]SystemInfo(非法JSON)", path))
		}
		rc.systemsInfo = c
	})
}

func (rc *MWServer) sendToWarningSystem(systemName, property, level, msg, ip string) (err error) {

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

			cachekey := fmt.Sprintf("%s_%s_%s_%s_%s", systemName, ip, property, level, memberName) // 同一个系统，同一个IP，同一个属性，同一个级别，同一个人，在指定的频率内只发送一次

			if memcacheClient != nil {
				if memcacheClient.Get(cachekey) != "" {
					rc.Log.Infof("发送太频繁，本次跳过！%s", cachekey)
					continue
				} else {
					rc.Log.Infof("缓存起来：%s", cachekey)
					memcacheClient.Add(cachekey, "Have Sent!", int(member.Frequency))
				}
			}

			if member.RecvSMS {
				rc.Log.Infof("向【%s】发送短信告警:%s", member.FullName, msg)
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
			}
			if member.RecvWeixin {
				rc.Log.Infof("向【%s】发送微信告警:%s", member.FullName, msg)
				fmt.Println("TODO 发送微信消息")
			}
			if member.RecvEmail {
				rc.Log.Infof("向【%s】发送邮件告警:%s", member.FullName, msg)
				fmt.Println("TODO 发送邮件")
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

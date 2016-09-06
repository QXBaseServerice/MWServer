package main

import "fmt"

/*------------- 系统用的配置(最终从zookeeper转换过来) -------------*/

type MonitorConfig struct {
	Path           string
	SystemName     string
	SystemFullName string
	WatchType      string
	Properties     map[string][]MapString
	TplMsgs        map[string]string
}

func (config *MonitorConfig) getKey() string {
	return fmt.Sprintf("%s:%s", config.Path, config.WatchType)
}

type MapString map[string]string

/*------------- Zookeeper中的 监控配置 -------------*/

type ZKMonitorConfig struct {
	Paths      []string
	WatchType  string `json:"watch"` // children or value
	Properties map[string][]MapString
	Msgs       map[string]string
}

/*------------- Zookeeper中的 报警配置 -------------*/

type WarningConfig struct {
	SubscribeGroups map[string]string `json:"subscriber"`
	Groups          map[string]WarningGroup
	Ways            map[string]SendWay
}

type SendWay struct {
	Channel    string `json:"channel"`
	Disabled   bool   `json:"disabled"`
	ParamsPath string `json:"params_path"`
}

type WarningGroup struct {
	FullName string `json:"full_name"`
	IsRev    bool   `json:"is_rev"`
	Members  map[string]WarningMember
}

type WarningMember struct {
	FullName   string `json:"full_name"`
	Mobile     string
	OpenID     string `json:"open_id"`
	EmailAddr  string `json:"email_addr"`
	RecvSMS    bool   `json:"recv_sms"`
	RecvEmail  bool   `json:"recv_email"`
	RecvWeixin bool   `json:"recv_weixin"`
	Frequency  int
}

/*type WarningGroup struct {
	GroupName string
	FullName  string
	IsRev     bool
}

type WarningMember struct {
	BelongGroup string
	FullName    string
	Mobile      string
	OpenID      string
	EmailAddr   string
	RecvSMS     bool
	RecvEmail   bool
	RecvWeixin  bool
	Frequency   int
}*/

package main

import "github.com/arsgo/lib4go/utility"

type INotifier interface {
	SendMessage(msg *Message) error
}

type Message struct {
	content           string // 发送内容
	to                string // 发送给谁
	timeStamp         string // 时间戳
	channel           string // 发送渠道
	channelParamsPath string // 渠道所需参数的路径
}

// WarningMessageEntity 报警消息实体
type WarningMessageEntity struct {
	property   string
	level      string
	path       string
	sysname    string
	currval    string
	limitval   string
	messagetpl string
	ip         string
}

func getWarningMessage(entity *WarningMessageEntity) string {
	dataMap := utility.NewDataMap()
	dataMap.Set("level", entity.level)
	dataMap.Set("ip", entity.ip)
	dataMap.Set("path", entity.path)
	dataMap.Set("sys", entity.sysname)
	dataMap.Set("currval", entity.currval)
	dataMap.Set("limitval", entity.limitval)

	return dataMap.Translate(entity.messagetpl)
}

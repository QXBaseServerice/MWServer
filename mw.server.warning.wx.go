package main

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/arsgo/lib4go/net"
	"github.com/arsgo/lib4go/security/md5"
)

// QXBaseWXSystemConfig 千行基础微信系统配置
type QXBaseWXSystemConfig struct {
	URL         string
	Token       string
	AppID       string
	SuccessCode string `json:"success_code"`
}

func sendWarningMessageByWeixin(message string, to string, config *QXBaseWXSystemConfig) {

	req := net.NewHTTPClient()

	url := config.URL
	token := config.Token

	signParams := make(map[string]string)
	signParams["app_id"] = config.AppID
	signParams["open_id"] = to
	signParams["content"] = message
	signParams["timestamp"] = time.Now().Format("20060102150405")

	keyarr := []string{}
	for key := range signParams {
		keyarr = append(keyarr, key)
	}
	sort.Strings(keyarr)

	signRaw := ""
	for _, key := range keyarr {
		if signParams[key] == "" {
			continue
		}
		signRaw = signRaw + key + signParams[key]
	}
	signRaw = token + signRaw + token

	fmt.Println("签名原串：", signRaw)
	signParams["sign"] = md5.Encrypt(signRaw)
	fmt.Println("签名：", signParams["sign"])

	params := "?"
	for key, value := range signParams {
		params = fmt.Sprintf("%s%s=%s&", params, key, value)
	}
	params = strings.TrimRight(params, "&")

	url = url + params
	fmt.Println("最终URL：", url)

	content, status, err := req.Get(url)
	if err != nil {
		fmt.Println("发送信息异常,err:", err)
		return
	}
	if status != 200 {
		fmt.Println("发送信息,非预期响应,状态码:", status)
		return
	}

	resultCode, err := getValueFromJSONString(content, "result.code", true)
	if err != nil {
		fmt.Println("无法解析请求结果,err:", err)
		return
	}
	if fmt.Sprint(resultCode) != config.SuccessCode {
		fmt.Println("发送失败:", content)
		return
	}
	fmt.Println("发送成功！")
	return
}

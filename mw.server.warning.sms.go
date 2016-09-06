package main

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/arsgo/lib4go/net"
	"github.com/arsgo/lib4go/security/base64"
	"github.com/arsgo/lib4go/security/md5"
)

// YTXConfig 云通讯的配置
type YTXConfig struct {
	Encoding         string
	HTTPURL          string `json:"http_url"`
	HTTPPort         string `json:"http_port"`
	MainAccount      string `json:"main_account"`
	MainAccountToken string `json:"main_account_token"`
	SubAccount       string `json:"sub_account"`
	SubAccountToken  string `json:"sub_account_token"`
	VoipAccount      string `json:"voip_account"`
	VoipPassword     string `json:"voip_password"`
	Appid            string `json:"appid"`
	SoftVersion      string `json:"soft_version"`
	MsgType          string `json:"msg_type"`
	URLFormat        string `json:"url_format"`
	SuccessCode      string `json:"success_code"`
	WarningTplID     string `json:"warning_tpl_id"`
}

func sendWarningMessageBySMS(message string, mobile string, sendChannel string, params string) error {
	if sendChannel == "ytx" {
		var ytx YTXConfig
		if err := json.Unmarshal([]byte(params), &ytx); err != nil {
			return fmt.Errorf("云通讯的短信发送配置错误,无法解析,err:%s", err)
		}
		return sendSMSByYTX(message, mobile, &ytx)
	}
	return nil
}

func sendSMSByYTX(message string, mobile string, ytx *YTXConfig) error {

	tsp := time.Now().Format("20060102150405")
	raw := fmt.Sprintf("%s%s%s", ytx.MainAccount, ytx.MainAccountToken, tsp)
	sign := strings.ToUpper(md5.Encrypt(raw))

	datas := "<data>" + message + "</data>"
	postData := fmt.Sprintf(`<?xml version='1.0' encoding='utf-8'?>
									<TemplateSMS>
										<to>%s</to>
										<appId>%s</appId>
										<templateId>%s</templateId>
										<datas>%s</datas>
									</TemplateSMS>`, mobile, ytx.Appid, ytx.WarningTplID, datas)

	base64Str := base64.Encode(fmt.Sprintf("%s:%s", ytx.MainAccount, tsp))
	url := fmt.Sprintf(ytx.URLFormat, ytx.HTTPURL, ytx.HTTPPort, ytx.SoftVersion, ytx.MainAccount, sign)

	client := net.NewHTTPClient()
	req := client.NewRequest("POST", url)

	req.SetData(postData)
	req.SetHeader("Accept", "application/xml")
	req.SetHeader("Content-type", "application/xml")
	req.SetHeader("charset", "utf-8")
	req.SetHeader("Authorization", base64Str)

	content, status, err := req.Request()
	if err != nil {
		return fmt.Errorf("发送信息异常,err:%s", err)
	}
	if status != 200 {
		return fmt.Errorf("发送信息,非预期响应,状态码:%v", status)
	}

	resultCode, err := getValueFromXMLString(content, "Response.statusCode", true)
	if err != nil {
		return fmt.Errorf("无法解析请求结果,err:%s", err)
	}
	if fmt.Sprint(resultCode) != ytx.SuccessCode {
		return fmt.Errorf("发送失败,content:%s", content)
	}
	fmt.Println("发送成功！")
	return nil
}

package main

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/arsgo/lib4go/concurrent"
	"github.com/arsgo/lib4go/net"
)

//time.Now().Sub(start).Seconds()
//theTime.Unix()
//dataTimeStr := time.Unix(sr, 0).Format(timeLayout)
func timeSubNowSeconds(t string) (int32, error) {
	layout := "20060102150405"
	t1, err := time.ParseInLocation(layout, t, time.Local) //time.Local)
	if err != nil {
		return 0, err
	}
	t2 := time.Now()
	return int32(t1.Sub(t2).Seconds()), nil
}

// timeDiffSeconds 时间相差的秒数
// time1 yyyymmddhh24miss
// time2 yyyymmddhh24miss
func time1SubTime2Seconds(time1, time2 string) (int32, error) {
	layout := "20060102150405"
	t1, err := time.ParseInLocation(layout, time1, time.Local) //time.Local)
	if err != nil {
		return 0, err
	}
	t2, err := time.ParseInLocation(layout, time2, time.Local) //time.Local)
	if err != nil {
		return 0, err
	}
	return int32(t1.Sub(t2).Seconds()), nil
}

// getNowTimeStamp 获取当前时间戳
func getNowTimeStamp() string {
	return time.Now().Format("20060102150405")
}

func getLocalRandomAddress(start ...int) string {
	return fmt.Sprintf(":%d", getPort(start...))
}

func getPort(start ...int) int {
	s := 10160
	if len(start) > 0 {
		s = start[0]
	}
	for i := 0; i < 100; i++ {
		port := s + i*8
		if net.IsTCPPortAvailable(port) {
			return port
		}
	}
	return -1
}

func getValueFromJSONString(source string, path string, igoreCase bool) (destination interface{}, err error) {
	if source == "" {
		return nil, errors.New("JSON字符串不能为空")
	}

	var obj interface{}
	if err := json.Unmarshal([]byte(source), &obj); err != nil {
		return nil, err
	}
	return getValueFromObject(obj, path, igoreCase)
}

func getValueFromXMLString(source string, path string, igoreCase bool) (destination interface{}, err error) {
	if source == "" {
		return nil, errors.New("XML字符串不能为空")
	}

	var obj interface{}
	if err := xml.Unmarshal([]byte(source), &obj); err != nil {
		return nil, err
	}
	return getValueFromObject(obj, path, igoreCase)
}

func getValueFromObject(source interface{}, path string, igoreCase bool) (destination interface{}, err error) {

	if path == "" {
		return nil, errors.New("Path不能为空")
	}

	namelist := strings.Split(path, ".")
	if destination, err = deepCopy(source); err != nil {
		fmt.Println("发生了错误")
		return nil, err
	}

	var getValueFromName = func(m map[string]interface{}, attrName string) interface{} {
		for key, value := range m {
			if igoreCase {
				if strings.ToLower(key) == strings.ToLower(attrName) {
					return value
				}
			} else {
				if key == attrName {
					return value
				}
			}
		}
		return nil
	}

	for {
		if len(namelist) <= 0 {
			break
		}

		attrName := namelist[0]              // 取出第一个属性名称
		namelist = namelist[1:len(namelist)] // 从属性路径中移除第一个

		if attrName == "" {
			return nil, errors.New("路径非法，含有空的属性值")
		}
		switch destination.(type) {
		case map[string]interface{}:
			if destination = getValueFromName(destination.(map[string]interface{}), attrName); destination == nil {
				return nil, nil
			}
			break
		case []interface{}:
			reg := regexp.MustCompile(`^\d+$`)
			indexStr := reg.FindString(attrName)
			if indexStr == "" {
				return nil, nil
			}
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				return nil, nil
			}
			if index >= len(destination.([]interface{})) {
				return nil, nil
			}
			destination = destination.([]interface{})[index]
			break
		default:
			return nil, nil
		}
	}

	return destination, nil
}

func deepCopy(src interface{}) (dst interface{}, err error) {
	b, err := json.Marshal(src)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(b, &dst)
	return dst, err
}

func getArrDifferentFromMap(old map[string]interface{}, new []string) (added, deleted []string) {
	newMap := make(map[string]bool)
	for _, item := range new {
		newMap[item] = true
		if old[item] == nil {
			added = append(added, item)
		}
	}
	for key := range old {
		if newMap[key] == false {
			deleted = append(deleted, key)
		}
	}
	return
}

func getMapDifferentFromMap(old map[string]interface{}, new map[string]interface{}) (added, deleted []string) {
	for key := range old {
		if new[key] == nil {
			deleted = append(deleted, key)
		}
	}
	for key := range new {
		if old[key] == nil {
			added = append(added, key)
		}
	}
	return
}

func getArrDifferentFromArr(old []string, new []string) (added, deleted []string) {
	oldMap := make(map[string]interface{})
	newMap := make(map[string]interface{})
	for _, item := range old {
		oldMap[item] = true
	}
	for _, item := range new {
		newMap[item] = true
	}
	return getMapDifferentFromMap(oldMap, newMap)
}

func getAllKeys(m *concurrent.ConcurrentMap) (keys []string) {
	kvs := m.GetAll()
	for key := range kvs {
		keys = append(keys, key)
	}
	return keys
}

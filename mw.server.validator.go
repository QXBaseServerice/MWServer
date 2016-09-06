package main

import (
	"fmt"
	"strconv"
)

const (
	//Greater 更大的比较
	Greater int = iota
	//Less 更小的比较
	Less
)

func validate(property string, value interface{}, rules []MapString) (level, currval, limitval, ip string) {
	switch property {
	case "balance":
		currval = value.(string)
		level, limitval = moreLessMoreDangerous(currval, rules) // TODO:后面把moreLessMoreDangerous和moreGreaterMoreDangerous的入参类型定下来，取消interface{}类型
	case "cpu":
		o, err := getValueFromJSONString(value.(string), property, true)
		if err != nil {
			fmt.Printf("从JSON中获取%s时异常,err:%s\n", property, err)
			break
		}
		if o == nil {
			fmt.Printf("从JSON中获取%s的值为nil,无法比较\n", property)
			break
		}
		currval = fmt.Sprint(o)
		level, limitval = moreGreaterMoreDangerous(currval, rules)
	case "memory":
		o, err := getValueFromJSONString(value.(string), "sys.mem.0.usedPercent", true)
		if err != nil {
			fmt.Printf("从JSON中获取%s时异常,err:%s\n", property, err)
			break
		}
		if o == nil {
			fmt.Printf("从JSON中获取%s的值为nil,无法比较\n", property)
			break
		}
		currval = fmt.Sprint(o)
		level, limitval = moreGreaterMoreDangerous(currval, rules)
	}
	o, err := getValueFromJSONString(value.(string), "address", true)
	if err == nil && o != nil {
		ip = fmt.Sprint(o)
	}
	return level, currval, limitval, ip
}

func moreLessMoreDangerous(value interface{}, rules []MapString) (level, limitValue string) {
	return compare(value, rules, Less)
}
func moreGreaterMoreDangerous(value interface{}, rules []MapString) (level, limitValue string) {
	return compare(value, rules, Greater)
}

func compare(value interface{}, rules []MapString, compareType int) (level, sadValue string) {

	levelEnums := make(map[string]int)
	levelEnums["S"] = 1
	levelEnums["A"] = 2
	levelEnums["B"] = 3
	levelEnums["C"] = 4
	levelEnums["D"] = 5

	var currValue float64
	isOk := false
	switch value.(type) {
	case string:
		theval, err := strconv.ParseFloat(value.(string), 64) // value.(float32)
		if err == nil {
			isOk = true
			currValue = theval
		}
	case int:
		currValue = float64(value.(int))
		isOk = true
	case float64:
		currValue, isOk = value.(float64)
	default:
		fmt.Println("不应该到达的分支类型：", value)
	}

	if !isOk {
		return "", ""
	}

	for _, rule := range rules {
		for limitValue, limitLevel := range rule {
			limitValue, err := strconv.ParseFloat(limitValue, 64) // value.(float32)
			if err != nil {
				return "", ""
			}
			if compareType == Greater {
				if currValue >= limitValue && (level == "" || levelEnums[limitLevel] <= levelEnums[level]) {
					level = limitLevel
					sadValue = fmt.Sprintf("%v", limitValue)
				}
			} else if compareType == Less {
				if currValue <= limitValue && (level == "" || levelEnums[limitLevel] <= levelEnums[level]) {
					level = limitLevel
					sadValue = fmt.Sprintf("%v", limitValue)
				}
			} else {
				return "", ""
			}
		}
	}
	return level, sadValue
}

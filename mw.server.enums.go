package main

//MWSnap RC server快照信息
type Enums struct {
	WatchType
}

type WatchType struct {
	ChildrenValue string
	ChildrenNodes string
	Value         string
}

func init() {
	//Enums.WatchType.ChildrenNodes = ""
}

package toolbox

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/thedevsaddam/gojsonq"
	"gopkg.in/ini.v1"
)

var (
	cfg *ini.File
)

func AnyToString(anyone interface{}) string {
	var anyones string
	switch anyone := anyone.(type) {
	case int:
		anyones = strconv.Itoa(anyone)
	case string:
		anyones = anyone
	case int64:
		anyones = strconv.FormatInt(anyone, 10)
	default:
		log.Println(anyone, "类型为: UNKNOW TYPE！")
	}
	return anyones
}

func StrtoIn64(str string) int64 {
	num64, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		log.Fatalln(err)
	}
	return num64
}

func TimeTostr(t int64) string {
	tm := time.Unix(t, 0)
	timeString := tm.Format("2006-01-02 15:04:05")
	return timeString
}

func ToTime(timeString string) time.Time {
	loc, _ := time.LoadLocation("Local")
	the_time, err := time.ParseInLocation("2006-01-02 15:04:05", timeString, loc)
	if err != nil {
		log.Println(err)
	}
	return the_time
}

func InitCfg() *ini.File {
	var err error
	args := os.Args
	if args == nil || len(args) != 2 || len(args[1]) == 0 {
		fmt.Printf(`请在当前目录下创建*.ini(配置文件名自定义)的配置文件,文件格式为:
				[zabbix]
				zabbix_hosts = 192.168.137.24;
				启动:%s CfgFileName
	`, args[0])
		os.Exit(1)
	}

	cfg, err = ini.Load(args[1])

	if err != nil {
		log.Printf("Fail to Load Config: %v", err)
	}
	return cfg
}

func NewTask() *cron.Cron {
	c := cron.New(cron.WithSeconds()) //精确到秒

	return c
}

func ConfigRedis(jsonfile string) []map[string]string {
	redis := []map[string]string{}
	gq := gojsonq.New().File(jsonfile)
	data := gq.Find("items").([]interface{})
	for _, v := range data {
		makemap := make(map[string]string)
		for k, vv := range v.(map[string]interface{}) {
			makemap[k] = vv.(string)
		}
		redis = append(redis, makemap)
	}
	return redis
}

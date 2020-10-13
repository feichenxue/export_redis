package dbstore

import (
	"fmt"
	"log"
	"time"

	"export_redis/toolbox"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

var (
	db  *gorm.DB
	err error
)

type Slow_log struct {
	gorm.Model
	HostsName     string `gorm:"size:255"`
	HostsTag      string `gorm:"size:255"`
	ClusterRole   string `gorm:"size:20"`
	SlowId        string `gorm:"size:255"`
	TimesTamp     time.Time
	ExecutionTime int64
	Command       string `gorm:"size:255"`
	ClientIP      string `gorm:"size:255"`
	ClientName    string `gorm:"size:255"`
}

func init() {
	cfg := toolbox.InitCfg()
	db_user := cfg.Section("mysql").Key("datasource.user").String()
	db_pwd := cfg.Section("mysql").Key("datasource.password").String()
	db_hosts := cfg.Section("mysql").Key("datasource.hosts").String()
	db_port := cfg.Section("mysql").Key("datasource.port").String()
	db_lib := cfg.Section("mysql").Key("datasource.db").String()
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?charset=utf8mb4&parseTime=True&loc=Local", db_user, db_pwd, db_hosts, db_port, db_lib)
	db, err = gorm.Open(mysql.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("failed to connect database!!!")
	}
	// 迁移 schema
	err := db.AutoMigrate(&Slow_log{})
	if err != nil {
		log.Println("AutoMigrate is ERROR: ", err)
	}
}

func InsertData(hostsname, hoststag, role string, rlog [][]string) error {
	slowdata := []Slow_log{}
	for _, data := range rlog {
		clientname := data[5]
		if clientname == "" {
			clientname = "null"
		}
		slowdata = append(slowdata, Slow_log{
			HostsName:     hostsname,
			HostsTag:      hoststag,
			ClusterRole:   role,
			SlowId:        data[0],
			TimesTamp:     toolbox.ToTime(data[1]),
			ExecutionTime: toolbox.StrtoIn64(data[2]),
			Command:       data[3],
			ClientIP:      data[4],
			ClientName:    clientname})
	}
	result := db.Create(&slowdata)
	return result.Error
}

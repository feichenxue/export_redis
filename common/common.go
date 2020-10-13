package common

import (
	"context"
	"fmt"
	"log"
	"strings"

	"export_redis/dbstore"
	"export_redis/toolbox"

	"github.com/go-redis/redis/v8"
)

var (
	ctx       = context.Background()
	redisdata []map[string]string
	cfg       = toolbox.InitCfg()
)

type redisDb struct {
	Redis *redis.Client
}

type clusterDB struct {
	Clusterdb *redis.ClusterClient
}

func newClient(hosts, redisPasswd string) *redis.Client {
	db := redis.NewClient(&redis.Options{
		Addr:     hosts,
		Password: redisPasswd, // no password set
		DB:       0,           // use default DB
	})

	_, err := db.Ping(ctx).Result()
	if err != nil {
		log.Println(err)
	}
	return db
}

func clusterRedis(redisCluster []string, redisPasswd string) *redis.ClusterClient {
	//----------初始化 redis 集群模式-----
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:         redisCluster, // redis address
		Password:      redisPasswd,  // password set
		ReadOnly:      true,
		RouteRandomly: true,
	})

	pong, err := rdb.Ping(ctx).Result()
	if err != nil {
		log.Println(err)
	} else {
		log.Println(pong, ": Redis Status OK!")
	}
	return rdb
}

func init() {
	jsonfile := cfg.Section("redis_conf").Key("redis.cluster.jsonfile").String()
	redisdata = toolbox.ConfigRedis(jsonfile)
}

func (c *clusterDB) clusterInfo() map[string]string {
	defer c.Clusterdb.Close()
	res, err := c.Clusterdb.Do(ctx, "cluster", "nodes").Result()
	if err != nil {
		log.Println(err)
	}
	clustermap := make(map[string]string)
	data := strings.Split(res.(string), "\n")
	// data[:len(data)-1]
	// 此方法可以删除切片最后一个元素,在此最后一个元素肯定为空，所以可以用此方法，但是更通用的是判断，不知道谁的性能更好
	for _, va := range data {
		if len(va) != 0 { //通过判断容量是否为零
			node_value := strings.Split(va, " ")
			key := strings.Split(node_value[1], "@")[0]
			value := strings.Replace(node_value[2], "myself,", "", -1)
			clustermap[key] = value
		}
	}
	return clustermap
}

func (r *redisDb) nodeSlowlog(node string) ([][]string, error) {
	var (
		slowId     string
		timestamp  string
		excupttime string
		clientip   string
		clientname string
	)
	defer r.Redis.Close() //
	node_slow := [][]string{}
	slowlog, err := r.Redis.Do(ctx, "slowlog", "get").Result()
	if err != nil {
		return nil, err
	}
	for _, slow := range slowlog.([]interface{}) {
		command := ""
		data := slow.([]interface{}) // 总数据
		slowId = toolbox.AnyToString(data[0])
		timestamp = toolbox.TimeTostr(data[1].(int64))
		excupttime = toolbox.AnyToString(data[2])
		//提取命令字段
		com := data[3].([]interface{})
		for _, c := range com {
			command = command + " " + c.(string)
		}
		clientip = toolbox.AnyToString(data[4])
		clientname = toolbox.AnyToString(data[5])
		slowinfo := []string{slowId, timestamp, excupttime, command, clientip, clientname}
		node_slow = append(node_slow, slowinfo)
	}

	//清空 slow 日志记录
	if cfg.Section("base").Key("app.env").String() == "prd" && len(node_slow) != 0 {
		rs, err := r.Redis.Do(ctx, "slowlog", "reset").Result()
		if err != nil {
			log.Println(err)
		}
		log.Printf("%s - %s: slowlog reset", node, rs)
	}

	//返回当前节点的所有慢日志
	return node_slow, nil
}

func exportSlowlog() {
	for _, value := range redisdata {
		cluster := value["cluster"]
		passwd := value["passwd"]
		redis_tag := value["tag"]
		clusterhosts := strings.Split(cluster, ",") // redis 集群切片

		//获取集群信息
		clredis := clusterRedis(clusterhosts, passwd)
		this := clusterDB{clredis}
		dmap := this.clusterInfo() // 获取集群信息数据
		fmt.Println("获取到的节点信息为: ", dmap)

		for node, role := range dmap {
			rnode := newClient(node, passwd)
			roles := role
			self := redisDb{rnode}
			d, err := self.nodeSlowlog(node)
			if err != nil {
				log.Println(err)
			}
			if len(d) != 0 {
				err := dbstore.InsertData(node, redis_tag, roles, d)
				if err != nil {
					log.Println("任务执行状态: ", err)
				}
			}
		}
	}
}

func TaskRun() {
	spec := cfg.Section("crontab").Key("crontab.spec").String() //cron表达式
	c := toolbox.NewTask()

	result, err := c.AddFunc(spec, func() {
		exportSlowlog()
	})
	if err != nil {
		log.Fatalln(err)
	}
	log.Printf("启动计划任务ID: %d，计划任务表达式为: [%s]", result, spec)

	c.Start()
	select {} //阻塞主线程停止
}

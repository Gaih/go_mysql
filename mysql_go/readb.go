package mysql_go

import (
	"fmt"
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"sync"
)
var wait sync.WaitGroup
type SqlRes struct{
	//从msg_meassage表中获取的数据
	status int32
	msg_key string
	app_key string
	//formid_collection表中数据
	openid int32
	formid int32
	//拼接的key
	key string

	db *sql.DB
}

var i int32
func (this *SqlRes)DBMessage() {
	sqlRes := new(SqlRes)
	db,err:=sql.Open("mysql","root:123456@tcp(127.0.0.1:3306)/msg?charset=utf8")
	if err != nil {
		fmt.Println("connect db error:",err)
		return
	}
	sqlRes.db = db
	defer db.Close()
	//从msg_meassage表中查询数据
	res,err:=db.Query("select app_key, status,msg_key from msg_message")
	if err != nil {
		fmt.Println("query db error:",err)
		return
	}
	if res != nil {
		for res.Next() {

			var app_key string
			var status int32
			var msg_key string
			res.Columns()
			err:=res.Scan(&app_key,  &status, &msg_key)
			if err!=nil {
				fmt.Println("query db result error:",err)
				return
			}
			fmt.Println("app-key:",app_key,"status:",status,"mag_key",msg_key)
			wait.Add(1)
			go dbCollect(db,msg_key,app_key,status)
		}

	}
	wait.Wait()
	fmt.Println("end--------------",i)
}

func dbCollect(db *sql.DB,msg_key string ,app_key string,status int32) {
	defer wait.Done()
	if status == 3 {

		//wait.Add(1)
		//从formid_collection表中查询数据
		sql:=fmt.Sprint("select openid ,formid from formid_collection where app_key=",app_key)
		res,err:= db.Query(sql)
		if err != nil {
			fmt.Println("query db error:",err)
			return
		}
		for res.Next() {
			i++
			var openid int32
			var formid int32
			res.Columns()
			err =res.Scan(&openid,&formid)
			if err != nil {
				fmt.Println("query db result error:",err)
				return
			}
			fmt.Println("openid:",openid,"formid:",formid)
			connect(msg_key,formid)

		}
	}

}

func connect(msg_key string ,formid int32){
	key :=fmt.Sprint("p_",msg_key,"_",formid)
	fmt.Println("key",key)
}

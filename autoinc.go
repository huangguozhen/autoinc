package autoinc

import (
	"database/sql"
	"errors"
	"log"
	"time"
)

type logger interface {
	Error(error)
}

// Logger Log接口，如果设置了Logger，就使用Logger打印日志，如果没有设置，就使用内置库log打印日志
var Logger logger

// ErrTimeOut 获取uid超时错误
var ErrTimeOut = errors.New("GetUID timeout")

// UID 自增主键结构体
type UID struct {
	db       *sql.DB    // 数据库连接
	business string     // 业务id
	ch       chan int64 // id缓冲池
	min, max int64      // id段最小值，最大值
}

// New 新建实例
// db:数据库连接
// business：业务类型
// len：缓冲池大小(长度可控制缓存中剩下多少id时，去DB中加载)
func New(db *sql.DB, business string, len int) (*UID, error) {
	uid := UID{
		db:       db,
		business: business,
		ch:       make(chan int64, len),
	}

	go uid.produce()
	return &uid, nil
}

// Get 获取自增id,当发生超时，返回错误，避免大量请求阻塞，服务器崩溃
func (u *UID) Get() (int64, error) {
	select {
	case <-time.After(1 * time.Second):
		return 0, ErrTimeOut
	case uid := <-u.ch:
		return uid, nil
	}
}

// produce 产生 ID，当ch达到最大容量时，这个方法会阻塞，直到ch中的id被消费
func (u *UID) produce() {
	u.reLoad()

	for {
		if u.min >= u.max {
			u.reLoad()
		}

		u.min++
		u.ch <- u.min
	}
}

// reLoad 在数据库获取id段，如果失败，会每隔一秒尝试一次
func (u *UID) reLoad() error {
	var err error
	for {
		err = u.getFromDB()
		if err == nil {
			return nil
		}

		// 数据库发生异常，等待一秒之后再次进行尝试
		if Logger != nil {
			Logger.Error(err)
		} else {
			log.Println(err)
		}

		time.Sleep(time.Second)
	}
}

// getFromDB 从数据库获取id段
func (u *UID) getFromDB() error {
	var (
		maxID int64
		step  int64
	)

	tx, err := u.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	row := tx.QueryRow("SELECT max,step FROM t_autoinc WHERE business = ? FOR UPDATE", u.business)
	err = row.Scan(&maxID, &step)
	if err != nil {
		return err
	}

	_, err = tx.Exec("UPDATE t_autoinc SET max = ? WHERE business = ?", maxID+step, u.business)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	u.min = maxID
	u.max = maxID + step
	return nil
}

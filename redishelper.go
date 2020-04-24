package redishelper

import (
	"fmt"
	"time"
	"github.com/gomodule/redigo/redis"
	"github.com/hawkwithwind/gohandler"
)

type GoHandler struct {
	gohandler.GoHandler
}

func (o *GoHandler) Error() bool {
	return o.GoHandler.Error()
}

func (o *GoHandler) Init(errp *error) {
	o.GoHandler.Init(errp)
}

func (o *GoHandler) Set(err error) {
	o.GoHandler.Set(err)
}

type RedisConfig struct {
	Host     string
	Port     string
	Password string
	Db       string
}

var (
	_timeout time.Duration = time.Duration(10) * time.Second
)

func SetDefaultTimeout(timeout int) {
	_timeout = time.Duration(timeout) * time.Second
}

func NewRedisPool(server string, db string, password string) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if len(password) > 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			if _, err := c.Do("SELECT", db); err != nil {
				c.Close()
				return nil, err
			}
			return c, nil
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

func (o *GoHandler) RedisDo(conn redis.Conn, timeout time.Duration,
	cmd string, args ...interface{}) interface{} {
	if o.Error() {
		return nil
	}

	ret, err := redis.DoWithTimeout(conn, _timeout, cmd, args...)
	
	o.Set(err)
	return ret
}

func (o *GoHandler) RedisSend(conn redis.Conn, cmd string, args ...interface{}) {
	if o.Error() {
		return
	}

	o.Set(conn.Send(cmd, args...))
}

func (o *GoHandler) RedisValue(reply interface{}) []interface{} {
	if o.Error() {
		return nil
	}

	var err error

	switch reply := reply.(type) {
	case []interface{}:
		return reply
	case nil:
		err = fmt.Errorf("redis nil returned")
	case redis.Error:
		err = reply
	}

	if err != nil {
		o.Set(err)
	} else {
		o.Set(fmt.Errorf("redis: unexpected type for Values, got type %T", reply))
	}
	
	return nil
}

func (o *GoHandler) RedisString(reply interface{}) string {
	if o.Error() {
		return ""
	}

	switch reply := reply.(type) {
	case []byte:
		return string(reply)
	case string:
		return reply
	case nil:
		return ""
	case redis.Error:
		o.Set(reply)
		return ""
	}

	if !o.Error() {
		o.Set(fmt.Errorf("redis: unexpected type for String, got type %T", reply))
	}
	return ""
}

func (o *GoHandler) RedisMatchCount(conn redis.Conn, keyPattern string) int {
	if o.Error() {
		return 0
	}

	key := "0"
	count := 0
	for !o.Error() {
		ret := o.RedisValue(o.RedisDo(conn, _timeout, "SCAN", key, "MATCH", keyPattern, "COUNT", 1000))
		if len(ret) != 2 {
			o.Set(fmt.Errorf("unexpected redis scan return %v", ret))
			return count
		}

		key = o.RedisString(ret[0])
		resultlist := o.RedisValue(ret[1])

		count += len(resultlist)

		if key == "0" {
			break
		}
	}

	return count
}

func (o *GoHandler) RedisMatchCountCond(conn redis.Conn, keyPattern string, cmp func(redis.Conn, string) bool) int {
	if o.Error() {
		return 0
	}

	key := "0"
	count := 0

	for !o.Error() {
		ret := o.RedisValue(o.RedisDo(conn, _timeout, "SCAN", key, "MATCH", keyPattern, "COUNT", 1000))
		if len(ret) != 2 {
			o.Set(fmt.Errorf("unexpected redis scan return %v", ret))
			return count
		}

		key = o.RedisString(ret[0])
		for _, line := range o.RedisValue(ret[1]) {
			if cmp(conn, string(line.([]uint8))) {
				count += 1
			}
		}

		if key == "0" {
			break
		}
	}

	return count
}

func (o *GoHandler) RedisMatch(conn redis.Conn, keyPattern string) []string {
	if o.Error() {
		return []string{}
	}

	key := "0"
	results := []string{}

	for !o.Error() {
		ret := o.RedisValue(o.RedisDo(conn, _timeout, "SCAN", key, "MATCH", keyPattern, "COUNT", 1000))
		if o.Error() {
			return results
		}

		if len(ret) != 2 {
			o.Set(fmt.Errorf("unexpected redis scan return %v", ret))
			return results
		}
		
		key = o.RedisString(ret[0])
		for _, v := range o.RedisValue(ret[1]) {
			results = append(results, o.RedisString(v))
		}

		if key == "0" {
			break
		}
	}

	return results
}

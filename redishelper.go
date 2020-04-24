package redishelper

import (
	"fmt"
	"time"
	"github.com/gomodule/redigo/redis"
	"github.com/hawkwithwind/errorhandler"
)

type ErrorHandler struct {
	errorhandler.ErrorHandler
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

func (o *ErrorHandler) RedisDo(conn redis.Conn, timeout time.Duration,
	cmd string, args ...interface{}) interface{} {
	if o.Err != nil {
		return nil
	}

	var ret interface{}
	ret, o.Err = redis.DoWithTimeout(conn, _timeout, cmd, args...)

	return ret
}

func (o *ErrorHandler) RedisSend(conn redis.Conn, cmd string, args ...interface{}) {
	if o.Err != nil {
		return
	}

	o.Err = conn.Send(cmd, args...)
}

func (o *ErrorHandler) RedisValue(reply interface{}) []interface{} {
	if o.Err != nil {
		return nil
	}

	switch reply := reply.(type) {
	case []interface{}:
		return reply
	case nil:
		o.Err = fmt.Errorf("redis nil returned")
		return nil
	case redis.Error:
		o.Err = reply
		return nil
	}

	if o.Err == nil {
		o.Err = fmt.Errorf("redis: unexpected type for Values, got type %T", reply)
	}
	return nil
}

func (o *ErrorHandler) RedisString(reply interface{}) string {
	if o.Err != nil {
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
		o.Err = reply
		return ""
	}

	if o.Err == nil {
		o.Err = fmt.Errorf("redis: unexpected type for String, got type %T", reply)
	}
	return ""
}

func (o *ErrorHandler) RedisMatchCount(conn redis.Conn, keyPattern string) int {
	if o.Err != nil {
		return 0
	}

	key := "0"
	count := 0
	for true {
		ret := o.RedisValue(o.RedisDo(conn, _timeout, "SCAN", key, "MATCH", keyPattern, "COUNT", 1000))
		if o.Err == nil {
			if len(ret) != 2 {
				o.Err = fmt.Errorf("unexpected redis scan return %v", ret)
				return count
			}
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

func (o *ErrorHandler) RedisMatchCountCond(conn redis.Conn, keyPattern string, cmp func(redis.Conn, string) bool) int {
	if o.Err != nil {
		return 0
	}

	key := "0"
	count := 0

	for true {
		ret := o.RedisValue(o.RedisDo(conn, _timeout, "SCAN", key, "MATCH", keyPattern, "COUNT", 1000))
		if o.Err == nil {
			if len(ret) != 2 {
				o.Err = fmt.Errorf("unexpected redis scan return %v", ret)
				return count
			}
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

func (o *ErrorHandler) RedisMatch(conn redis.Conn, keyPattern string) []string {
	if o.Err != nil {
		return []string{}
	}

	key := "0"
	results := []string{}

	for true {
		ret := o.RedisValue(o.RedisDo(conn, _timeout, "SCAN", key, "MATCH", keyPattern, "COUNT", 1000))
		if o.Err != nil {
			return results
		}

		if o.Err == nil {
			if len(ret) != 2 {
				o.Err = fmt.Errorf("unexpected redis scan return %v", ret)
				return results
			}
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

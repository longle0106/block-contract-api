package sdk

import (
	"fmt"
	"github.com/globalsign/mgo/bson"
	"math"
	"strconv"
	"time"

	"github.com/labstack/gommon/log"
)

// Marshaler ...
type Marshaler interface {
	MarshalJSON() ([]byte, error)
}

// ISOTime ...
type ISOTime time.Time

// MarshalJSON ...
func (t ISOTime) MarshalJSON() ([]byte, error) {
	stamp := fmt.Sprintf("\"%s\"", time.Time(t).Format("2006-01-02T15:04:05Z"))
	return []byte(stamp), nil
}

// ParseInt convert string to int
func ParseInt(text string, defaultValue int) int {
	if text == "" {
		return defaultValue
	}

	num, err := strconv.Atoi(text)
	if err != nil {
		return defaultValue
	}
	return num
}

// ParseInt64 convert string to int
func ParseInt64(text string, defaultValue int64) int64 {
	if text == "" {
		return defaultValue
	}

	num, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return defaultValue
	}
	return num
}

// IfObj convert string to int
func IfObj(condition bool, defaultValue interface{}, ifFalse interface{}) interface{} {
	if condition {
		return defaultValue
	}

	return ifFalse
}

// IfInt convert string to int
func IfInt(condition bool, defaultValue int, ifFalse int) int {
	if condition {
		return defaultValue
	}

	return ifFalse
}

// IfInt64 convert string to int
func IfInt64(condition bool, defaultValue int64, ifFalse int64) int64 {
	if condition {
		return defaultValue
	}

	return ifFalse
}

// IfStr convert string to int
func IfStr(condition bool, defaultValue string, ifFalse string) string {
	if condition {
		return defaultValue
	}

	return ifFalse
}

// Execute  run func with recover
func Execute(runnable RunnableFn) {
	defer func() {
		if err := recover(); err != nil {
			log.Error(err)
		}
	}()

	if runnable != nil {
		runnable()
	}
}

// RunnableFn ...
type RunnableFn = func()

func deleteEmpty(s []string) []string {
	var result []string
	for _, str := range s {
		if str != "" {
			result = append(result, str)
		}
	}
	return result
}

func NormalizeIntValue(value int, min int, max int) int {
	if value < min {
		return min
	}

	if value > max {
		return max
	}

	return value
}

func ConvertToCode(number int64, length int64, template string) string {
	var result = ""
	var i = int64(0)
	var ln = int64(len(template))
	var capacity = int64(math.Pow(float64(ln), float64(length)))
	number = number % capacity
	for i < length {
		var cur = number % ln
		if i > 0 {
			cur = (cur + int64(result[i-1])) % ln
		}
		result = result + string(template[cur])
		number = number / ln
		i++
	}
	return result
}

func ConvertToBson(ent interface{}) (bson.M, error) {
	if ent == nil {
		return bson.M{}, nil
	}

	sel, err := bson.Marshal(ent)
	if err != nil {
		return nil, err
	}

	obj := bson.M{}
	bson.Unmarshal(sel, &obj)

	return obj, nil
}
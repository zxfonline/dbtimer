// Copyright 2016 zxfonline@sina.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbtimer

import (
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"
	"sync/atomic"

	"fmt"

	"github.com/zxfonline/chanutil"
	"github.com/zxfonline/particletimer"

	"github.com/zxfonline/golog"
	"github.com/zxfonline/taskexcutor"
)

var (
	MAX_TIMER_CHAN_SIZE = 0xFFFF

	logger *golog.Logger = golog.New("DBTimer")

	timer_map map[int64]*RTimer

	timer_ch      chan int64
	nextTimerId   int64
	loadstate     int32
	stopD         chanutil.DoneChan
	tsexcutor     taskexcutor.Excutor
	event_trigger map[Action]*taskexcutor.TaskService
)

func init() {
	timer_map = make(map[int64]*RTimer)
	timer_ch = make(chan int64, MAX_TIMER_CHAN_SIZE)
	stopD = chanutil.NewDoneChan()
	event_trigger = make(map[Action]*taskexcutor.TaskService)
}

//消息执行事件
type Action int32

//定时器数据包
type Msg struct {
	Action Action
	Data   interface{} //事件数据
}

//事件
type RTimer struct {
	Receiver int64 //接受者表示符
	Msg      Msg   //存储的数据
	expired  int64 // 到期的 UTC时间 毫秒
}

type TimerInfo struct {
	Timers []DBTimer
	NextId int64
}

//存档定时器数据 Marshal/unmarshal timers
type DBTimer struct {
	Receiver int64 //接受者表示符
	Expired  int64 // 到期的 UTC时间 毫秒
	Id       int64 //定时器唯一id
	Data     Msg   //存储的数据
}

func Closed() bool {
	return stopD.R().Done()
}

func Close() {
	if !atomic.CompareAndSwapInt32(&loadstate, 1, 2) {
		return
	}
	stopD.SetDone()
}

// 创建延时执行定时器 delay 毫秒 返回定时器唯一id 当返回0表示添加失败
func CreateTimer(userId int64, delay int64, msg Msg) int64 {
	if atomic.LoadInt32(&loadstate) != 1 {
		logger.Warnf("timer no open or closed, msg:%+v", msg)
		return 0
	}
	if len(timer_ch) >= MAX_TIMER_CHAN_SIZE {
		logger.Warnf("timer overflow, cur size: %v, discard timer userId:%v,delay:%v,msg:%+v",
			len(timer_ch), userId, delay, msg)
		return 0
	}
	if _, ok := event_trigger[msg.Action]; !ok {
		logger.Warnf("no found trigger, msg:%+v", msg)
		return 0
	}
	if delay < 0 {
		delay = 0
	}
	expired := particletimer.Current() + delay
	tid := atomic.AddInt64(&nextTimerId, 1)

	particletimer.Add(tid, expired, timer_ch)
	timer_map[tid] = &RTimer{
		Receiver: userId,
		expired:  expired,
		Msg:      msg,
	}
	return tid
}

//注册定时事件执行函数 handler默认第一个参数将由定时器赋值为 *dbtimer.RTimer
func RegistTimerHander(action Action, handler *taskexcutor.TaskService) {
	if handler == nil {
		panic(errors.New("illegal handler error"))
	}
	if _, ok := event_trigger[action]; ok {
		panic(fmt.Errorf("repeat regist handler error,action=%d handler=%+v", action, handler))
	}
	event_trigger[action] = handler
	logger.Infof("Regist action=%d handler=%+v", action, handler)
}

/*加载定时器数据，返回开启定时器函数
*注意:需要在服务器时间同步后再调用返回的函数
 */
func StartTimers(data []byte, excutor taskexcutor.Excutor) func() {
	if !atomic.CompareAndSwapInt32(&loadstate, 0, 1) {
		panic(errors.New("dbtimer closed"))
	}
	if excutor == nil || reflect.ValueOf(excutor).IsNil() {
		panic(errors.New("illegal timer excutor"))
	}
	info := &TimerInfo{
		Timers: make([]DBTimer, 0),
		NextId: 0,
	}
	if data != nil {
		enc := gob.NewDecoder(bytes.NewReader(data))
		if err := enc.Decode(info); err != nil {
			panic(fmt.Errorf("decode timer data error,err:%v", err))
		}
	}

	// reset next timer id
	atomic.StoreInt64(&nextTimerId, info.NextId)
	logger.Infof("load timerNextId=%d", nextTimerId)
	tsexcutor = excutor

	return func() {
		rescheduleTimers(info)
		logger.Infof("start db timer.")
		go working()
	}
}

//开始处理所有的定时器事件转发
func working() {
	defer func() {
		if !Closed() {
			if e := recover(); e != nil {
				logger.Errorf("recover error:%+v", e)
			}
			logger.Infof("restart db timer.")
			go working()
		} else {
			if e := recover(); e != nil {
				logger.Debugf("recover error:%+v", e)
			}
		}
	}()
	for q := false; !q; {
		select {
		case id := <-timer_ch:
			if ts, ex := timer_map[id]; ex {
				CancelTimer(id)
				if tt, ok := event_trigger[ts.Msg.Action]; ok {
					tt.AddArgs(0, ts)
					tsexcutor.Excute(tt)
				} else {
					logger.Warnf("no found trigger, timer:%+v", ts)
				}
			} else {
				CancelTimer(id)
			}
		case <-stopD:
			q = true
		}
	}
}

func rescheduleTimers(info *TimerInfo) {
	for _, item := range info.Timers {
		// reschedule
		particletimer.Add(item.Id, item.Expired, timer_ch)
		timer_map[item.Id] = &RTimer{
			Receiver: item.Receiver,
			Msg:      item.Data,
			expired:  item.Expired,
		}
	}
}

func DumpTimers() (error, []byte) {
	info := TimerInfo{
		Timers: make([]DBTimer, 0, len(timer_map)),
		NextId: nextTimerId,
	}

	for tid, tm := range timer_map {
		if tm.Receiver > 0 {
			entry := DBTimer{
				Receiver: tm.Receiver,
				Expired:  tm.expired,
				Id:       tid,
				Data:     tm.Msg,
			}
			info.Timers = append(info.Timers, entry)
		}
	}

	buffer := new(bytes.Buffer)
	enc := gob.NewEncoder(buffer)
	if err := enc.Encode(info); err != nil {
		return err, nil
	}
	logger.Infof("save timerNextId=%d,size:", nextTimerId, len(info.Timers))
	return nil, buffer.Bytes()
}

//关闭指定id的定时事件
func CancelTimer(timerId int64) {
	particletimer.Del(timerId)
	delete(timer_map, timerId)
}

//指定定时事件是否存在
func TimerExist(timerId int64) bool {
	if timerId == 0 {
		return false
	}
	if _, present := timer_map[timerId]; present {
		return true
	}
	return false
}

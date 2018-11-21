// Copyright 2016 zxfonline@sina.com. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package dbtimer

import (
	"bytes"
	"encoding/gob"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"

	"fmt"

	"github.com/zxfonline/chanutil"
	"github.com/zxfonline/expvar"
	"github.com/zxfonline/particletimer"

	"github.com/zxfonline/golog"
	"github.com/zxfonline/taskexcutor"
)

var (
	MAX_TIMER_CHAN_SIZE = 10000

	logger *golog.Logger = golog.New("DBTimer")

	timer_map   map[int64]*RTimer
	_timer_lock sync.Mutex

	timer_ch      chan int64
	nextTimerId   int64
	loadstate     int32
	stopD         chanutil.DoneChan
	tsexcutor     taskexcutor.Excutor
	event_trigger map[Action]*TimerHandler
)

func init() {
	timer_map = make(map[int64]*RTimer)
	timer_ch = make(chan int64, MAX_TIMER_CHAN_SIZE)
	stopD = chanutil.NewDoneChan()
	event_trigger = make(map[Action]*TimerHandler)
}

type TimerHandler struct {
	callback taskexcutor.CallBack
	ID       interface{}
}

func NewTimerHandler(callback taskexcutor.CallBack) *TimerHandler {
	return &TimerHandler{callback: callback}
}

//消息执行事件
type Action int32

//定时器数据包
type Msg struct {
	Action Action
	//事件数据 需要 gob.Register(Struct{})
	Data interface{}
}

//事件
type RTimer struct {
	Receiver uint64 //接受者标识符
	Msg      Msg    //存储的数据
	expired  int64  // 到期的 UTC时间 毫秒
	//定时任务唯一id
	TimerId int64
	//particleTimer id
	Ptid int64
}

type TimerInfo struct {
	Timers []DBTimer
	NextId int64
}

//存档定时器数据 Marshal/unmarshal timers
type DBTimer struct {
	Receiver uint64 //接受者标识符
	Expired  int64  // 到期的 UTC时间 毫秒
	Id       int64  //定时器唯一id
	Data     Msg    //存储的数据
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

/**
*创建延时执行定时器 delay 毫秒 返回定时器唯一id 当返回0表示添加失败
* 注意：存档导出只会导出设置了userId>0标识符的任务
* 其他任务统一当临时任务处理，不做存档
 */
func CreateTimer(userId uint64, delay int64, msg Msg) int64 {
	if atomic.LoadInt32(&loadstate) != 1 {
		logger.Warnf("timer no open or closed, msg:%+v", msg)
		return 0
	}
	if len(timer_ch) >= MAX_TIMER_CHAN_SIZE {
		logger.Errorf("timer overflow, cur size: %v, discard timer userId:%v,delay:%v,msg:%+v",
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
	_timer_lock.Lock()
	defer _timer_lock.Unlock()
	nextTimerId++
	rtimer := &RTimer{
		Receiver: userId,
		expired:  particletimer.Current() + delay,
		Msg:      msg,
		TimerId:  nextTimerId,
	}
	timer_map[rtimer.TimerId] = rtimer
	rtimer.Ptid = particletimer.Add(rtimer.TimerId, rtimer.expired, timer_ch)
	return rtimer.TimerId
}

//注册定时事件执行函数 handler默认第一个参数将由定时器赋值为 *dbtimer.RTimer
func RegistTimerHander(action Action, handler *TimerHandler) {
	if handler == nil {
		panic(errors.New("illegal handler error"))
	}
	if _, ok := event_trigger[action]; ok {
		panic(fmt.Errorf("repeat regist handler error,action=%d handler=%+v", action, handler))
	}
	event_trigger[action] = handler
	handler.ID = action
	logger.Infof("regist action=%d", action)
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
	if data != nil && len(data) > 0 {
		enc := gob.NewDecoder(bytes.NewReader(data))
		if err := enc.Decode(info); err != nil {
			panic(fmt.Errorf("decode timer data error,err:%v", err))
		}
	}

	_timer_lock.Lock()
	nextTimerId = info.NextId
	// reset next timer id
	logger.Infof("load timerNextId=%d", nextTimerId)
	_timer_lock.Unlock()
	tsexcutor = excutor

	return func() {
		rescheduleTimers(info)
		logger.Infof("start db timer.")
		expvar.RegistChanMonitor("chanDBTimer", timer_ch)
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
		case <-stopD:
			q = true
		case id := <-timer_ch:
			_timer_lock.Lock()
			if ts, ex := timer_map[id]; ex {
				_timer_lock.Unlock()
				//需要玩家手动释放，避免任务执行失败或丢失
				// CancelTimer(id)
				if tt, ok := event_trigger[ts.Msg.Action]; ok {
					task := taskexcutor.NewTaskService(tt.callback, ts)
					task.ID = tt.ID
					tsexcutor.Excute(task)
				} else {
					logger.Warnf("timer no found trigger, timer:%+v", ts)
				}
			} else {
				_timer_lock.Unlock()
				CancelTimer(id)
				logger.Warnf("timer no found, timer:%+v", ts)
			}
		}
	}
}

func rescheduleTimers(info *TimerInfo) {
	for _, item := range info.Timers {
		rtimer := &RTimer{
			Receiver: item.Receiver,
			Msg:      item.Data,
			expired:  item.Expired,
			TimerId:  item.Id,
		}
		timer_map[item.Id] = rtimer
		// reschedule
		rtimer.Ptid = particletimer.Add(item.Id, item.Expired, timer_ch)
	}
}

/**
* 数据库定时任务导出
* 注意：只会导出设置了Receiver>0标识符的任务
* 其他任务统一当临时任务处理，不做存档
 */
func DumpTimers() (error, []byte) {
	_timer_lock.Lock()
	defer _timer_lock.Unlock()
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
	logger.Debugf("save timerNextId=%d,size:%d", nextTimerId, len(info.Timers))
	return nil, buffer.Bytes()
}

//关闭指定id的定时事件
func CancelTimer(timerId int64) {
	if timerId == 0 {
		return
	}
	_timer_lock.Lock()
	defer _timer_lock.Unlock()
	if _, present := timer_map[timerId]; !present {
		return
	}
	delete(timer_map, timerId)
	particletimer.Del(timerId)
}

//指定定时事件是否存在
func TimerExist(timerId int64, checkParticle bool) bool {
	if timerId == 0 {
		return false
	}
	_timer_lock.Lock()
	defer _timer_lock.Unlock()
	rtimer, present := timer_map[timerId]
	if present && checkParticle {
		if ok := particletimer.ParticleTimerExist(rtimer.TimerId); !ok {
			rtimer.Ptid = particletimer.Add(rtimer.TimerId, rtimer.expired, timer_ch)
			logger.Warnf("particle timer not exits,fix timerId:%d", timerId)
		}
	}
	return present
}

func GetTimerInfo(timerId int64) *RTimer {
	if timerId == 0 {
		return nil
	}
	_timer_lock.Lock()
	defer _timer_lock.Unlock()
	return timer_map[timerId]
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/alicebob/miniredis/v2"
	mapset "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/jinzhu/gorm"
	_ "github.com/mattn/go-sqlite3"
	"github.com/sjqzhang/bus"
	"gopkg.in/natefinch/lumberjack.v2"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"
)

const WEBSOCKET_MESSAGE = "WEBSOCKET_MESSAGE"

var hubLocal = newHub()
var logger *log.Logger

var redisClient *miniredis.Miniredis

var db *gorm.DB

type Response struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Msg  string      `json:"message"`
}

type NocIncident struct {
	ID              int                      `json:"id"`
	IncidentID      string                   `json:"incident_id"`
	Title           string                   `json:"title"`
	StartTime       int64                    `json:"start_time"`
	EndTime         int64                    `json:"end_time"`
	Duration        int                      `json:"duration"`
	EscalationTime  int64                    `json:"escalation_time"`
	Region          []string                 `json:"region" gorm:"-"`
	ProductLine     string                   `json:"product_line"`
	Lvl2Team        string                   `json:"lvl2_team"`
	Lvl3Team        string                   `json:"lvl3_team"`
	Metric          string                   `json:"metric"`
	Record          []map[string]interface{} `json:"record" gorm:"-"`
	ServiceCmdbName string                   `json:"service_cmdb_name"`
	Operator        string                   `json:"operator"`
	ReportURL       string                   `json:"report_url"`
	GroupName       string                   `json:"group_name"`
	Records         string                   `json:"-" gorm:"records"`
	Regions         string                   `json:"-" gorm:"regions"`
}

func init() {
	var err error

	db, err = gorm.Open("sqlite3", "test.db")
	if err != nil {
		panic(err)
	}
	db.AutoMigrate(&NocIncident{})
	redisClient, err = miniredis.Run()
	if err != nil {
		panic(err)
	}
	bus.Subscribe(WEBSOCKET_MESSAGE, 50, func(ctx context.Context, message interface{}) {
		if message == nil {
			return
		}
		if v, ok := message.(Subscription); ok {
			hubLocal.SendMessage(v)
		}
	})
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		// 校验请求的来源，可以根据需求自定义逻辑
		return true
	},
}

// 订阅消息的结构体
type Subscription struct {
	Action  string      `json:"action"`
	Topic   string      `json:"topic"`
	ID      string      `json:"id"`
	Message interface{} `json:"message"`
}

type hub struct {
	subs  sync.Map
	conns mapset.Set
}

func newHub() *hub {
	return &hub{
		subs:  sync.Map{},
		conns: mapset.NewSet(),
	}
}

func (h *hub) SendMessage(subscription Subscription) {
	key := fmt.Sprintf("%s_$_%s", subscription.Topic, subscription.ID)
	if m, ok := h.subs.Load(key); ok {
		for _, conn := range m.(mapset.Set).ToSlice() {
			conn.(*websocket.Conn).SetWriteDeadline(time.Now().Add(time.Second * 2))
			err := conn.(*websocket.Conn).WriteJSON(subscription)
			if err != nil {
				if _, ok := err.(*websocket.CloseError); ok || err == websocket.ErrCloseSent {
					h.Unsubscribe(conn.(*websocket.Conn), subscription)
					h.RemoveFailedConn(conn.(*websocket.Conn))
				}
			}

		}
	}

}

func (h *hub) Run() {
	for {
		logger.Println("Goroutines", runtime.NumGoroutine(), "Cardinality", h.conns.Cardinality())
		time.Sleep(time.Second * 10)
		for _, c := range h.conns.ToSlice() {
			c.(*websocket.Conn).SetWriteDeadline(time.Now().Add(time.Second * 2))
			err := c.(*websocket.Conn).WriteMessage(websocket.PingMessage, []byte{})
			if err != nil {
				if _, ok := err.(*websocket.CloseError); ok || err == websocket.ErrCloseSent {
					h.RemoveFailedConn(c.(*websocket.Conn))
				}
			}
		}
	}
}

func (h *hub) Subscribe(conn *websocket.Conn, subscription Subscription) {
	key := fmt.Sprintf("%s_$_%s", subscription.Topic, subscription.ID)
	if m, ok := h.subs.Load(key); ok {
		m.(mapset.Set).Add(conn)
		h.subs.Store(key, m)
	} else {
		m := mapset.NewSet()
		m.Add(conn)
		h.subs.Store(key, m)
	}
	h.conns.Add(conn)
}

// unsubscribe from a topic
func (h *hub) Unsubscribe(conn *websocket.Conn, subscription Subscription) {
	key := fmt.Sprintf("%s_$_%s", subscription.Topic, subscription.ID)
	if m, ok := h.subs.Load(key); ok {
		m.(mapset.Set).Remove(conn)
		if m.(mapset.Set).Cardinality() == 0 {
			h.subs.Delete(key)
		} else {
			h.subs.Store(key, m)
		}
	}
}

// unsubscribe from a topic
func (h *hub) RemoveFailedConn(conn *websocket.Conn) {
	go func() {
		h.subs.Range(func(key, value interface{}) bool {
			for _, con := range value.(mapset.Set).ToSlice() {
				c := con.(*websocket.Conn)
				if c == conn {
					if m, ok := h.subs.Load(key); ok {
						m.(mapset.Set).Remove(conn)
						h.subs.Store(key, m)
					}
				}
			}
			return true
		})
		h.conns.Remove(conn)
	}()
}

func readMessages(conn *websocket.Conn) {

	defer func() {
		if err := recover(); err != nil {
			hubLocal.RemoveFailedConn(conn)
		}
	}()
	defer conn.Close()
	for {
		// 读取客户端发来的消息
		//conn.SetReadDeadline(time.Now().Add(time.Second * 10))
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			if _, ok := err.(*websocket.CloseError); ok {
				hubLocal.RemoveFailedConn(conn)
				return
			}
		}

		switch messageType {

		case websocket.CloseMessage:
			return
		case websocket.PingMessage:
			conn.WriteMessage(websocket.PongMessage, nil)
		case websocket.TextMessage:
			// 解析订阅消息
			var subscription Subscription
			err = json.Unmarshal(message, &subscription)
			if err != nil {
				logger.Println("Failed to parse subscription message:", err)
				continue
			}
			handleMessages(conn, subscription)

			logger.Println("messageType", messageType, "message", message)

		}

	}
}

func handleMessages(conn *websocket.Conn, subscription Subscription) {
	hubLocal.Subscribe(conn, subscription)
} // 获取订阅

var addr = flag.String("addr", ":8866", "http service address")

func main() {

	logFile := &lumberjack.Logger{
		Filename:   "gin.log", // 日志文件名称
		MaxSize:    100,       // 每个日志文件的最大大小（以MB为单位）
		MaxBackups: 5,         // 保留的旧日志文件的最大个数
		MaxAge:     30,        // 保留的旧日志文件的最大天数
		Compress:   true,      // 是否压缩旧的日志文件
	}
	logger = log.New(logFile, "[WS] ", log.LstdFlags)
	go hubLocal.Run()
	router := gin.Default()
	router.Use(gin.LoggerWithConfig(gin.LoggerConfig{
		Output: logFile,
	}))
	gin.DefaultErrorWriter = logFile
	wd, _ := os.Getwd()
	os.Chdir(wd + "/examples/message")
	router.GET("/", func(c *gin.Context) {
		body, err := ioutil.ReadFile("home.html")
		if err != nil {
			logger.Println(err)
			return
		}
		c.Writer.Write(body)

	})

	router.GET("/ws", func(c *gin.Context) {
		conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
		if err != nil {
			logger.Println("Failed to upgrade connection:", err)
			return
		}
		go readMessages(conn)
	})

	router.GET("/ws/noc_incident", func(c *gin.Context) {
		var incidents []NocIncident
		// 取当天的数据
		today := time.Now().UTC()
		todayStart := time.Date(today.Year(), today.Month(), today.Day(), 0, 0, 0, 0, time.UTC)
		tomorrowStart := todayStart.Add(24 * time.Hour)
		db.Where("start_time >= ? AND start_time < ?", todayStart.Unix(), tomorrowStart.Unix()).Find(&incidents)
		c.JSON(200, Response{
			Code: 200,
			Data: incidents,
			Msg:  "ok",
		})

	})

	router.POST("/ws/noc_incident", func(c *gin.Context) {

		var incident NocIncident

		var subscription Subscription
		err := c.BindJSON(&incident)
		if err != nil {
			logger.Println("Failed to parse subscription message:", err)
			return
		}
		if len(incident.Region) > 0 {
			v, err := json.Marshal(incident.Region)
			if err == nil {
				incident.Regions = string(v)
			}
		}
		if len(incident.Record) > 0 {
			v, err := json.Marshal(incident.Record)
			if err == nil {
				incident.Regions = string(v)
			}
		}
		var oldIncident NocIncident
		if db.First(&oldIncident, "incident_id=?", incident.IncidentID).Error != nil {
			db.Create(&incident)
		} else {
			if oldIncident.ID != 0 {
				incident.ID = oldIncident.ID
				db.Save(incident)
			}
		}
		subscription.Topic = "noc_incident"
		subscription.Message = incident
		logger.Println(fmt.Sprintf("订阅消息：%v", subscription))
		bus.Publish(WEBSOCKET_MESSAGE, subscription)
		c.JSON(http.StatusOK, subscription)

	})

	router.POST("/ws/api", func(c *gin.Context) {

		var subscription Subscription
		err := c.BindJSON(&subscription)
		if err != nil {
			logger.Println("Failed to parse subscription message:", err)
			return
		}
		logger.Println(fmt.Sprintf("订阅消息：%v", subscription))
		bus.Publish(WEBSOCKET_MESSAGE, subscription)
		c.JSON(http.StatusOK, subscription)

	})

	router.Run(*addr)

}

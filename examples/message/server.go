package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
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

func init() {

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
		time.Sleep(time.Second*10)
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
	os.Chdir(wd + "/examples/message")package main

	import (
		"context"
	"encoding/json"
	"flag"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
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

	func init() {

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
			time.Sleep(time.Second*10)
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
		router.POST("/wsapi", func(c *gin.Context) {

			var subscription Subscription
			err := c.BindJSON(&subscription)
			if err != nil {
				logger.Println("Failed to parse subscription message:", err)
				return
			}
			logger.Println(fmt.Sprintf("订阅消息：%s", subscription))
			bus.Publish(WEBSOCKET_MESSAGE, subscription)
			c.JSON(http.StatusOK, subscription)

		})

		router.Run(*addr)

	}

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
	router.POST("/wsapi", func(c *gin.Context) {

		var subscription Subscription
		err := c.BindJSON(&subscription)
		if err != nil {
			logger.Println("Failed to parse subscription message:", err)
			return
		}
		logger.Println(fmt.Sprintf("订阅消息：%s", subscription))
		bus.Publish(WEBSOCKET_MESSAGE, subscription)
		c.JSON(http.StatusOK, subscription)

	})

	router.Run(*addr)

}

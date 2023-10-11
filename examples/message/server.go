package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"

	mapset "github.com/deckarep/golang-set"
	"github.com/gorilla/websocket"
	"github.com/sjqzhang/bus"
)

const WEBSOCKET_MESSAGE = "WEBSOCKET_MESSAGE"

var hubLocal = newHub()

func init() {

	bus.Subscribe(WEBSOCKET_MESSAGE, 3, func(ctx context.Context, message interface{}) {
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
	key := fmt.Sprintf("%s$%s", subscription.Topic, subscription.ID)
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
		fmt.Println("Cardinality conn", h.conns.Cardinality())
		if m, ok := h.subs.Load("topicA$1"); ok {
			fmt.Println("Cardinality", m.(mapset.Set).Cardinality())
		}
		time.Sleep(time.Second)
	}
}

func (h *hub) Subscribe(conn *websocket.Conn, subscription Subscription) {
	key := fmt.Sprintf("%s$%s", subscription.Topic, subscription.ID)
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
	key := fmt.Sprintf("%s$%s", subscription.Topic, subscription.ID)
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

// 存储所有订阅的连接和订阅消息
var subscriptions = struct {
	sync.RWMutex
	conns map[*websocket.Conn]Subscription
}{
	conns: make(map[*websocket.Conn]Subscription),
}

func wsHandler(w http.ResponseWriter, r *http.Request) {

	defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
	// 升级HTTP连接为WebSocket连接
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Println("Failed to upgrade connection:", err)
		return
	}

	// 处理WebSocket连接
	go readMessages(conn)
	//go handleMessages(conn)
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
			fmt.Println("messageType", messageType, "message", message)
		case websocket.TextMessage:
			// 解析订阅消息
			var subscription Subscription
			err = json.Unmarshal(message, &subscription)
			if err != nil {
				fmt.Println("Failed to parse subscription message:", err)
				continue
			}
			handleMessages(conn, subscription)

			fmt.Println("messageType", messageType, "message", message)

		}

	}
}

func handleMessages(conn *websocket.Conn, subscription Subscription) {
	hubLocal.Subscribe(conn, subscription)
} // 获取订阅

func serveApi(w http.ResponseWriter, r *http.Request) {

	//get body message
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Println(err)
		return
	}
	var subscription Subscription
	err = json.Unmarshal(body, &subscription)
	if err != nil {
		log.Println(err)
		return
	}
	//hubLocal.SendMessage(subscription)
	bus.Publish(WEBSOCKET_MESSAGE, subscription)
	w.Write([]byte("ok"))
}

func serveHome(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL)
	if r.URL.Path != "/" {
		http.Error(w, "Not found", http.StatusNotFound)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	http.ServeFile(w, r, "home.html")
}

var addr = flag.String("addr", ":8866", "http service address")

func main() {

	go func() {
		for {

			time.Sleep(1 * time.Second)
			fmt.Println(runtime.NumGoroutine())

		}
	}()

	go hubLocal.Run()

	go func() {
		for {
			bus.Publish(WEBSOCKET_MESSAGE, Subscription{
				Action:  "subscribe",
				Topic:   "topicA",
				ID:      "123",
				Message: "hello",
			})
			time.Sleep(1 * time.Second)
		}
	}()

	os.Chdir("examples/message")
	fmt.Println(os.Getwd())
	flag.Parse()

	http.HandleFunc("/", serveHome)

	http.HandleFunc("/wsapi", serveApi)

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		wsHandler(w, r)
	})
	server := &http.Server{
		Addr:              *addr,
		ReadHeaderTimeout: 3 * time.Second,
	}
	err := server.ListenAndServe()
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

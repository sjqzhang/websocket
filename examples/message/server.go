package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
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

var hub = NewHub()

func init() {

	bus.Subscribe(WEBSOCKET_MESSAGE, 3, func(ctx context.Context, message interface{}) {
		if message == nil {
			return
		}
		if v, ok := message.(Subscription); ok {
			hub.SendMessage(v)
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

type Hub struct {
	subs sync.Map
}

func NewHub() *Hub {
	return &Hub{
		subs: sync.Map{},
	}
}

func (h *Hub) SendMessage(subscription Subscription) {
	key:=fmt.Sprintf("%s$%s",subscription.Topic,subscription.ID)
	if m, ok := h.subs.Load(key); ok {
		for conn:= range m.(mapset.Set).Iter() {
			conn.(*websocket.Conn).WriteJSON(subscription)
		}
	}

}

func (h *Hub) Subscribe(conn *websocket.Conn, subscription Subscription) {
	key := fmt.Sprintf("%s$%s", subscription.Topic, subscription.ID)
	if m, ok := h.subs.Load(key); ok {
		m.(mapset.Set).Add(conn)
		h.subs.Store(key, m)
	} else {
		m := mapset.NewSet()
		m.Add(conn)
		h.subs.Store(key, m)
	}
}

// unsubscribe from a topic
func (h *Hub) Unsubscribe(conn *websocket.Conn, subscription Subscription) {
	key := fmt.Sprintf("%s$%s", subscription.Topic, subscription.ID)
	if m, ok := h.subs.Load(key); ok {
		m.(mapset.Set).Remove(conn)
		h.subs.Store(key, m)
	}
}

// 存储所有订阅的连接和订阅消息
var subscriptions = struct {
	sync.RWMutex
	conns map[*websocket.Conn]Subscription
}{
	conns: make(map[*websocket.Conn]Subscription),
}

// 处理订阅消息的接口
type SubscriptionHandler interface {
	Handle(conn *websocket.Conn, subscription Subscription)
}

// TopicA处理器
type TopicAHandler struct{}

func (h *TopicAHandler) Handle(conn *websocket.Conn, subscription Subscription) {

	for {
		err := conn.WriteJSON(map[string]string{
			"topic": subscription.Topic,
			"id":    subscription.ID,
		})
		if err != nil {
			break
		}
		time.Sleep(1 * time.Second)
	}

	return
}

// TopicB处理器
type TopicBHandler struct{}

func (h *TopicBHandler) Handle(conn *websocket.Conn, subscription Subscription) {
	// 在这里实现TopicB的处理逻辑
	for {
		err := conn.WriteJSON(map[string]string{
			"topic": subscription.Topic,
			"id":    subscription.ID,
		})
		if err != nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	log.Printf("TopicB: Received message for topic %s, ID %s\n", subscription.Topic, subscription.ID)
	// TODO: 根据topic+id确定唯一的对象是否发生变化，如果发生变化，将消息推送给前端

	// 返回处理结果给前端
	return
}

// 订阅消息处理器映射表
var subscriptionHandlers = map[string]SubscriptionHandler{
	"topicA": &TopicAHandler{},
	"topicB": &TopicBHandler{},
}

func wsHandler(w http.ResponseWriter, r *http.Request) {
	// 升级HTTP连接为WebSocket连接
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Failed to upgrade connection:", err)
		return
	}

	// 处理WebSocket连接
	go readMessages(conn)
	//go handleMessages(conn)
}

func readMessages(conn *websocket.Conn) {
	defer conn.Close()

	for {
		// 读取客户端发来的消息
		//conn.SetReadDeadline(time.Now().Add(time.Second * 10))
		messageType, message, err := conn.ReadMessage()

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
				log.Println("Failed to parse subscription message:", err)
				continue
			}
			handleMessages(conn, subscription)

			log.Println("messageType", messageType, "message", message)

		}

	}
}

func handleMessages(conn *websocket.Conn, subscription Subscription) {


	hub.Subscribe(conn, subscription)

	//subscriptions.RLock()
	//subscription := subscriptions.conns[conn]
	//subscriptions.RUnlock()

	//handler, ok := subscriptionHandlers[subscription.Topic]
	//if !ok {
	//	log.Println("Unsupported topic:", subscription.Topic)
	//	return
	//}
	//
	//// 调用处理器处理订阅消息
	//handler.Handle(conn, subscription)

} // 获取订阅

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

var addr = flag.String("addr", ":8080", "http service address")

func main() {

	go func() {
		for {

			time.Sleep(1 * time.Second)
			fmt.Println(runtime.NumGoroutine())

		}
	}()


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

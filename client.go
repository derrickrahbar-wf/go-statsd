package statsd

import (
	"fmt"
	"net"

	"appengine"
	"appengine/socket"
)

var lock = make(chan int, 1)

// The StatsdClient type defines the relevant properties of a StatsD connection.
type StatsdClient struct {
	Host string
	Port int
	conn net.Conn
}

// Factory method to initialize udp connection
//
// Usage:
//
//     import "statsd"
//     client := statsd.New('localhost', 8125)
func New(c appengine.Context, host string, port int) *StatsdClient {
	client := StatsdClient{Host: host, Port: port}
	client.EstablishConnection(c)
	return &client
}

// Method to open udp connection, called by default client factory
func (client *StatsdClient) EstablishConnection(c appengine.Context) {
	connectionString := fmt.Sprintf("%s:%d", client.Host, client.Port)
	conn, err := socket.Dial(c, "udp", connectionString)
	if err != nil {
		c.Errorf("Connection Error")
	}
	client.conn = conn
}

// Method to close udp connection
func (client *StatsdClient) Close() {
	client.conn.Close()
}

// Log timing information (in milliseconds) without sampling
//
// Usage:
//
//     import (
//         "statsd"
//         "time"
//     )
//
//     client := statsd.New('localhost', 8125)
//     t1 := time.Now()
//     expensiveCall()
//     t2 := time.Now()
//     duration := int64(t2.Sub(t1)/time.Millisecond)
//     client.Timing("foo.time", duration)
func (client *StatsdClient) Timing(c appengine.Context, stat string, time int64) {
	updateString := fmt.Sprintf("%d|ms", time)
	stats := map[string]string{stat: updateString}
	client.Send(c, stats)
}

// Increments one stat counter without sampling
//
// Usage:
//
//     import "statsd"
//     client := statsd.New('localhost', 8125)
//     client.Increment('foo.bar')
func (client *StatsdClient) Increment(c appengine.Context, stat string) {
	updateString := fmt.Sprintf("%d|c", 1)
	stats := map[string]string{stat: updateString}
	client.Send(c, stats)
}

// Decrements one stat counter without sampling
//
// Usage:
//
//     import "statsd"
//     client := statsd.New('localhost', 8125)
//     client.Decrement('foo.bar')
func (client *StatsdClient) Decrement(c appengine.Context, stat string) {
	updateString := fmt.Sprintf("%d|c", -1)
	stats := map[string]string{stat: updateString}
	client.Send(c, stats)
}

// Arbitrarily updates a list of stats by a delta
// func (client *StatsdClient) UpdateStats(stats []string, delta int, sampleRate ) {
// 	statsToSend := make(map[string]string)
// 	for _, stat := range stats {
// 		updateString := fmt.Sprintf("%d|c", delta)
// 		statsToSend[stat] = updateString
// 	}
// 	client.Send(statsToSend, sampleRate)
// }

// func (client *StatsdClient) SendData(c appengine.Context) {
// 	<-lock

// 	lock <- 1
// }

// Sends data to udp statsd daemon
func (client *StatsdClient) Send(c appengine.Context, data map[string]string) {
	for k, v := range data {
		update_string := fmt.Sprintf("%s:%s", k, v)
		_, err := fmt.Fprintf(client.conn, update_string)
		if err != nil {
			c.Errorf("Error sending data")
		}
	}
}

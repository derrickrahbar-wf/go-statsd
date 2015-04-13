package statsd

import (
	"fmt"
	"net"

	"appengine"
	"appengine/socket"
)

var (
	lock         = make(chan int, 1)
	requestLimit = 100
	requestCount = 0
	bufferSize   = 512
	Conn         net.Conn
)

// The StatsdClient type defines the relevant properties of a StatsD connection.
type StatsdClient struct {
	Host     string
	Port     string
	buffer   string
	reqs     chan string
	shutdown chan bool
}

func init() {
	lock <- 1
}

// Factory method to initialize udp connection
//
// Usage:
//
//     import "statsd"
//     client := statsd.New('localhost', 8125)
func New(c appengine.Context, host string, port string) *StatsdClient {
	client := StatsdClient{Host: host,
		Port:     port,
		reqs:     make(chan string),
		shutdown: make(chan bool)}

	if Conn == nil {
		client.EstablishConnection(c)
	}
	go client.Send(c)
	return &client
}

// Method to open udp connection, called by default client factory
func (client *StatsdClient) EstablishConnection(c appengine.Context) {
	connectionString := fmt.Sprintf("%s:%s", client.Host, client.Port)
	var err error
	Conn, err = socket.Dial(c, "udp", connectionString)
	if err != nil {
		c.Errorf("Connection Error")
	}
}

// Method to close udp connection
func (client *StatsdClient) Close() {
	client.shutdown <- true
	Conn.Close()
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
func (client *StatsdClient) Timing(stat string, time int64) {
	updateString := fmt.Sprintf("%s:%d|ms", stat, time)
	client.reqs <- updateString
}

// Increments one stat counter without sampling
//
// Usage:
//
//     import "statsd"
//     client := statsd.New('localhost', 8125)
//     client.Increment('foo.bar')
func (client *StatsdClient) Increment(stat string) {
	updateString := fmt.Sprintf("%s:%d|c", stat, 1)
	client.reqs <- updateString
}

// Decrements one stat counter without sampling
//
// Usage:
//
//     import "statsd"
//     client := statsd.New('localhost', 8125)
//     client.Decrement('foo.bar')
func (client *StatsdClient) Decrement(stat string) {
	updateString := fmt.Sprintf("%s:%d|c", stat, -1)
	client.reqs <- updateString
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

// Sends the data in client buffer to statsd. Since there is a shared
// connection between all clients, this function is protected buy
// a lock
func (client *StatsdClient) flush(c appengine.Context) {
	<-lock
	data := client.buffer
	if requestCount == 100 {
		client.EstablishConnection(c)
	}
	_, err := fmt.Fprintf(Conn, data)
	if err != nil {
		client.EstablishConnection(c)
		_, err := fmt.Fprintf(Conn, data)
		if err != nil {
			c.Errorf("Error sending data")
		}
	}
	client.buffer = ""
	requestCount += 1
	lock <- 1

}

// Sends data to udp statsd daemon
func (client *StatsdClient) Send(c appengine.Context) {
	for {
		select {
		case data := <-client.reqs:
			newLine := data + "\n"
			if len(client.buffer)+len(newLine) > bufferSize {
				client.flush(c)
			}
			client.buffer += (newLine)
			if len(client.buffer) >= bufferSize {
				client.flush(c)
			}

		case <-client.shutdown:
			if len(client.buffer) > 0 {
				client.flush(c)
			}
			break
		}
	}
}

// func (client *StatsdClient) Send(c appengine.Context) {
// 	for k, v := range data {
// 		if requestCount == 100 {
// 			client.EstablishConnection(c)
// 		}
// 		update_string := fmt.Sprintf("%s:%s", k, v)
// 		_, err := fmt.Fprintf(Conn, update_string)
// 		if err != nil {
// 			client.EstablishConnection(c)
// 			_, err := fmt.Fprintf(Conn, update_string)
// 			if err != nil {
// 				c.Errorf("Error sending data")
// 			}
// 		}
// 		requestCount += 1
// 	}
// }

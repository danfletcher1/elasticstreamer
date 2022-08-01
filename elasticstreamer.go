/*
TODO
*/

/*
elasticstream simplifies connection to elastic server and posting messages. Whilst much more functionality
is available here we are only interested in the queueing and posting of large numbers of messsages.

To improve throughput it always batch process messages and use mulitple workers threads simultainiously. It
blocks messages when no more buffer space is available, and handles elastic 426 busy message

	estream := elasticstreamer.NewConnection(AddHost("127.0.0.1"))
	defer estream.Close()

	estream.Submit(
		"MyIndex",
		"MyType",
		"MyID",
		myStruct{
			str: "1",
			num: 22,
			block: 0x1234567890AABBCCDDEEFF
		}
	)

The struct will be posted as a JSON message into the elastic index with the given type and ID
you should predefine the elastic template.
*/
package elasticstreamer

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/olivere/elastic.v6"
)

// Client stored the information for each connnection and is returned upon creating a new connection
type Client struct {
	hosts           []string           // Elastic hosts
	context         context.Context    //  Starting with elastic.v5, you must pass a context to execute each service
	cancelFunc      context.CancelFunc // Context cancel function
	client          *elastic.Client    // Elastic client connection
	sniff           bool               // Elastic option sniff
	workers         int                // Post using this many workers processes simultainiously
	bulksize        int                // Post if message count reaches this size
	flushinterval   time.Duration      // Post when timeout reaches this point
	maxsize         int                // Post when total messages reach this MB limit
	inserttime      int64              // accumelated insert time
	postingcallback func(*Client, int64, []elastic.BulkableRequest, *elastic.BulkResponse, error)
	errorcallback   func(*Client)
	bulkService     *elastic.BulkProcessor
	wait            int
}

// Option are functions that can be applied to a new connection
type Option func(*Client) error

// NewConnection returns a new configured client. The only mandatory options is AddHost
func NewConnection(options ...Option) (*Client, error) {
	// Create a blank client

	var (
		t, _      = time.ParseDuration("10s")
		newClient = Client{
			workers:       4,
			wait:          -4,
			sniff:         false,
			bulksize:      2000,
			flushinterval: t,
			maxsize:       100,
		}
		err error
		x   Option
	)

	// Go through all the options adding to the client struct
	for _, x = range options {
		err = x(&newClient)
		if err != nil {
			return nil, err
		}
	}

	// Connect to elastic
	err = newClient.connect()
	if err != nil {
		return nil, err
	}

	// Return a sucessful client connection
	return &newClient, err
}

// AddHost option, adds a host to the list of available hosts, command may be repeated
func AddHost(host string) Option {
	return func(c *Client) error {
		if len(host) < 1 {
			return fmt.Errorf("ERR, host string is less then 1 character")
		}
		c.hosts = append(c.hosts, host)
		return nil
	}
}

// Sniff option, sets the elastic sniff option to detect the hostname automatically. If elastic is running in docker best avoid.
func Sniff() Option {
	return func(c *Client) error {
		c.sniff = true
		return nil
	}
}

// Workers option, sets how many simultaninious posting processes will run
func Workers(i int) Option {
	return func(c *Client) error {
		if i < 1 {
			return fmt.Errorf("ERR, workers option is set less than 1")
		}
		c.workers = i
		c.wait = -i
		return nil
	}
}

// Bulksize option, sets the accumelated number of messages queued before posting is forced
func BulkSize(i int) Option {
	return func(c *Client) error {
		if i < 1 {
			return fmt.Errorf("ERR, bulksize option is set less than 1")
		}
		c.bulksize = i
		return nil
	}
}

// Flushinterval option, sets the time limit point before posting is forced
func FlushInterval(s string) Option {
	return func(c *Client) error {
		var (
			t   time.Duration
			err error
		)
		if t, err = time.ParseDuration(s); err != nil {
			return fmt.Errorf("ERR, flushinterval option was not understood, error was: %v", err)
		}
		c.flushinterval = t
		return nil
	}
}

// Maxsize option, sets the maximum accumelated size of messages queued before posting is forced
func Maxsize(i int) Option {
	return func(c *Client) error {
		if i < 1 {
			return fmt.Errorf("ERR, maxsize option is set to less than 1")
		}
		c.maxsize = i
		return nil
	}
}

// PostingCallback sets a function thats called every time a post completes sucessfully
// or not. Useful for reporting back success/failure or stats
func PostingCallback(f func(c *Client, executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error)) Option {
	return func(c *Client) error {
		if f == nil {
			return fmt.Errorf("ERR, posting callback option, function not set")
		}
		c.postingcallback = f
		return nil
	}
}

// ErrorCallback sets a function that is called every time a single message fails
func ErrorCallback(f func(*Client)) Option {
	return func(c *Client) error {
		if f == nil {
			return fmt.Errorf("ERR, error callback option, function not set")
		}
		c.errorcallback = f
		return nil
	}
}

// stats := bulkService.Stats()
// thisStats.CountEsBatches = int(stats.Flushed)
// thisStats.CountEsDocCreate = int(stats.Indexed)
// thisStats.CountEsDocErrors = int(stats.Failed)

// Connects to the Elastic service, and starts the queuer
func (c *Client) connect() error {

	var (
		nodeList []string
		err      error
	)

	if len(c.hosts) == 0 {
		return fmt.Errorf("ERR, No hosts listed")
	}

	// Starting with elastic.v5, you must pass a context to execute each service
	if c.context != nil {
		return fmt.Errorf("ERR, Already connected")
	}
	c.context, c.cancelFunc = context.WithCancel(context.Background())

	// Create the connection to the elastic IP's given
	// NewClient also does health checks
	c.client, err = elastic.NewClient(
		elastic.SetURL(nodeList...),
		elastic.SetSniff(c.sniff),
	)

	if err != nil {
		c.context = nil
		return fmt.Errorf("ERR, Connecting to SIP Elastic node %v, returned Error: %v", nodeList, err)
	}

	// Start elastic bulk services
	c.bulkService, err = c.client.BulkProcessor().
		Before(func(executionId int64, requests []elastic.BulkableRequest) {
			// Add a wait
			c.wait++
			//thisStats.TimeEsInsert = thisStats.TimeEsInsert - float64(time.Now().UnixNano())/1000000000
		}).
		After(func(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
			// remove a wait
			c.wait--
			//thisStats.TimeEsInsert = thisStats.TimeEsInsert + float64(time.Now().UnixNano())/1000000000
			c.postingcallback(c, executionId, requests, response, err)
		}).
		RetryItemStatusCodes(429).                                                     // only retry on busy code
		Backoff(elastic.NewExponentialBackoff(time.Duration(5), time.Duration(3645))). // will retry ever 7 times total 1.5h
		Workers(c.workers).
		BulkActions(c.bulksize).
		FlushInterval(c.flushinterval).
		BulkSize(c.maxsize << 20).
		Stats(true).
		Do(c.context)

	if err != nil {
		return fmt.Errorf("ERR, Elastic Bulk Service unable able to start, error was: %v", err)
	}
	return nil
}

// Submit will add a message to the buffer queue. Submit will block if the queue is full.
func (c *Client) Submit(Index, Type, ID string, Doc interface{}) {

	//If we are instructed to wait as all workers are busy then wait
	for c.wait >= 0 {
		time.Sleep(time.Millisecond * 500)
	}

	// Add to the bulk queue
	c.bulkService.Add(elastic.NewBulkIndexRequest().Index(Index).Type(Type).Id(ID).Doc(Doc))
}

// Close will stop the service and close connection to elastic server after flushing all messages
func (c *Client) Close() {
	c.bulkService.Flush()
	c.bulkService.Close()
	c.cancelFunc()
}

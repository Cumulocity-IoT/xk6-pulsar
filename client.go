// Package pulsar xk6 extenstion to suppor pulsar with k6
package pulsar

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"os"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/grafana/sobek"
	"go.k6.io/k6/js/common"
	"go.k6.io/k6/js/modules"
)

type client struct {
	vu              modules.VU
	initEnvironment *common.InitEnvironment
	metricsLabels   pulsarMetricsLabels
	metrics         *pulsarMetrics
	conf            conf
	pulsarClient    pulsar.Client

	pulsarProducer pulsar.Producer

	pulsarConsumer pulsar.Consumer

	obj *sobek.Object // the object that is given to js to interact with the WebSocket

	// listeners
	// this return sobek.value *and* error in order to return error on exception instead of panic
	// https://pkg.go.dev/github.com/dop251/goja#hdr-Functions
	messageListener func(sobek.Value) (sobek.Value, error)
	errorListener   func(sobek.Value) (sobek.Value, error)

	subRefCount int64 // reference count for subscribe, used to exit the loop when this count is reached
	subDuration int64 // duration in milliseconds to keep listening for messages
}

type conf struct {
	// The Pulsar server URL to connect to
	serverURL string
	// Client id (publisher and/or subscriber name)
	name string
	// A username to authenticate to the Pulsar server
	user string
	// Password to match username
	password string
	// Authentication token
	authenticationToken string
	// connectTimeout in ms
	connectTimeout uint
	// path to caRoot path
	caRootPath string
	// path to client cert file
	clientCertPath string
	// path to client cert key file
	clientCertKeyPath string
	// wether to skip the cert validity check
	skipTLSValidation bool
	// configure tls min version
	tlsMinVersion uint16
	// publishTimeout in ms
	publishTimeout uint
}

const (
	sentBytesLabel             = "pulsar_sent_bytes"
	receivedBytesLabel         = "pulsar_received_bytes"
	sentMessagesCountLabel     = "pulsar_sent_messages_count"
	receivedMessagesCountLabel = "pulsar_received_messages_count"
	sentDatesLabel             = "pulsar_sent_messages_dates"
	receivedDatesLabel         = "pulsar_received_messages_dates"
)

func getLabels(labelsArg sobek.Value, rt *sobek.Runtime) pulsarMetricsLabels {
	labels := pulsarMetricsLabels{}
	metricsLabels := labelsArg
	if metricsLabels == nil || sobek.IsUndefined(metricsLabels) {
		// set default values
		labels.SentBytesLabel = sentBytesLabel
		labels.ReceivedBytesLabel = receivedBytesLabel
		labels.SentMessagesCountLabel = sentMessagesCountLabel
		labels.ReceivedMessagesCountLabel = receivedMessagesCountLabel
		labels.SentDatesLabel = sentDatesLabel
		labels.ReceivedDatesLabel = receivedDatesLabel
		return labels
	}

	labelsJS, ok := metricsLabels.Export().(map[string]any)
	if !ok {
		common.Throw(rt, fmt.Errorf("invalid metricsLabels %#v", metricsLabels.Export()))
	}
	labels.SentBytesLabel, ok = labelsJS["sentBytesLabel"].(string)
	if !ok {
		common.Throw(rt, fmt.Errorf("invalid metricsLabels sentBytesLabel %#v", metricsLabels.Export()))
	}
	labels.ReceivedBytesLabel, ok = labelsJS["receivedBytesLabel"].(string)
	if !ok {
		common.Throw(rt, fmt.Errorf("invalid metricsLabels receivedBytesLabel %#v", metricsLabels.Export()))
	}
	labels.SentMessagesCountLabel, ok = labelsJS["sentMessagesCountLabel"].(string)
	if !ok {
		common.Throw(rt, fmt.Errorf("invalid metricsLabels sentMessagesCountLabel %#v", metricsLabels.Export()))
	}
	labels.ReceivedMessagesCountLabel, ok = labelsJS["receivedMessagesCountLabel"].(string)
	if !ok {
		common.Throw(rt, fmt.Errorf("invalid metricsLabels receivedMessagesCountLabel %#v", metricsLabels.Export()))
	}
	labels.SentDatesLabel, ok = labelsJS["sentDatesLabel"].(string)
	if !ok {
		common.Throw(rt, fmt.Errorf("invalid metricsLabels sentDatesLabel %#v", metricsLabels.Export()))
	}
	labels.ReceivedDatesLabel, ok = labelsJS["receivedDatesLabel"].(string)
	if !ok {
		common.Throw(rt, fmt.Errorf("invalid metricsLabels receivedDatesLabel %#v", metricsLabels.Export()))
	}

	return labels
}

func tlsVersionStringToNumber(version string) (uint16, error) {
	versionMap := map[string]uint16{
		"TLS 1.0": tls.VersionTLS10,
		"TLS 1.1": tls.VersionTLS11,
		"TLS 1.2": tls.VersionTLS12,
		"TLS 1.3": tls.VersionTLS13,
	}

	if versionNumber, ok := versionMap[version]; ok {
		return versionNumber, nil
	}

	return 0, errors.New("unknown TLS version")
}

//nolint:nosnakecase // their choice not mine
func (m *PulsarAPI) client(c sobek.ConstructorCall) *sobek.Object {
	rt := m.vu.Runtime()

	var clientConf conf
	serverURL := c.Argument(0)
	if isNilOrUndefined(serverURL) {
		common.Throw(rt, errors.New("Client requires a server URL"))
	}
	clientConf.serverURL = serverURL.String()
	name := c.Argument(1)
	if isNilOrUndefined(name) {
		common.Throw(rt, errors.New("Client requires a name"))
	}
	clientConf.name = name.String()

	// optional args
	userValue := c.Argument(2)
	if !isNilOrUndefined(userValue) {
		clientConf.user = userValue.String()
	}
	passwordValue := c.Argument(3)
	if !isNilOrUndefined(passwordValue) {
		clientConf.password = passwordValue.String()
	}
	authenticationToken := c.Argument(4)
	if !isNilOrUndefined(authenticationToken) {
		clientConf.authenticationToken = authenticationToken.String()
	}
	connectTimeoutValue := c.Argument(5)
	if !isNilOrUndefined(connectTimeoutValue) {
		connectTimeoutIntValue := connectTimeoutValue.ToInteger()
		if connectTimeoutIntValue < 0 {
			common.Throw(rt, errors.New("negative timeout value is not allowed"))
		}
		clientConf.connectTimeout = uint(connectTimeoutIntValue)
	}
	if caRootPathValue := c.Argument(6); isNilOrUndefined(caRootPathValue) {
		clientConf.caRootPath = ""
	} else {
		clientConf.caRootPath = caRootPathValue.String()
	}
	if clientCertPathValue := c.Argument(7); isNilOrUndefined(clientCertPathValue) {
		clientConf.clientCertPath = ""
	} else {
		clientConf.clientCertPath = clientCertPathValue.String()
	}
	if clientCertKeyPathValue := c.Argument(8); isNilOrUndefined(clientCertKeyPathValue) {
		clientConf.clientCertKeyPath = ""
	} else {
		clientConf.clientCertKeyPath = clientCertKeyPathValue.String()
	}

	skipTLS := c.Argument(9)
	clientConf.skipTLSValidation = skipTLS.ToBoolean()

	if tlsMinVersionValue := c.Argument(10); isNilOrUndefined(tlsMinVersionValue) {
		clientConf.tlsMinVersion = tls.VersionTLS13
	} else {
		tlsMinVersion, err := tlsVersionStringToNumber(tlsMinVersionValue.String())
		if err != nil {
			common.Throw(m.vu.Runtime(), err)
		} else {
			clientConf.tlsMinVersion = tlsMinVersion
		}
	}

	publishTimeoutValue := c.Argument(11)
	if !isNilOrUndefined(publishTimeoutValue) {
		publishTimeoutIntValue := publishTimeoutValue.ToInteger()
		if publishTimeoutIntValue < 0 {
			common.Throw(rt, errors.New("negative timeout value is not allowed"))
		}
		clientConf.publishTimeout = uint(publishTimeoutIntValue)
	}

	labels := getLabels(c.Argument(13), rt)

	client := &client{
		vu:              m.vu,
		initEnvironment: m.initEnvironment,
		metricsLabels:   labels,
		conf:            clientConf,
		obj:             rt.NewObject(),
	}

	m.defineRuntimeMethods(client)

	return client.obj
}

func isNilOrUndefined(v sobek.Value) bool {
	if v == nil || v.Export() == nil || sobek.IsUndefined(v) {
		return true
	}

	return false
}

func (m *PulsarAPI) defineRuntimeMethods(client *client) {
	rt := m.vu.Runtime()
	must := func(err error) {
		if err != nil {
			common.Throw(rt, err)
		}
	}

	// TODO add onmessage,onclose and so on
	must(client.obj.DefineDataProperty(
		"addEventListener", rt.ToValue(client.AddEventListener), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"subContinue", rt.ToValue(client.SubContinue), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"connect", rt.ToValue(client.Connect), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"isConnected", rt.ToValue(client.IsConnected), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"publish", rt.ToValue(client.Publish), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"publishAsyncForDuration", rt.ToValue(client.PublishAsyncForDuration), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"publishSyncForDuration", rt.ToValue(client.PublishSyncForDuration), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"subscribe", rt.ToValue(client.Subscribe), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"subscribeForDuration", rt.ToValue(client.SubscribeForDuration), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(client.obj.DefineDataProperty(
		"close", rt.ToValue(client.Close), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))

}

// Connect create a connection to pulsar
func (c *client) Connect() error {
	if c.metrics == nil {
		metrics, err := registerMetrics(c.initEnvironment, c.metricsLabels)
		if err != nil {
			common.Throw(c.vu.Runtime(), err)
		}

		c.metrics = &metrics
	}

	opts := pulsar.ClientOptions{}

	// check timeout value
	timeoutValue, err := safeUintToInt64(c.conf.connectTimeout)
	if err != nil {
		panic("timeout value is too large")
	}

	var tlsConfig *tls.Config
	// Use root CA if specified
	if len(c.conf.caRootPath) > 0 {
		pulsarTLSCA, err := os.ReadFile(c.conf.caRootPath)
		if err != nil {
			panic(err)
		}
		rootCA := x509.NewCertPool()
		loadCA := rootCA.AppendCertsFromPEM(pulsarTLSCA)
		if !loadCA {
			panic("failed to parse root certificate")
		}
		tlsConfig = &tls.Config{
			RootCAs:    rootCA,
			MinVersion: c.conf.tlsMinVersion, // #nosec G402
		}
	}
	// Use local cert if specified
	if len(c.conf.clientCertPath) > 0 {
		cert, err := tls.LoadX509KeyPair(c.conf.clientCertPath, c.conf.clientCertKeyPath)
		if err != nil {
			panic("failed to parse client certificate")
		}
		if tlsConfig != nil {
			tlsConfig.Certificates = []tls.Certificate{cert}
		} else {
			tlsConfig = &tls.Config{
				Certificates: []tls.Certificate{cert},
				MinVersion:   c.conf.tlsMinVersion, // #nosec G402
			}
		}
	}

	// set tls if skip is forced
	if c.conf.skipTLSValidation {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: c.conf.skipTLSValidation, //nolint:gosec

		}
	}

	if tlsConfig != nil {
		opts.TLSConfig = tlsConfig
	}
	opts.URL = c.conf.serverURL
	if c.conf.user != "" && c.conf.password != "" {
		if auth, err := pulsar.NewAuthenticationBasic(c.conf.user, c.conf.password); err != nil {
			common.Throw(c.vu.Runtime(), fmt.Errorf("failed to create authentication: %w", err))
		} else {
			opts.Authentication = auth
		}
	}
	if c.conf.authenticationToken != "" {
		opts.Authentication = pulsar.NewAuthenticationToken(c.conf.authenticationToken)
	}
	opts.ConnectionTimeout = time.Duration(timeoutValue) * time.Millisecond // #nosec G115

	rt := c.vu.Runtime()
	client, err := pulsar.NewClient(opts)
	if err != nil {
		common.Throw(rt, fmt.Errorf("failed to create pulsar client: %w", err))
	}
	c.pulsarClient = client

	return nil
}

// Close the given client
// wait for pending connections for timeout (ms) before closing
func (c *client) Close() {
	// close producer
	if c.pulsarProducer != nil {
		c.pulsarProducer.FlushWithCtx(c.vu.Context())
		c.pulsarProducer.Close()
		c.pulsarProducer = nil
	}

	// close consumer
	if c.pulsarConsumer != nil {
		c.pulsarConsumer.UnsubscribeForce()
		c.pulsarConsumer.Close()
		c.pulsarConsumer = nil
	}

	// disconnect client
	if c.pulsarClient != nil {
		c.pulsarClient.Close()
		c.pulsarClient = nil
	}
}

// IsConnected the given client
func (c *client) IsConnected() bool {
	if c.pulsarClient == nil {
		return false
	}
	return true
}

// error event for async
//
//nolint:nosnakecase // their choice not mine
func (c *client) newErrorEvent(msg string) *sobek.Object {
	rt := c.vu.Runtime()
	o := rt.NewObject()
	must := func(err error) {
		if err != nil {
			common.Throw(rt, err)
		}
	}

	must(o.DefineDataProperty("type", rt.ToValue("error"), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	must(o.DefineDataProperty("message", rt.ToValue(msg), sobek.FLAG_FALSE, sobek.FLAG_FALSE, sobek.FLAG_TRUE))
	return o
}

func safeUintToInt64(u uint) (int64, error) {
	if u > math.MaxInt64 {
		return 0, errors.New("value too large to convert to int64")
	}
	return int64(u), nil
}

package sdk

import (
	"encoding/json"
	"fmt"
	"gitlab.com/treehousefi/go-sdk/sdk/thriftapi"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
)

// GWRoute ...
type GWRoute struct {
	Path     string `json:"path" bson:"path"`
	Address  string `json:"address" bson:"address"`
	Protocol string `json:"protocol" bson:"protocol"`
}

type GWWhiteList struct {
	IP     string `json:"ip" bson:"ip"`
	Status bool   `json:"status" bson:"status"`
	Name   string `json:"name" bson:"name"`
}

type GWBypassRouting struct {
	Routing string `json:"routing" bson:"routing"`
	Status  bool   `json:"status" bson:"status"`
	Name    string `json:"name" bson:"name"`
}

// APIGateway ...
type APIGateway struct {
	server          APIServer
	conf            *mgo.Collection
	slow            *DBModel2
	routes          []GWRoute
	clients         map[string]APIClient
	internalRouting map[string]APIServer
	allowHeaders    map[string]string
	optionResponse  *APIResponse
	preForward      Handler
	afterRespond    func(req APIRequest, response *APIResponse)
	onBadGateway    Handler
	debug           bool

	whiteListDB  *mgo.Collection
	whiteListMux sync.Mutex
	whiteList    map[string]bool
	onBlacklist  Handler

	bypassRoutingDB  *mgo.Collection
	bypassRoutingMux sync.Mutex
	bypassRouting    map[string]bool

	slowThreshold int
}

// NewAPIGateway Create new API Gateway
func NewAPIGateway(server APIServer) *APIGateway {
	var gw = &APIGateway{
		server:          server,
		clients:         make(map[string]APIClient),
		internalRouting: make(map[string]APIServer),
		debug:           false,
		slowThreshold:   2000,
	}
	server.PreRequest(gw.route)
	return gw
}

// SetDebug  ...
func (gw *APIGateway) SetDebug(val bool) {
	gw.debug = val
}

// SetSlowMsThreshold  ...
func (gw *APIGateway) SetSlowMsThreshold(val int) {
	gw.slowThreshold = val
}

// GetGWRoutes get current routes
func (gw *APIGateway) GetGWRoutes() []GWRoute {
	return gw.routes
}

// LoadConfigFromDB ..
func (gw *APIGateway) LoadConfigFromDB(col *mgo.Collection) {
	gw.conf = col
	go gw.scanConfig()
}

// InitDB ..
func (gw *APIGateway) InitDB(s *DBSession, dbName string) {
	db := s.GetMGOSession().DB(dbName)
	gw.conf = db.C("_gateway_config")
	gw.conf.EnsureIndex(mgo.Index{
		Key:        []string{"path"},
		Unique:     true,
		Background: true,
	})

	gw.whiteListDB = db.C("_white_list")
	gw.whiteListDB.EnsureIndex(mgo.Index{
		Key:        []string{"ip"},
		Unique:     true,
		Background: true,
	})

	gw.bypassRoutingDB = db.C("_by_pass_routing")
	gw.bypassRoutingDB.EnsureIndex(mgo.Index{
		Key:        []string{"routing"},
		Unique:     true,
		Background: true,
	})

	go gw.scanConfig()
	go gw.scanWhiteList()
	go gw.scanBypassRouting()
}

func (gw *APIGateway) InitDBLog(s *DBSession, dbName string) {
	gw.slow = &DBModel2{
		ColName:        "_slow_request",
		DBName:         dbName,
		TemplateObject: &requestLog{},
	}
	gw.slow.Init(s)
	gw.slow.CreateIndex(mgo.Index{
		Key:        []string{"elapsed_time", "created_time"},
		Background: true,
	})
	gw.slow.CreateIndex(mgo.Index{
		Key:        []string{"created_time"},
		Background: true,
	})
}

// LoadConfigFromObject ..
func (gw *APIGateway) LoadConfigFromObject(routes []GWRoute) {
	gw.routes = routes
}

// SetPreForward set pre-handler for filter / authen / author
func (gw *APIGateway) SetPreForward(hdl Handler) {
	gw.preForward = hdl
}

// SetAfterRespond set handler for process after respond
func (gw *APIGateway) SetAfterRespond(proc func(req APIRequest, response *APIResponse)) {
	gw.afterRespond = proc
}

func (gw *APIGateway) SetInternalRouting(server APIServer, path string) {
	gw.internalRouting[path] = server
}

// SetBadGateway set handler for bad gateway cases
func (gw *APIGateway) SetBadGateway(hdl Handler) {
	gw.onBadGateway = hdl
}

// SetOptionResponse set handler for bad gateway cases
func (gw *APIGateway) SetOptionResponse(response *APIResponse) {
	gw.optionResponse = response
}

func (gw *APIGateway) SetAllowHeaders(headers map[string]string) {
	gw.allowHeaders = headers
}

// SetBlackList set handler for bad gateway cases
func (gw *APIGateway) SetBlackList(hdl Handler) {
	gw.onBlacklist = hdl
}

func (gw *APIGateway) scanConfig() {
	for true {
		var result []GWRoute
		gw.conf.Find(bson.M{}).All(&result)
		if result != nil {
			gw.routes = result
		}

		time.Sleep(20 * time.Second)
	}
}

func (gw *APIGateway) scanWhiteList() {
	for true {
		var result []GWWhiteList
		gw.whiteListDB.Find(bson.M{}).All(&result)
		if result != nil {
			gw.whiteListMux.Lock()
			gw.whiteList = make(map[string]bool)
			for _, v := range result {
				gw.whiteList[v.IP] = v.Status
			}
			gw.whiteListMux.Unlock()
		}

		time.Sleep(20 * time.Second)
	}
}

func (gw *APIGateway) scanBypassRouting() {
	for true {
		var result []GWBypassRouting
		gw.bypassRoutingDB.Find(bson.M{}).All(&result)
		if result != nil {
			gw.bypassRoutingMux.Lock()
			gw.bypassRouting = make(map[string]bool)
			for _, v := range result {
				gw.bypassRouting[v.Routing] = v.Status
			}
			gw.bypassRoutingMux.Unlock()
		}

		time.Sleep(20 * time.Second)
	}
}

func (gw *APIGateway) getClient(routeInfo GWRoute) APIClient {
	if gw.clients[routeInfo.Path] != nil {
		return gw.clients[routeInfo.Path]
	}
	config := &APIClientConfiguration{
		Address:       routeInfo.Address,
		Protocol:      routeInfo.Protocol,
		Timeout:       20 * time.Second,
		MaxRetry:      0,
		WaitToRetry:   2 * time.Second,
		MaxConnection: 200,
	}

	client := NewAPIClient(config)
	client.SetDebug(gw.debug)

	gw.clients[routeInfo.Path] = client
	return client
}

func (gw *APIGateway) route(req APIRequest, res APIResponder) error {
	if !gw.isAllowIP(req.GetIP()) {
		if gw.onBlacklist != nil {
			return gw.onBlacklist(req, res)
		}
		return gw.onBadGateway(req, res)
	}

	start := time.Now()
	path := req.GetPath()
	method := req.GetMethod()

	if method.Value == APIMethod.OPTIONS.Value {
		if gw.optionResponse != nil {
			res.Respond(gw.optionResponse)
		} else {
			res.Respond(&APIResponse{
				Status:  APIStatus.Ok,
				Message: "API Gateway run normally",
				Headers: gw.allowHeaders,
			})
		}
		return RequestBreakPoint
	}

	if gw.debug {
		fmt.Println("Receive Method / Path = " + method.Value + " => " + path)
		bytes, _ := json.Marshal(gw.routes)
		if gw.internalRouting != nil {
			fmt.Printf("Current internal-routing: %v\n", reflect.ValueOf(gw.internalRouting).MapKeys())
		}
		fmt.Println("Current routes: " + string(bytes))
	}

	var response *APIResponse
	var endProcessSignal error
	for irPath, server := range gw.internalRouting {
		if strings.HasPrefix(path, irPath) {
			result, procErr := server.Process(&thriftapi.APIRequest{
				Path:    path,
				Method:  req.GetMethod().Value,
				Content: req.GetContentText(),
				Params:  req.GetParams(),
				Headers: req.GetHeaders(),
			})
			if procErr != nil && result == nil {
				endProcessSignal = procErr
				response = &APIResponse{
					Status:    APIStatus.Error,
					Message:   endProcessSignal.Error(),
					Headers:   gw.allowHeaders,
					ErrorCode: "INTERNAL_ERROR",
				}
			} else {
				endProcessSignal = RequestBreakPoint
				response = &APIResponse{
					Status:    result.GetStatus().String(),
					Message:   result.GetMessage(),
					Headers:   result.GetHeaders(),
					Total:     result.GetTotal(),
					ErrorCode: result.GetErrorCode(),
				}
				json.Unmarshal([]byte(result.GetContent()), &response.Data)
			}
		}
	}

	if endProcessSignal == nil {
		isBadGW := true
		for i := 0; i < len(gw.routes); i++ {
			if strings.HasPrefix(path, gw.routes[i].Path) {
				isBadGW = false
				if gw.debug {
					fmt.Println(" => Found route: " + gw.routes[i].Protocol + " / " + gw.routes[i].Address)
				}

				// filter, for authen / author ....
				if gw.preForward != nil {
					if gw.debug {
						fmt.Println(" + Gateway has pre-handler")
					}
					err := gw.preForward(req, res)
					if err != nil {
						if gw.debug {
							fmt.Println(" => Pre-handler return error: " + err.Error())
						}
						return err
					}
				}

				// forward the request
				client := gw.getClient(gw.routes[i])
				if gw.debug {
					fmt.Println(" + Get client successfully.")
				}

				// forward remote IP
				xForwarded := req.GetHeader("X-Forwarded-For")
				remoteIP := req.(*HTTPAPIRequest).GetIP()
				if xForwarded == "" {
					xForwarded = remoteIP
				} else {
					xForwarded += "," + remoteIP
				}

				// check added header
				headers := req.GetAttribute("AddedHeaders")
				curHeaders := req.GetHeaders()
				if headers != nil {
					headerMap := headers.(map[string]string)
					for key, value := range headerMap {
						curHeaders[key] = value
					}
				}
				curHeaders["X-Forwarded-For"] = xForwarded

				req = NewOutboundAPIRequest(
					req.GetMethod().Value,
					req.GetPath(),
					req.GetParams(),
					req.GetContentText(),
					curHeaders,
				)

				response = client.MakeRequest(req)
				endProcessSignal = RequestBreakPoint
				break
			}
		}

		if isBadGW {
			// if not found on gateway config
			if gw.onBadGateway != nil {
				return gw.onBadGateway(req, res)
			}
		}
	}

	if response == nil {
		response = &APIResponse{
			Status:    APIStatus.Error,
			Message:   "There is no response",
			Headers:   gw.allowHeaders,
			ErrorCode: "INTERNAL_ERROR",
		}
	}

	if gw.debug {
		fmt.Println(" => Call ended.")
		fmt.Println(" => Result: " + response.Status + " / " + response.Message)
	}
	if response.Headers == nil {
		response.Headers = make(map[string]string)
	}
	if response.Headers["X-Execution-Time"] != "" {
		response.Headers["X-Endpoint-Time"] = response.Headers["X-Execution-Time"]
	}
	if response.Headers["X-Hostname"] != "" {
		response.Headers["X-Endpoint-Hostname"] = response.Headers["X-Hostname"]
	}

	if gw.allowHeaders != nil {
		for k, v := range gw.allowHeaders {
			if response.Headers[k] == "" {
				response.Headers[k] = v
			}
		}
	}

	res.Respond(response)

	if gw.slow != nil {
		var dif = float64(time.Since(start).Nanoseconds()) / 1000000
		if dif > float64(gw.slowThreshold) {
			go Execute(func() {
				go gw.writeSlowLog(req, response, dif)
			})
		}
	}

	if gw.afterRespond != nil {
		go Execute(func() {
			go gw.afterRespond(req, response)
		})
	}

	if endProcessSignal != nil {
		return endProcessSignal
	}

	return nil
}

type requestLog struct {
	Req  APIRequest   `bson:"request"`
	Resp *APIResponse `bson:"response"`
	Time string       `bson:"elapsed_time"`
}

func (gw *APIGateway) writeSlowLog(req APIRequest, resp *APIResponse, timeDif float64) {
	gw.slow.Create(&requestLog{
		Req:  req,
		Resp: resp,
		Time: fmt.Sprintf("%.2f ms", timeDif),
	})
}

func (gw *APIGateway) IsBypassRouting(routing string) bool {
	if gw.bypassRouting == nil || len(gw.bypassRouting) == 0 {
		return false
	}

	gw.bypassRoutingMux.Lock()
	defer gw.bypassRoutingMux.Unlock()

	result := gw.bypassRouting[routing]
	return result
}

func (gw *APIGateway) isAllowIP(ip string) bool {

	// if no config -> always allow
	if gw.whiteList == nil || len(gw.whiteList) == 0 {
		return true
	}

	// ipv6, ipv4 localhost -> always allow
	if ip == "::1" || ip == "127.0.0.1" {
		return true
	}

	// if config = '*' -> allow
	gw.whiteListMux.Lock()
	defer gw.whiteListMux.Unlock()

	status, ok := gw.whiteList["*"]
	if ok && status {
		return true
	}

	// search for exact IP
	status, ok = gw.whiteList[ip]
	if ok && status {
		return true
	}

	// split IP address into 4 strings
	// 192.168.100.123 -> [192, 168, 100, 123]
	arr := strings.Split(ip, ".")

	// 1* -> search for 192.168.100.*
	wcIP := fmt.Sprintf("%s.%s.%s.*", arr[0], arr[1], arr[2])
	status, ok = gw.whiteList[wcIP]
	if ok && status {
		return true
	}

	// 2* -> search for 192.168.*.*
	wcIP = fmt.Sprintf("%s.%s.*.*", arr[0], arr[1])
	status, ok = gw.whiteList[wcIP]
	if ok && status {
		return true
	}

	// 3* -> search for 192.*.*.*
	wcIP = fmt.Sprintf("%s.*.*.*", arr[0])
	status, ok = gw.whiteList[wcIP]
	if ok && status {
		return true
	}

	return false
}

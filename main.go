package main

import (
	"Test1/api"
	"Test1/auth"
	"Test1/model"
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

var app *sdk.App

func test(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS,POST,PUT")
	w.Header().Set("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type,Accept, x-client-key, x-client-token, x-client-secret, Authorization")
	Body := make(map[string]interface{})
	bodyByte, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(bodyByte, &Body)
	if err != nil {
		panic(err)
	}
	a := Body["a"].(string)
	b := Body["b"].(string)
	resp := make(map[string]string)

	resp["AccessToken"] = a
	resp["RefreshToken"] = b
	jData, _ := json.Marshal(resp)
	w.Write(jData)
	log.Print("ok")

}

func main() {

	app = sdk.NewApp("Test1")

	setupDatabase()

	app.OnAllDBConnected(onAllConnected)

	protocol := os.Getenv("protocol")
	if protocol == "" {
		protocol = "HTTP"
	}
	server, _ := app.SetupAPIServer(protocol)
	//server.PreRequest(func(req sdk.APIRequest, res sdk.APIResponder) error {
	//
	//	req.SetHeader("Content-Type", "application/json")
	//	req.SetHeader("Access-Control-Allow-Origin", "*")
	//	req.SetHeader("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS,POST,PUT")
	//	req.SetHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type,Accept, x-client-key, x-client-token, x-client-secret, Authorization")
	//	return nil
	//})

	server.PreRequest(func(req sdk.APIRequest, res sdk.APIResponder) error {

		if strings.Contains(req.GetPath(), "/login") {
			return nil
		}
		if strings.Contains(req.GetPath(), "/register") {
			return nil
		}
		err := auth.Verify(req)
		if err != nil {
			return res.Respond(&sdk.APIResponse{
				Status: sdk.APIStatus.Unauthorized,
			})
		}
		return nil
	})

	server.Expose(8000)
	server.SetHandler(sdk.APIMethod.GET, "/blockList", api.BlockHighest)
	server.SetHandler(sdk.APIMethod.GET, "/transactions", api.Transactions)
	server.SetHandler(sdk.APIMethod.POST, "/login", api.Login)
	server.SetHandler(sdk.APIMethod.POST, "/register", api.Register)
	server.SetHandler(sdk.APIMethod.GET, "/refresh", api.RefreshToken)
	server.SetHandler(sdk.APIMethod.GET, "/block", api.BlockInformation)
	server.SetHandler(sdk.APIMethod.GET, "/transaction-info", api.TransactionInformation)
	server.SetHandler(sdk.APIMethod.GET, "/contract", api.Contract)
	server.SetHandler(sdk.APIMethod.GET, "/balanceof", api.BalanceOf)
	server.SetHandler(sdk.APIMethod.GET, "/last-minted-timestamp", api.LastMintedTimestamp)
	server.SetHandler(sdk.APIMethod.GET, "/allowance", api.Allowance)
	server.SetHandler(sdk.APIMethod.GET, "/logout", api.Logout)

	app.Launch()

}

func setupDatabase() {
	dbClient := app.SetupDBClient(sdk.DBConfiguration{
		Address:            []string{"localhost:27017"},
		Username:           "admin",
		Password:           "123456789",
		AuthDB:             "golang",
		Ssl:                false,
		SecondaryPreferred: false,
	})
	dbClient.OnConnected(onDBConnected)
}

func onDBConnected(session *sdk.DBSession) error {
	model.InitDB_User(session)
	model.InitDB_Jwt(session)
	return nil
}

func onAllConnected() {

	//model.Queue.StartConsumer(model.Consumer, 50)
}

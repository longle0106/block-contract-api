package api

import (
	"Test1/auth"
	"Test1/model"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"go.mongodb.org/mongo-driver/bson"
	"log"
)

func Login(req sdk.APIRequest, res sdk.APIResponder) error {
	var user model.User
	err := req.GetContent(&user)
	if err != nil {
		return err
	}
	username := model.Format(user.Username)
	password := model.Format(user.Password)
	result := model.DB_User.QueryOne(bson.M{"username": username})
	if result.Status != sdk.APIStatus.Ok {
		return res.Respond(result)
	}
	re := result.Data.([]*model.User)
	dataPassword := re[0].Password
	log.Print(password)
	err = model.CheckPasswordHash(dataPassword, password)

	if err != nil {
		return res.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Unauthorized,
			Message: err.Error(),
		})
	}

	accessToken, errCreate := auth.CreateAccessToken(username)
	if errCreate != nil {
		return res.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Error,
			Message: errCreate.Error(),
		})
	}

	refreshToken, errCreate := auth.CreateRefreshToken(username)
	if errCreate != nil {
		return res.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Error,
			Message: errCreate.Error(),
		})
	}

	token := bson.M{"username": username, "accessToken": accessToken, "refreshToken": refreshToken}
	result = model.DB_Jwt.QueryOne(bson.M{"username": username})
	if result.Status == sdk.APIStatus.Ok {
		result = model.DB_Jwt.Delete(bson.M{"username": username})
		if result.Status != sdk.APIStatus.Ok {
			return res.Respond(result)
		}
	}
	log.Print(username)

	result = model.DB_Jwt.Create(token)
	if result.Status != sdk.APIStatus.Ok {
		return res.Respond(result)
	}

	resp := make(map[string]string)
	resp["AccessToken"] = accessToken
	resp["RefreshToken"] = refreshToken

	//req.SetHeader("Content-Type", "application/json")
	//req.SetHeader("Access-Control-Allow-Origin", "*")
	//req.SetHeader("Access-Control-Allow-Methods", "GET,HEAD,OPTIONS,POST,PUT")
	//req.SetHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type,Accept, x-client-key, x-client-token, x-client-secret, Authorization")
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Login Successfully",
		Headers: map[string]string{
			"Content-Type":                 "application/json",
			"Access-Control-Allow-Origin":  "*",
			"Access-Control-Allow-Methods": "GET,HEAD,OPTIONS,POST,PUT",
			"Access-Control-Allow-Headers": "*",
			"Access-Control-Max-Age":       "99999999",
		},
	})
}

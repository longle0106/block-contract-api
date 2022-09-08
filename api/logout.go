package api

import (
	"Test1/auth"
	"Test1/model"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"go.mongodb.org/mongo-driver/bson"
)

func Logout(req sdk.APIRequest, res sdk.APIResponder) error {
	username, _ := auth.ExtractUsernameFromToken(req)

	result := model.DB_Jwt.Delete(bson.M{"username": username})
	if result.Status != sdk.APIStatus.Ok {
		return res.Respond(result)
	}

	return res.Respond(&sdk.APIResponse{
		Status: sdk.APIStatus.Ok,
	})
}

package api

import (
	"Test1/model"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"go.mongodb.org/mongo-driver/bson"
)

func Register(req sdk.APIRequest, res sdk.APIResponder) error {
	var user model.User
	err := req.GetContent(&user)
	if err != nil {
		return res.Respond(&sdk.APIResponse{
			Status: sdk.APIStatus.Invalid,
		})
	}
	username := model.Format(user.Username)
	email := model.Format(user.Email)
	password := model.Format(user.Password)

	result := model.DB_User.QueryOne(bson.M{"username": username})
	if result.Status == sdk.APIStatus.Ok {
		return res.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Existed,
			Message: "Username does exist",
		})
	}

	password, _ = model.Hash(password)

	result = model.DB_User.Create(bson.M{"username": username, "email": email, "password": password})

	if result.Status != sdk.APIStatus.Ok {
		return res.Respond(result)

	}
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Message: "Register Succesfully",
	})
}

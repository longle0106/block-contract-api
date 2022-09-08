package api

import (
	"Test1/auth"
	"Test1/model"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

func RefreshToken(req sdk.APIRequest, res sdk.APIResponder) error {

	bearerToken := req.GetHeader("Authorization")
	token := strings.Split(bearerToken, " ")[1] //access

	result := model.DB_Jwt.QueryOne(bson.M{"refreshToken": token})
	if result.Status != sdk.APIStatus.Ok {
		return res.Respond(&sdk.APIResponse{
			Status:  sdk.APIStatus.Error,
			Message: "Token does not exist",
		})
	}

	err := auth.VerifyRefesh(req)
	if err != nil {
		return err
	}

	username, err := auth.ExtractUsernameFromRefreshToken(req)

	auth.CreateAccessToken(username)
	result = model.DB_Jwt.Delete(bson.M{"username": username})
	if result.Status != sdk.APIStatus.Ok {
		return res.Respond(result)
	}
	// OK

	accessToken, errCreate := auth.CreateAccessToken(username)
	if errCreate != nil {
		return errCreate
	}
	refreshToken, errCreate := auth.CreateRefreshToken(username)
	if errCreate != nil {
		return errCreate
	}

	createToken := bson.M{"username": username, "accessToken": accessToken, "refreshToken": refreshToken}

	_ = model.DB_Jwt.Create(createToken)

	respo := make(map[string]string)

	respo["AccessToken"] = accessToken
	respo["RefreshToken"] = refreshToken
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{respo},
		Message: "Create JWT successfully",
	})
}

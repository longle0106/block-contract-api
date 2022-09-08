package model

import "gitlab.com/treehousefi/go-sdk/sdk"

type Jwt struct {
	Username     string `json:"username" bson:"username,omitempty"`
	AccessToken  string `json:"accessToken" bson:"accessToken,omitempty"`
	RefreshToken string `json:"refreshToken" bson:"refreshToken,omitempty"`
}

var DB_Jwt = &sdk.DBModel2{
	ColName:        "jwt",
	TemplateObject: Jwt{},
}

func InitDB_Jwt(session *sdk.DBSession) {
	DB_Jwt.DBName = "golang"
	DB_Jwt.Init(session)
}

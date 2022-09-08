package model

import (
	"gitlab.com/treehousefi/go-sdk/sdk"
	"golang.org/x/crypto/bcrypt"
	"html"
	"strings"
)

type User struct {
	Username string `json:"username" bson:"username,omitempty"`
	Email    string `json:"email" bson:"email,omitempty"`
	Password string `json:"password" bson:"password,omitempty"`
}

var DB_User = &sdk.DBModel2{
	ColName:        "users",
	TemplateObject: &User{},
}

func InitDB_User(session *sdk.DBSession) {
	DB_User.DBName = "golang"
	DB_User.Init(session)
}

func Format(data string) string {
	data = html.EscapeString(strings.TrimSpace(data))
	return data
}
func Hash(password string) (string, error) {
	bytes, err := bcrypt.GenerateFromPassword([]byte(password), 14)
	return string(bytes), err
}
func CheckPasswordHash(hashedPassword, password string) error {
	return bcrypt.CompareHashAndPassword([]byte(hashedPassword), []byte(password))
}

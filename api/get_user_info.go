package api

//import (
//	db "Test1/DB"
//	"Test1/auth"
//	"Test1/model"
//	"context"
//	"encoding/json"
//	"fmt"
//	"gitlab.com/treehousefi/go-sdk/sdk"
//	"log"
//	"net/http"
//)
//
//func GetUser(req sdk.APIRequest, res sdk.APIResponder) error {
//	username, err := auth.ExtractUsernameFromTokenDB(req.GetContent())
//
//	if err != nil {
//		errorResponse(w, "Internal Server Error", http.StatusInternalServerError)
//		return
//	}
//
//	collection := db.ConnectUsers()
//
//	fmt.Println(username)
//
//	var user model.User
//	errFindUsername := collection.FindOne(context.TODO(), bson.M{"username": username}).Decode(&user)
//	if errFindUsername != nil {
//		errorResponse(w, "User does not exist", http.StatusConflict)
//		return
//	}
//	log.Println(user)
//	errorResponse(w, "Succesfully", http.StatusOK)
//	resp := make(map[string]string)
//
//	resp["username: "] = user.Username
//	resp["email: "] = user.Email
//	jsonResp, _ := json.Marshal(resp)
//	w.Write(jsonResp)
//
//}

package main

import (
	db "Test1/DB"
	"context"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"go.mongodb.org/mongo-driver/bson"
	"io/ioutil"
	"log"
	"net/http"
)

func main() {
	router := httprouter.New()
	router.POST("/login", Login)
	router.POST("/register", Register)
	router.GET("/user/get", CheckJwt(GetMyPosts))
	//router.GET("/user/get", GetMyPosts(GetUser))
	fmt.Println("Listening to port 8000")
	log.Fatal(http.ListenAndServe(":8000", router))

}
func Login(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var user User
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(body, &user)
	if err != nil {
		panic(err)
	}
	username := Format(user.Username)
	password := Format(user.Password)
	collection := db.ConnectUsers()
	var result bson.M
	err = collection.FindOne(context.TODO(), bson.M{"username": username}).Decode(&result)

	if err != nil {
		errorResponse(w, "Username or Password incorrect", http.StatusBadRequest)
		return
	}
	hashedPassword := fmt.Sprintf("%v", result["password"])
	err = CheckPasswordHash(hashedPassword, password)

	if err != nil {
		errorResponse(w, "Password incorrect", http.StatusUnauthorized)
		return
	}
	token, errCreate := CreateToken(username)
	if errCreate != nil {
		errorResponse(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	log.Println(token)
	errorResponse(w, "Login success", http.StatusOK)
	return
}

func Register(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	var user User
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}
	err = json.Unmarshal(body, &user)
	if err != nil {
		panic(err)
	}
	username := Format(user.Username)
	email := Format(user.Email)
	password := Format(user.Password)

	collection := db.ConnectUsers()
	var result bson.M
	errFindUsername := collection.FindOne(context.TODO(), bson.M{"username": username}).Decode(&result)
	if errFindUsername == nil {
		errorResponse(w, "User does exists", http.StatusConflict)
		return
	}
	password, err = Hash(password)

	if err != nil {
		errorResponse(w, "Register has failed", http.StatusInternalServerError)
		return
	}

	newUser := bson.M{"username": username, "email": email, "password": password}
	_, err = collection.InsertOne(context.TODO(), newUser)

	if err != nil {
		errorResponse(w, "Register has failed", http.StatusInternalServerError)
		return
	}
	errorResponse(w, "Register Succesfully", http.StatusCreated)
}

func errorResponse(w http.ResponseWriter, message string, httpStatusCode int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatusCode)
	resp := make(map[string]string)
	resp["message"] = message
	jsonResp, _ := json.Marshal(resp)
	w.Write(jsonResp)
}

func GetMyPosts(w http.ResponseWriter, r *http.Request, _ httprouter.Params) {
	username, err := ExtractUsernameFromToken(r)

	if err != nil {
		errorResponse(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}

	collection := db.ConnectUsers()

	fmt.Println(username)

	//var result interface{}
	var user User
	errFindUsername := collection.FindOne(context.TODO(), bson.M{"username": username}).Decode(&user)
	if errFindUsername != nil {
		errorResponse(w, "User does not exist", http.StatusConflict)
		return
	}
	log.Println(user)
	errorResponse(w, "Succesfully", http.StatusOK)
	resp := make(map[string]string)

	resp["username: "] = user.Username
	resp["email: "] = user.Email
	jsonResp, _ := json.Marshal(resp)
	w.Write(jsonResp)

}

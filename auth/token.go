package auth

import (
	"fmt"
	"github.com/dgrijalva/jwt-go"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"os"
	"strings"
	"time"
)

func CreateAccessToken(username string) (string, error) {
	claims := jwt.MapClaims{}
	claims["authorized"] = true
	claims["username"] = username
	claims["exp"] = time.Now().Add(time.Minute * 30).Unix()
	at := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := at.SignedString([]byte(os.Getenv("SECRET_JWT")))
	if err != nil {
		return "", err
	}
	return token, err
}
func CreateRefreshToken(username string) (string, error) {
	claims := jwt.MapClaims{}
	claims["authorized"] = true
	claims["username"] = username
	claims["exp"] = time.Now().Add(time.Hour * 24).Unix()
	rt := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	token, err := rt.SignedString([]byte(os.Getenv("REFRESH_JWT")))
	if err != nil {
		return "", err
	}
	return token, err
}
func Verify(req sdk.APIRequest) error {
	tokenString := Extract(req)
	_, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		return []byte(os.Getenv("SECRET_JWT")), nil
	})
	if err != nil {
		return err
	}
	return err
}

func VerifyRefesh(req sdk.APIRequest) error {
	tokenString := Extract(req)
	_, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}

		return []byte(os.Getenv("REFRESH_JWT")), nil
	})
	if err != nil {
		return err
	}
	return err
}
func Extract(req sdk.APIRequest) string {
	keys := req.GetHeaders()
	token := keys["token"]

	if token != "" {
		return token
	}

	bearerToken := req.GetHeader("Authorization")
	return strings.Split(bearerToken, " ")[1]
}

//asda

func ExtractUsernameFromToken(req sdk.APIRequest) (string, error) {
	var username string
	tokenString := Extract(req)
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(os.Getenv("SECRET_JWT")), nil
	})

	if err != nil {
		return username, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		username = fmt.Sprintf("%v", claims["username"])
	}

	return username, nil
}
func ExtractUsernameFromRefreshToken(req sdk.APIRequest) (string, error) {
	var username string
	tokenString := Extract(req)
	token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(os.Getenv("REFRESH_JWT")), nil
	})

	if err != nil {
		return username, err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		username = fmt.Sprintf("%v", claims["username"])
	}

	return username, nil
}

func ExtractUsernameFromTokenDB(dbToken string) error {

	token, err := jwt.Parse(dbToken, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("Unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(os.Getenv("SECRET_JWT")), nil
	})

	if err != nil {
		return err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		fmt.Println("%v", claims["username"])
	}
	return err
}

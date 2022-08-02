package main

import (
	"github.com/julienschmidt/httprouter"
	"net/http"
)

func CheckJwt(next httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		err := Verify(r)

		if err != nil {
			errorResponse(w, "Unauthorized", http.StatusUnauthorized)
			return
		}

		next(w, r, ps)
	}
}

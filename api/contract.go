package api

import (
	"fmt"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"log"
	"math"
	"math/big"
	"strconv"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func Contract(req sdk.APIRequest, res sdk.APIResponder) error {
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}

	// Golem (GNT) Address
	tokenAddress := common.HexToAddress("0xD850942eF8811f2A866692A623011bDE52a462C1")
	instance, err := NewToken(tokenAddress, client)
	if err != nil {
		log.Fatal(err)
	}

	//address := common.HexToAddress("0xF1A7F8Dc3f7777Bae90b551be397416fe2954fc6")

	name, err := instance.Name(&bind.CallOpts{})
	if err != nil {
		log.Fatal(err)
	}

	symbol, err := instance.Symbol(&bind.CallOpts{})
	if err != nil {
		log.Fatal(err)
	}

	decimals, err := instance.Decimals(&bind.CallOpts{})
	if err != nil {
		log.Fatal(err)
	}
	owner, err := instance.Owner(&bind.CallOpts{})
	if err != nil {
		log.Fatal(err)
	}

	isSealed, err := instance.IsSealed(&bind.CallOpts{})
	if err != nil {
		log.Fatal(err)
	}

	totalSupply, err := instance.TotalSupply(&bind.CallOpts{})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("name: %s\n", name)
	fmt.Printf("totalSupply: %v\n", totalSupply)
	fmt.Printf("decimals: %v\n", decimals)
	fmt.Printf("isSealed: %v\n", isSealed)
	fmt.Printf("owner: %v\n", owner)
	fmt.Printf("symbol: %s\n", symbol)

	resp := make(map[string]string)
	resp["name"] = name
	resp["totalSupply"] = totalSupply.String()
	resp["decimals"] = strconv.FormatUint(uint64(decimals), 10)
	resp["isSealed"] = strconv.FormatBool(isSealed)
	resp["owner"] = owner.String()
	resp["symbol"] = symbol
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})
}
func BalanceOf(req sdk.APIRequest, res sdk.APIResponder) error {
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}
	tokenAddress := common.HexToAddress("0xD850942eF8811f2A866692A623011bDE52a462C1")
	instance, err := NewToken(tokenAddress, client)
	if err != nil {
		log.Fatal(err)
	}

	ownerAddress := req.GetParam("address")
	if err != nil {
		return err
	}
	log.Print(ownerAddress)
	address := common.HexToAddress(ownerAddress)
	bal, err := instance.BalanceOf(&bind.CallOpts{}, address)
	if err != nil {
		log.Fatal(err)
	}
	decimals, err := instance.Decimals(&bind.CallOpts{})
	if err != nil {
		log.Fatal(err)
	}
	fbal := new(big.Float)
	fbal.SetString(bal.String())
	value := new(big.Float).Quo(fbal, big.NewFloat(math.Pow10(int(decimals))))

	//fmt.Printf("balance: %f", value) // "balance: 74605500.647409"
	fmt.Printf("balanceOf: %f\n", value)
	resp := make(map[string]string)
	resp["balanceOf"] = value.String()
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})
}

func LastMintedTimestamp(req sdk.APIRequest, res sdk.APIResponder) error {
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}
	tokenAddress := common.HexToAddress("0xD850942eF8811f2A866692A623011bDE52a462C1")
	instance, err := NewToken(tokenAddress, client)
	if err != nil {
		log.Fatal(err)
	}
	ownerAddress := req.GetParam("address")
	if err != nil {
		return err
	}
	log.Print(ownerAddress)
	address := common.HexToAddress(ownerAddress)
	lastMintedTimestamp, err := instance.LastMintedTimestamp(&bind.CallOpts{}, address)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("lastMintedTimestamp: %v\n", lastMintedTimestamp)
	resp := make(map[string]string)
	resp["lastMintedTimestamp"] = strconv.FormatUint(uint64(lastMintedTimestamp), 10)
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})
}

func Allowance(req sdk.APIRequest, res sdk.APIResponder) error {
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}
	tokenAddress := common.HexToAddress("0xD850942eF8811f2A866692A623011bDE52a462C1")
	instance, err := NewToken(tokenAddress, client)
	if err != nil {
		log.Fatal(err)
	}

	ownerAddress := req.GetParam("address")
	if err != nil {
		return err
	}
	log.Print(ownerAddress)
	address := common.HexToAddress(ownerAddress)
	spAddress := req.GetParam("spadress")
	if err != nil {
		return err
	}
	log.Print(ownerAddress)
	spenderAddress := common.HexToAddress(spAddress)

	allowance, err := instance.Allowance(&bind.CallOpts{}, address, spenderAddress)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("allowance: %v\n", allowance)
	resp := make(map[string]string)
	resp["allowance"] = allowance.String()
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})
}

package api

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"log"
	"math/big"
	"strconv"
)

func Transactions(req sdk.APIRequest, res sdk.APIResponder) error {
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}
	Body := req.GetParam("block")
	if err != nil {
		return err
	}
	log.Print(Body)
	//blockHeight := Body["block"].(string)
	blockH, _ := strconv.ParseInt(Body, 10, 64)
	log.Print(blockH)
	blockNumber := big.NewInt(blockH)
	block, err := client.BlockByNumber(context.Background(), blockNumber)
	if err != nil {
		log.Fatal(err)
	}
	//var tx *types.Transaction
	blockHash := block.Hash()
	var arr []string
	for idx := uint(0); idx < 10; idx++ {
		tx, err := client.TransactionInBlock(context.Background(), blockHash, idx)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(tx.Hash().Hex()) // 0x5d49fcaa394c97ec8a9c3e7bd9e8388d420fb050a52083ca52ff24b3b65bc9c2
		arr = append(arr, tx.Hash().String())
	}

	resp := make(map[string][]string) // check thiss // OK
	resp["Transactions"] = arr
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})
}
func TransactionInformation(req sdk.APIRequest, res sdk.APIResponder) error {

	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}

	txHash := req.GetParam("transaction")
	if err != nil {
		return err
	}
	log.Print(txHash)
	//Body := make(map[string]interface{})
	//err = req.GetContent(&Body)
	//if err != nil {
	//	return err
	//}

	//txHash := Body["txHash"].(string)

	Hash := common.HexToHash(txHash)
	tx, _, err := client.TransactionByHash(context.Background(), Hash)
	if err != nil {
		return err
	}

	receipt, err := client.TransactionReceipt(context.Background(), tx.Hash())
	if err != nil {
		log.Fatal(err)
	}

	resp := make(map[string]string)
	resp["TxHash"] = txHash
	resp["Status"] = strconv.FormatUint(receipt.Status, 10)
	resp["Block"] = receipt.BlockNumber.String()
	resp["To"] = tx.To().String()
	resp["Value"] = tx.Value().String()
	resp["TransactionFee"] = tx.Cost().String()
	resp["GasPrice"] = tx.GasPrice().String()
	resp["GasLimit"] = strconv.FormatUint(tx.Gas(), 10)
	resp["Nonce"] = strconv.FormatUint(tx.Nonce(), 10)

	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})
}

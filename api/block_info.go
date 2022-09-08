package api

import (
	"context"
	"fmt"
	"github.com/ethereum/go-ethereum/ethclient"
	"gitlab.com/treehousefi/go-sdk/sdk"
	"log"
	"math/big"
	"strconv"
)

func BlockHighest(req sdk.APIRequest, res sdk.APIResponder) error {
	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}
	header, err := client.HeaderByNumber(context.Background(), nil)
	if err != nil {
		log.Fatal(err)
	}
	BlockNumber := header.Number.String()

	fmt.Println(BlockNumber)
	resp := make(map[string]string)
	resp["BlockHighest"] = BlockNumber
	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})
}

func BlockInformation(req sdk.APIRequest, res sdk.APIResponder) error {
	//err := auth.Verify(req)
	//if err != nil {
	//	return res.Respond(&sdk.APIResponse{
	//		Status: sdk.APIStatus.Unauthorized,
	//	})
	//}

	client, err := ethclient.Dial("https://mainnet.infura.io/v3/9dc2ec26dbf34e6cb0722a0dc0e16828")
	if err != nil {
		log.Fatal(err)
	}

	//Body := make(map[string]interface{})
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

	blockHash := block.Hash()
	//count := uint(block.Transactions().Len())
	for idx := uint(0); idx < 20; idx++ {
		tx, err := client.TransactionInBlock(context.Background(), blockHash, idx)
		if err != nil {
			log.Fatal(err)
		}

		fmt.Println(tx.Hash().Hex()) // 0x5d49fcaa394c97ec8a9c3e7bd9e8388d420fb050a52083ca52ff24b3b65bc9c2
	}
	log.Print(blockNumber)
	resp := make(map[string]string)
	resp["BlockHeight"] = blockNumber.String()
	resp["TimeStamp"] = strconv.FormatUint(block.Time(), 10)
	resp["Transactions"] = strconv.Itoa(block.Transactions().Len())
	resp["GasUsed"] = strconv.FormatUint(block.GasUsed(), 10)
	resp["GasLimit"] = strconv.FormatUint(block.GasLimit(), 10)
	resp["BaseFeePerGas"] = block.BaseFee().String()
	resp["Hash"] = block.Hash().String()
	resp["ParentHash"] = block.ParentHash().String()
	resp["Nonce"] = "0x" + strconv.FormatUint(block.Nonce(), 16)

	return res.Respond(&sdk.APIResponse{
		Status:  sdk.APIStatus.Ok,
		Data:    []interface{}{resp},
		Message: "Success",
	})

}

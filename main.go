package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func main() {
	// コマンドライン引数をパースする
	bucketName := flag.String("bucket", "", "Bucket name")
	objectKey := flag.String("key", "", "Object key")
	op := flag.String("op", "", "Operation (url, upload)")
	flag.Parse()

	switch *op {
	case "url":
		// Pre-signed URLを生成する
		signedURL, err := generatePresignedURL(*bucketName, *objectKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		fmt.Println(signedURL)

	case "upload":
		// ファイルをアップロードする
		err := uploadFileWithHTTP(*bucketName, *objectKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

	default:
		// Pre-signed URLを生成して、ファイルをアップロードする
		signedURL, err := generatePresignedURL(*bucketName, *objectKey)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}

		err = uploadFileWithHTTP(*objectKey, signedURL)
		if err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
	}
}

// Pre-signed URLを生成する関数
func generatePresignedURL(bucketName string, objectKey string) (string, error) {
	// AWS SDKを初期化する
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return "", err
	}
	s3Client := s3.NewFromConfig(cfg)

	// Pre-signed URLを生成するためのリクエストを作成する
	presignClient := s3.NewPresignClient(s3Client)
	req, err := presignClient.PresignPutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, func(opts *s3.PresignOptions) {
		opts.Expires = time.Duration(60 * int64(time.Second))
	})
	if err != nil {
		return "", err
	}

	// Pre-signed URLを取得する
	signedURL := req.URL

	return signedURL, nil
}

// HTTP PUTリクエストを送信してファイルをアップロードする関数
func uploadFileWithHTTP(objectKey string, signedURL string) error {
	// アップロードするファイルを読み込む
	file, err := os.Open(objectKey)
	if err != nil {
		return err
	}
	defer file.Close()

	// リクエストヘッダーに設定するContent-Lengthを取得する
	stat, err := file.Stat()
	if err != nil {
		return err
	}
	size := stat.Size()

	// HTTP PUTリクエストを作成する
	req, err := http.NewRequest(http.MethodPut, signedURL, file)
	if err != nil {
		return err
	}

	// Content-Lengthを設定する
	req.ContentLength = size

	// HTTP PUTリクエストを送信する
	client := http.DefaultClient
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	// レスポンスを読み込む
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	fmt.Println(string(body))
	return nil
}

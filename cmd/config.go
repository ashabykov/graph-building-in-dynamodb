package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
)

type Config struct {
	AwsConfig           aws.Config
	LocalDynamoEndpoint string // e.g. "http://localhost:8000"
}

func LoadConfig() (*Config, error) {
	localEndpoint := getEnv("local_dynamodb_endpoint", "http://localhost:8000")
	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %v", err)
	}
	cnf := &Config{
		LocalDynamoEndpoint: localEndpoint,
		AwsConfig:           cfg,
	}
	return cnf, nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return strings.ToLower(value)
	}
	return defaultValue
}

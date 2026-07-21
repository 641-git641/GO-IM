// Logic service — consumes messages from Kafka, persists to MySQL,
// and serves gRPC queries (history, user lookup) for the Gateway.
//
// Usage:
//
//	go run ./cmd/logic/
//
// Requires MySQL (docker-compose up -d) and optionally Kafka.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/im/api/proto"
	"github.com/im/configs"
	"github.com/im/internal/logic"
	"github.com/im/internal/mq"
	"github.com/im/internal/repo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	configPath := os.Getenv("CONFIG_PATH")
	if configPath == "" {
		configPath = "configs/config.json"
	}
	cfg, err := configs.Load(configPath)
	if err != nil {
		log.Fatalf("[logic] load config: %v", err)
	}

	// --- MySQL (required for Logic service) ---
	// Use logic.mysql config if set, otherwise fall back to gateway.mysql.
	// Only fall back when DSN is empty, not simply because Enabled is false —
	// a config with Enabled=true but empty DSN is a config error, not a fallback signal.
	mySQLCfg := cfg.Logic.MySQL
	if mySQLCfg.DSN == "" {
		mySQLCfg = cfg.Gateway.MySQL
	}
	if mySQLCfg.DSN == "" {
		log.Fatalf("[logic] MySQL must be enabled (set logic.mysql.dsn or gateway.mysql.dsn)")
	}

	mysqlStore, err := repo.NewMySQLStore(mySQLCfg.DSN)
	if err != nil {
		log.Fatalf("[logic] MySQL init: %v", err)
	}
	defer mysqlStore.Close()
	log.Printf("[logic] MySQL connected")

	// --- gRPC server ---
	grpcAddr := cfg.Logic.ListenAddr
	if grpcAddr == "" {
		grpcAddr = cfg.Gateway.LogicGateway.ListenAddr
	}
	if grpcAddr == "" {
		grpcAddr = ":50051" // default Logic gRPC port
	}
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("[logic] gRPC listen %s: %v", grpcAddr, err)
	}

	grpcServer := grpc.NewServer()
	logicSrv := logic.NewServer(mysqlStore, cfg.Logic.WorkerID)
	proto.RegisterLogicServer(grpcServer, logicSrv)
	reflection.Register(grpcServer) // enables grpcurl for debugging
	log.Printf("[logic] gRPC server listening on %s", grpcAddr)

	// Channel to surface fatal gRPC serve errors without bypassing deferred cleanup.
	gRPCDone := make(chan error, 1)
	go func() {
		if err := grpcServer.Serve(lis); err != nil {
			gRPCDone <- err
		}
	}()

	// --- Kafka consumer (optional) ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Use logic.kafka config if set, otherwise fall back to gateway.kafka.
	// Only fall back when Brokers is empty — Enabled=false with valid Brokers should still work.
	kafkaCfg := cfg.Logic.Kafka
	if len(kafkaCfg.Brokers) == 0 {
		kafkaCfg = cfg.Gateway.Kafka
	}

	var consumer *mq.Consumer
	if kafkaCfg.Enabled && len(kafkaCfg.Brokers) > 0 {
		topic := kafkaCfg.Topic
		if topic == "" {
			topic = "im.message.persist"
		}
		consumerCfg := mq.ConsumerConfig{
			Brokers: kafkaCfg.Brokers,
			Topic:   topic,
			GroupID: "im-logic",
		}
		consumer = mq.NewConsumer(consumerCfg, mysqlStore)

		go consumer.Run(ctx)
		log.Printf("[logic] Kafka consumer started (topic=%s group=%s)", topic, consumerCfg.GroupID)
	} else {
		log.Printf("[logic] Kafka disabled (kafka.enabled=false), consumer not started")
	}

	// --- Graceful shutdown ---
	// Block on signal or fatal gRPC error, then perform ordered shutdown:
	//   1. GracefulStop gRPC → stop accepting RPCs, drain in-flight requests
	//   2. Cancel context → consumer Run loop exits (flushes buffer)
	//   3. Wait for consumer → all buffered messages flushed to MySQL
	//   4. Close consumer → release Kafka reader
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-sigCh:
		log.Println("[logic] shutting down...")
	case err := <-gRPCDone:
		log.Printf("[logic] gRPC serve fatal error: %v — shutting down...", err)
	}
	// Note: we intentionally do NOT use log.Fatalf here so that deferred
	// cleanup (mysqlStore.Close()) runs via main's deferred calls.

	// 1. Stop the gRPC server first so no new RPCs arrive while consumer drains.
	grpcServer.GracefulStop()

	// 2. Stop the Kafka consumer loop (triggers final flush).
	cancel()

	// 3. Wait for consumer to finish flushing, then close.
	if consumer != nil {
		consumer.Wait()
		consumer.Close()
	}

	log.Println("[logic] stopped")
}

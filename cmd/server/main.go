package main

import (
	"log"
	"test_scylla/internal/api"
	"test_scylla/internal/config"
	"test_scylla/internal/database"

	"github.com/gin-gonic/gin"
)

func main() {
	// 加載配置檔案 (包含帳號密碼)
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 使用配置檔中的資訊初始化 ScyllaDB
	if err := database.InitScylla(
		cfg.Scylla.Hosts,
		cfg.Scylla.Keyspace,
		cfg.Scylla.Username,
		cfg.Scylla.Password,
	); err != nil {
		log.Fatalf("Could not connect to ScyllaDB: %v", err)
	}
	defer database.CloseScylla()

	log.Println("ScyllaDB connected successfully")

	r := gin.Default()
	
	userHandler := api.NewUserHandler()

	// 路由設定
	v1 := r.Group("/api/v1")
	{
		v1.POST("/users", userHandler.CreateUser)
		v1.POST("/users/trace", userHandler.CreateUserWithTracing) // 新增：帶 tracing 資訊的寫入
		v1.POST("/users/get", userHandler.GetUser)
		v1.POST("/users/list", userHandler.ListUsers)
	}

	log.Println("Server starting on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatalf("Failed to run server: %v", err)
	}
}

package api

import (
	"fmt"
	"test_scylla/internal/config"
	"test_scylla/internal/database"
	"test_scylla/internal/models"
	"test_scylla/internal/repository"
	"testing"
	"time"
)

// Helper: 從配置檔載入連線資訊
func initTestDB(t *testing.T) {
	cfg, err := config.LoadConfig("../../config.yaml") // 測試目錄較深，需調整路徑
	if err != nil {
		t.Fatalf("Failed to load test config: %v", err)
	}
	database.InitScylla(cfg.Scylla.Hosts, cfg.Scylla.Keyspace, cfg.Scylla.Username, cfg.Scylla.Password)
}

// 初始化 ScyllaDB 連線 (包含 3 個本地埠號)
var hosts = []string{"127.0.0.1:9042", "127.0.0.1:9043", "127.0.0.1:9044"}
var keyspace = "test_keyspace"

// TestCreateUser 測試單筆使用者寫入 (直接呼叫 Repository)
func TestCreateUserDirect(t *testing.T) {
	initTestDB(t)
	repo := repository.NewUserRepository()

	user := models.User{
		UserID:     "test_user_direct",
		TimeBucket: "2024031816",
		Username:   "tester_direct",
		Email:      "test_direct@example.com",
		CreatedAt:  time.Now(),
	}

	if err := repo.CreateUser(&user); err != nil {
		t.Errorf("Failed to create user directly: %v", err)
	}
}

// TestBatchInsert50Users 批次寫入 50 筆資料，直接測試 Repository
func TestBatchInsert50Users(t *testing.T) {
	initTestDB(t)
	repo := repository.NewUserRepository()

	// 模擬 5 個用戶，每個用戶在 10 個不同的時間桶中各寫入一筆 (共 50 筆)
	userIDs := []string{"user_001", "user_002", "user_003", "user_004", "user_005"}

	// 混合日級別與小時級別的 time_bucket
	timeBuckets := []string{
		"20260315",   // 日級別
		"20260316",   // 日級別
		"20260317",   // 日級別
		"20260318",   // 日級別
		"2026031908", // 小時級別 08:00
		"2026031010", // 小時級別 10:00
		"2026031112", // 小時級別 12:00
		"2026031214", // 小時級別 14:00
		"2026031316", // 小時級別 16:00
		"2026031418", // 小時級別 18:00
	}

	successCount := 0
	failCount := 0
	total := len(userIDs) * len(timeBuckets)

	for i, uid := range userIDs {
		for j, bucket := range timeBuckets {
			user := models.User{
				UserID:     uid,
				TimeBucket: bucket,
				Username:   fmt.Sprintf("user_%d_%d", i, j),
				Email:      fmt.Sprintf("user_%d_%d@example.com", i, j),
				CreatedAt:  time.Now(),
			}

			if err := repo.CreateUser(&user); err == nil {
				successCount++
			} else {
				failCount++
				t.Logf("Failed to insert user %s bucket %s: %v", uid, bucket, err)
			}
		}
	}

	t.Logf("Batch insert complete: %d success, %d failed (total %d)", successCount, failCount, total)

	if successCount != total {
		t.Errorf("Expected %d successful inserts, got %d", total, successCount)
	}
}

// TestCreateUserWithTracing 測試帶 tracing 的寫入，直接測試 Repository
func TestCreateUserWithTracing(t *testing.T) {
	initTestDB(t)
	repo := repository.NewUserRepository()

	// 準備多筆資料，觀察不同 partition key 的 node 分佈
	testCases := []models.User{
		{UserID: "trace_user_001", TimeBucket: "2026033820", Username: "tracer_1", Email: "tracer1@test.com"},
		{UserID: "trace_user_002", TimeBucket: "2026032820", Username: "tracer_2", Email: "tracer2@test.com"},
		{UserID: "trace_user_003", TimeBucket: "2026031820", Username: "tracer_3", Email: "tracer3@test.com"},
	}

	for _, user := range testCases {
		user.CreatedAt = time.Now()
		traceResult, err := repo.CreateUserWithTracing(&user)

		if err != nil {
			t.Errorf("Failed to create user with tracing: %v", err)
			continue
		}

		if traceResult != nil {
			t.Logf("\n📍 User: %s | Bucket: %s | Coordinator: %s | Replicas: %v", 
				user.UserID, user.TimeBucket, traceResult.Coordinator, traceResult.ReplicaNodes)
		} else {
			t.Errorf("Tracing result is nil for user %s", user.UserID)
		}
	}
}

// TestListUsersByBucket 測試按分區查詢，直接測試 Repository
func TestListUsersByBucket(t *testing.T) {
	initTestDB(t)
	repo := repository.NewUserRepository()

	userID := "list_test_user_direct"
	bucket := "20260318"

	// 1. 寫入 3 筆測試資料到同一個分區 (Partition)
	for i := 1; i <= 3; i++ {
		user := models.User{
			UserID:     userID,
			TimeBucket: bucket,
			Username:   fmt.Sprintf("list_user_%d", i),
			Email:      fmt.Sprintf("list%d@test.com", i),
			CreatedAt:  time.Now(),
		}
		if err := repo.CreateUser(&user); err != nil {
			t.Fatalf("Failed to seed test data: %v", err)
		}
		// 稍微延遲以確保 created_at 有順序差異
		time.Sleep(10 * time.Millisecond)
	}

	// 2. 測試查詢，並設定 Limit = 2
	users, err := repo.ListByBucket(userID, bucket, 2)
	if err != nil {
		t.Errorf("Expected success, but got error: %v", err)
	}

	// 3. 驗證
	t.Logf("List result: count=%d", len(users))
	if len(users) != 2 {
		t.Errorf("Expected 2 users (due to Limit), but got %d", len(users))
	}

	for _, u := range users {
		t.Logf(" - User: %s, CreatedAt: %s", u.Username, u.CreatedAt)
	}
}

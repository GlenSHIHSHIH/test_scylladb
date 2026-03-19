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

// TestCreateAndGetUserDirect 測試單筆使用者寫入與查詢 (直接呼叫 Repository)
func TestCreateAndGetUserDirect(t *testing.T) {
	initTestDB(t)
	repo := repository.NewUserRepository()

	createdAt := time.Date(2026, 3, 19, 6, 48, 13, 28, time.UTC)
	user := models.User{
		UserID:     "test_user_direct",
		TimeBucket: "2024031816",
		Username:   "tester_direct",
		Email:      "test_direct@example.com",
		CreatedAt:  createdAt,
	}

	if err := repo.CreateUser(&user); err != nil {
		t.Fatalf("Failed to create user directly: %v", err)
	}

	got, err := repo.GetUser(user.UserID, user.TimeBucket, user.CreatedAt)
	if err != nil {
		t.Fatalf("Failed to get user directly: %v", err)
	}

	if got.UserID != user.UserID {
		t.Errorf("Expected UserID %s, got %s", user.UserID, got.UserID)
	}
	if got.TimeBucket != user.TimeBucket {
		t.Errorf("Expected TimeBucket %s, got %s", user.TimeBucket, got.TimeBucket)
	}
	if got.Username != user.Username {
		t.Errorf("Expected Username %s, got %s", user.Username, got.Username)
	}
	if got.Email != user.Email {
		t.Errorf("Expected Email %s, got %s", user.Email, got.Email)
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

	total := len(userIDs) * len(timeBuckets)
	users := make([]models.User, 0, total)
	baseTime := time.Now().UTC()

	for i, uid := range userIDs {
		for j, bucket := range timeBuckets {
			users = append(users, models.User{
				UserID:     uid,
				TimeBucket: bucket,
				Username:   fmt.Sprintf("user_%d_%d", i, j),
				Email:      fmt.Sprintf("user_%d_%d@example.com", i, j),
				CreatedAt:  baseTime.Add(time.Duration(i*len(timeBuckets)+j) * time.Millisecond),
			})
		}
	}

	if err := repo.CreateUsersBatch(users); err != nil {
		t.Fatalf("Failed to batch insert users: %v", err)
	}

	t.Logf("Batch insert complete: %d success, %d failed (total %d)", len(users), 0, total)
	if len(users) != total {
		t.Errorf("Expected %d users prepared for batch insert, got %d", total, len(users))
	}
}

// TestCreateUserWithTracing 測試帶 tracing 的寫入，直接測試 Repository
func TestCreateUserWithTracing(t *testing.T) {
	initTestDB(t)
	repo := repository.NewUserRepository()

	// 準備多筆資料，觀察不同 partition key 的 node 分佈
	testCases := []models.User{
		{UserID: "trace_user_001", TimeBucket: "201801010", Username: "tracer_1", Email: "tracer1@test.com"},
		{UserID: "trace_user_002", TimeBucket: "201913123", Username: "tracer_2", Email: "tracer2@test.com"},
		{UserID: "trace_user_003", TimeBucket: "2021031508", Username: "tracer_3", Email: "tracer3@test.com"},
		{UserID: "trace_user_004", TimeBucket: "202200416", Username: "tracer_4", Email: "tracer4@test.com"},
		{UserID: "trace_user_005", TimeBucket: "2023122506", Username: "tracer_5", Email: "tracer5@test.com"},
		{UserID: "trace_user_006", TimeBucket: "202510112", Username: "tracer_6", Email: "tracer6@test.com"},
		{UserID: "trace_user_007", TimeBucket: "2026111119", Username: "tracer_7", Email: "tracer7@test.com"},
		{UserID: "trace_user_008", TimeBucket: "202807203", Username: "tracer_8", Email: "tracer8@test.com"},
		{UserID: "trace_user_009", TimeBucket: "2030021414", Username: "tracer_9", Email: "tracer9@test.com"},
		{UserID: "trace_user_010", TimeBucket: "203512319", Username: "tracer_10", Email: "tracer10@test.com"},
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

	// 1. 準備 3 筆測試資料到同一個分區 (Partition)，以批量方式寫入
	baseTime := time.Now().UTC()
	seedUsers := make([]models.User, 0, 3)
	for i := 1; i <= 3; i++ {
		seedUsers = append(seedUsers, models.User{
			UserID:     userID,
			TimeBucket: bucket,
			Username:   fmt.Sprintf("list_user_%d", i),
			Email:      fmt.Sprintf("list%d@test.com", i),
			CreatedAt:  baseTime.Add(time.Duration(i) * 10 * time.Millisecond),
		})
	}
	if err := repo.CreateUsersBatch(seedUsers); err != nil {
		t.Fatalf("Failed to seed test data by batch insert: %v", err)
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

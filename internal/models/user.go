package models

import (
	"time"

	"github.com/scylladb/gocqlx/v2/table"
)

// User 代表 ScyllaDB 中的 users 資料表結構
// 使用 db tag 配合 gocqlx
type User struct {
	UserID     string    `db:"user_id"`
	TimeBucket string    `db:"time_bucket"` // 格式範例：20240318 (日) 或 2024031816 (時)
	Username   string    `db:"username"`
	Email      string    `db:"email"`
	CreatedAt  time.Time `db:"created_at"`
}

// UserMetadata 定義 Table 的欄位
var UserMetadata = table.Metadata{
	Name:    "users",
	Columns: []string{"user_id", "time_bucket", "username", "email", "created_at"},
	PartKey: []string{"user_id", "time_bucket"}, // 複合分區鍵 (UserID + 時間桶)
	SortKey: []string{"created_at"},              // 集群鍵 (同分區內按時間排序)
}

// UserTable 是 gocqlx 的 table 物件，方便產生 Query
var UserTable = table.New(UserMetadata)

package repository

import (
	"log"
	"test_scylla/internal/database"
	"test_scylla/internal/models"
	"test_scylla/internal/tracing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

// UserRepository 負責處理 User 相關的 DB 操作
type UserRepository struct {
	session gocqlx.Session
}

func NewUserRepository() *UserRepository {
	return &UserRepository{
		session: database.Session,
	}
}

// CreateUser 建立新使用者記錄
func (r *UserRepository) CreateUser(user *models.User) error {
	q := r.session.Query(models.UserTable.Insert()).BindStruct(user)
	return q.ExecRelease()
}

// CreateUsersBatch 批量建立使用者記錄
func (r *UserRepository) CreateUsersBatch(users []models.User) error {
	if len(users) == 0 {
		return nil
	}

	insertStmt, _ := models.UserTable.Insert()
	batch := r.session.Session.NewBatch(gocql.LoggedBatch)

	for _, user := range users {
		batch.Query(insertStmt, user.UserID, user.TimeBucket, user.Username, user.Email, user.CreatedAt)
	}

	return r.session.Session.ExecuteBatch(batch)
}

// CreateUserWithTracing 建立新使用者記錄，同時收集 tracing 資訊
// 回傳 tracing 結果，包含 coordinator 和 replica nodes
func (r *UserRepository) CreateUserWithTracing(user *models.User) (*tracing.TraceResult, error) {
	// 建立自訂的 QueryTracer（實作 gocql.Tracer 介面）
	tracer := tracing.NewQueryTracer(r.session.Session)

	q := r.session.Query(models.UserTable.Insert()).BindStruct(user)

	// 在底層 gocql.Query 上啟用 tracing
	q.Query.Trace(tracer)

	err := q.ExecRelease()
	if err != nil {
		return nil, err
	}

	// 取得 tracing 結果
	result := tracer.GetResult()
	if result != nil {
		log.Print(tracing.FormatTraceLog(result))
	}

	return result, nil
}

// GetUser 透過 userID, timeBucket 與 createdAt 精確定位唯一的使用者記錄
func (r *UserRepository) GetUser(userID, timeBucket string, createdAt time.Time) (*models.User, error) {
	var user models.User
	// Get() 會根據完整的 Primary Key (Partition Key + Sort Key) 進行查詢，需傳入 3 個參數
	q := r.session.Query(models.UserTable.Get()).Bind(userID, timeBucket, createdAt)
	err := q.GetRelease(&user)
	if err != nil {
		return nil, err
	}
	return &user, nil
}

// ListByBucket 列出特定 User 在特定時間桶內的所有記錄，可指定回傳筆數 (limit)
func (r *UserRepository) ListByBucket(userID, timeBucket string, limit int) ([]models.User, error) {
	var users []models.User
	
	stmt, names := models.UserTable.Select()
	var q *gocqlx.Queryx

	if limit > 0 {
		// 手動加上 LIMIT 子句
		stmt += " LIMIT ?"
		q = r.session.Query(stmt, names).Bind(userID, timeBucket, limit)
	} else {
		q = r.session.Query(stmt, names).Bind(userID, timeBucket)
	}

	err := q.SelectRelease(&users)
	return users, err
}

// ListUsers 列出全表 (警告：ScyllaDB scan 效能較差，此處給予 100 筆的限制)
func (r *UserRepository) ListUsers() ([]models.User, error) {
	var users []models.User
	stmt, names := models.UserTable.Select()
	
	// 全表掃描強制加上 LIMIT 預防萬一
	stmt += " LIMIT 100"
	q := r.session.Query(stmt, names)
	
	err := q.SelectRelease(&users)
	return users, err
}

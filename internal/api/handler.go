package api

import (
	"net/http"
	"test_scylla/internal/models"
	"test_scylla/internal/repository"
	"time"

	"github.com/gin-gonic/gin"
)

type UserHandler struct {
	repo *repository.UserRepository
}

func NewUserHandler() *UserHandler {
	return &UserHandler{
		repo: repository.NewUserRepository(),
	}
}

// UserSearchRequest 定義查詢複合分區鍵 (精確定位) 所需的 JSON 參數
type UserSearchRequest struct {
	UserID     string    `json:"user_id" binding:"required"`
	TimeBucket string    `json:"time_bucket" binding:"required"`
	CreatedAt  time.Time `json:"created_at" binding:"required"`
}

// UserListRequest 定義按分區查詢 (清單) 所需的 JSON 參數
type UserListRequest struct {
	UserID     string `json:"user_id" binding:"required"`
	TimeBucket string `json:"time_bucket" binding:"required"`
	Limit      int    `json:"limit"`
}

func (h *UserHandler) CreateUser(c *gin.Context) {
	var user models.User
	if err := c.ShouldBindJSON(&user); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	
	user.CreatedAt = time.Now()
	if err := h.repo.CreateUser(&user); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	
	c.JSON(http.StatusOK, user)
}

// CreateUserWithTracing 建立使用者並回傳 tracing 資訊（哪些 node 參與寫入）
func (h *UserHandler) CreateUserWithTracing(c *gin.Context) {
	var user models.User
	if err := c.ShouldBindJSON(&user); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user.CreatedAt = time.Now()
	traceResult, err := h.repo.CreateUserWithTracing(&user)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"user":    user,
		"tracing": traceResult,
	})
}

func (h *UserHandler) GetUser(c *gin.Context) {
	var req UserSearchRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	user, err := h.repo.GetUser(req.UserID, req.TimeBucket, req.CreatedAt)
	if err != nil {
		c.JSON(http.StatusNotFound, gin.H{"error": "User record not found with given criteria"})
		return
	}
	c.JSON(http.StatusOK, user)
}

func (h *UserHandler) ListUsers(c *gin.Context) {
	var req UserListRequest
	// 若有帶 Body，則按 bucket 篩選且支援指定 limit
	if err := c.ShouldBindJSON(&req); err == nil {
		users, err := h.repo.ListByBucket(req.UserID, req.TimeBucket, req.Limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, users)
		return
	}

	// 預設全表掃描
	users, err := h.repo.ListUsers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, users)
}

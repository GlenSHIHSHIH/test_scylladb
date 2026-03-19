package database

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
)

// Session 封裝了 gocqlx 的 Session，方便全域使用
var Session gocqlx.Session

// InitScylla 初始化 ScyllaDB 連線 (支援選用帳號密碼)
func InitScylla(hosts []string, keyspace, username, password string) error {
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = keyspace
	cluster.Consistency = gocql.Quorum
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 10 * time.Second
	
	// 設定身分驗證 (若有機資則啟用)
	if username != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	// 優化：針對 ScyllaDB 效能，建議使用 TokenAwareHostPolicy
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	
	// 設定連線池大小 (根據 CPU 核心數適度調整)
	cluster.NumConns = 2

	s, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		return fmt.Errorf("failed to create ScyllaDB session: %w", err)
	}

	Session = s
	return nil
}

// CloseScylla 關閉連線
func CloseScylla() {
	Session.Close()
}

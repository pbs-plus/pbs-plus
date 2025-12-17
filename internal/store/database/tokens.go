//go:build linux

package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/types"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
	_ "modernc.org/sqlite"
)

func generateWinInstall(token string) string {
	hostname := os.Getenv("PBS_PLUS_HOSTNAME")
	if utils.IsProxyCertValid(hostname) {
		return fmt.Sprintf("irm https://%s:8018/plus/agent/install/win?t=%s | iex", hostname, token)
	}

	return fmt.Sprintf(`[System.Net.ServicePointManager]::ServerCertificateValidationCallback={$true}; `+
		`[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; `+
		`iex(New-Object Net.WebClient).DownloadString("https://%s:8018/plus/agent/install/win?t=%s")`, hostname, token)
}

// CreateToken generates a new token using the manager and stores it.
func (database *Database) CreateToken(comment string) error {
	tokenStr, err := database.TokenManager.GenerateToken()
	if err != nil {
		return fmt.Errorf("CreateToken: error generating token: %w", err)
	}
	now := time.Now().Unix()
	_, err = database.writeDb.Exec(`
        INSERT INTO tokens (token, comment, created_at, revoked)
        VALUES (?, ?, ?, ?)
    `, tokenStr, comment, now, false)
	if err != nil {
		return fmt.Errorf("CreateToken: error inserting token: %w", err)
	}
	return nil
}

// GetToken retrieves a token’s entry and double-checks its validity.
func (database *Database) GetToken(tokenStr string) (types.AgentToken, error) {
	row := database.readDb.QueryRow(`
        SELECT token, comment, created_at, revoked
        FROM tokens
        WHERE token = ?
    `, tokenStr)

	var tokenProp types.AgentToken
	err := row.Scan(&tokenProp.Token, &tokenProp.Comment, &tokenProp.CreatedAt, &tokenProp.Revoked)
	if err != nil {
		if err == sql.ErrNoRows {
			return types.AgentToken{}, fmt.Errorf("GetToken: not found")
		}
		return types.AgentToken{}, fmt.Errorf("GetToken: error fetching token: %w", err)
	}

	// Validate the token using the token manager.
	if err := database.TokenManager.ValidateToken(tokenStr); err != nil {
		// Do not mutate DB here; just reflect effective validity.
		tokenProp.Revoked = true
	}

	tokenProp.WinInstall = generateWinInstall(tokenProp.Token)
	return tokenProp, nil
}

func (database *Database) GetAllTokens(includeRevoked bool) ([]types.AgentToken, error) {
	var rows *sql.Rows
	var err error
	if includeRevoked {
		rows, err = database.readDb.Query(`
            SELECT token
            FROM tokens
            ORDER BY created_at DESC
        `)
	} else {
		rows, err = database.readDb.Query(`
            SELECT token
            FROM tokens
            WHERE revoked = 0
            ORDER BY created_at DESC
        `)
	}
	if err != nil {
		return nil, fmt.Errorf("GetAllTokens: error querying tokens: %w", err)
	}
	defer rows.Close()

	var tokens []types.AgentToken
	for rows.Next() {
		var tokenStr string
		if err := rows.Scan(&tokenStr); err != nil {
			continue
		}
		tokenProp, err := database.GetToken(tokenStr)
		if err != nil {
			syslog.L.Error(err).WithField("id", tokenStr).Write()
			continue
		}
		// If includeRevoked is false, filter out those that validate as revoked due to manager check.
		if !includeRevoked && tokenProp.Revoked {
			continue
		}
		tokenProp.WinInstall = generateWinInstall(tokenProp.Token)
		tokens = append(tokens, tokenProp)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("GetAllTokens: row iteration error: %w", err)
	}

	return tokens, nil
}

func (database *Database) RevokeToken(tokenData types.AgentToken) error {
	current, err := database.GetToken(tokenData.Token)
	if err != nil {
		return fmt.Errorf("RevokeToken: fetch current token: %w", err)
	}

	if current.Revoked {
		// Already effectively revoked; still proceed to pruning.
	} else {
		_, err = database.writeDb.Exec(`
            UPDATE tokens SET revoked = 1 WHERE token = ?
        `, current.Token)
		if err != nil {
			return fmt.Errorf("RevokeToken: error updating token: %w", err)
		}
	}

	// Prune old tokens
	if err := database.pruneOldTokensKeepLastValid(20); err != nil {
		return fmt.Errorf("RevokeToken: prune error: %w", err)
	}

	return nil
}

func (database *Database) pruneOldTokensKeepLastValid(keep int) error {
	rows, err := database.readDb.Query(`
        SELECT token, created_at, revoked
        FROM tokens
    `)
	if err != nil {
		return fmt.Errorf("pruneOldTokensKeepLastValid: query tokens: %w", err)
	}
	defer rows.Close()

	type rowInfo struct {
		Token     string
		CreatedAt int64
		Revoked   bool
		ValidNow  bool
	}

	var all []rowInfo
	for rows.Next() {
		var t string
		var c int64
		var r bool
		if err := rows.Scan(&t, &c, &r); err != nil {
			continue
		}
		// Validate via TokenManager. Treat error as invalid.
		valid := database.TokenManager.ValidateToken(t) == nil
		all = append(all, rowInfo{
			Token:     t,
			CreatedAt: c,
			Revoked:   r,
			ValidNow:  valid,
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("pruneOldTokensKeepLastValid: row iteration: %w", err)
	}

	var validNonRevoked []rowInfo
	for _, ri := range all {
		if ri.ValidNow && !ri.Revoked {
			validNonRevoked = append(validNonRevoked, ri)
		}
	}
	sort.Slice(validNonRevoked, func(i, j int) bool {
		return validNonRevoked[i].CreatedAt > validNonRevoked[j].CreatedAt
	})

	keepSet := make(map[string]struct{}, keep)
	for i := 0; i < len(validNonRevoked) && i < keep; i++ {
		keepSet[validNonRevoked[i].Token] = struct{}{}
	}

	var toDelete []string
	for _, ri := range all {
		if _, kept := keepSet[ri.Token]; kept {
			continue
		}
		if ri.Revoked || !ri.ValidNow {
			toDelete = append(toDelete, ri.Token)
		}
	}

	if len(toDelete) == 0 {
		return nil
	}

	tx, err := database.writeDb.Begin()
	if err != nil {
		return fmt.Errorf("pruneOldTokensKeepLastValid: begin tx: %w", err)
	}
	stmt, err := tx.Prepare(`DELETE FROM tokens WHERE token = ?`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("pruneOldTokensKeepLastValid: prepare delete: %w", err)
	}
	defer stmt.Close()

	for _, t := range toDelete {
		if _, err := stmt.Exec(t); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("pruneOldTokensKeepLastValid: delete %s: %w", t, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pruneOldTokensKeepLastValid: commit: %w", err)
	}

	return nil
}

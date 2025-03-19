//go:build linux

package sqlite

import (
	"fmt"
	"time"

	"github.com/sonroyaalmerol/pbs-plus/internal/store/types"
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	_ "modernc.org/sqlite"
)

// CreateToken generates a new token using the manager and stores it.
func (database *Database) CreateToken(comment string) error {
	tokenStr, err := database.TokenManager.GenerateToken()
	if err != nil {
		return fmt.Errorf("CreateToken: error generating token: %w", err)
	}
	now := time.Now().Unix()
	_, err = database.db.Exec(`
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
	row := database.db.QueryRow(`
        SELECT token, comment, created_at, revoked FROM tokens WHERE token = ?
    `, tokenStr)
	var tokenProp types.AgentToken
	err := row.Scan(&tokenProp.Token, &tokenProp.Comment, &tokenProp.CreatedAt,
		&tokenProp.Revoked)
	if err != nil {
		return types.AgentToken{}, fmt.Errorf("GetToken: error fetching token: %w", err)
	}
	// Validate the token using the token manager.
	if err := database.TokenManager.ValidateToken(tokenStr); err != nil {
		tokenProp.Revoked = true
	}
	return tokenProp, nil
}

// GetAllTokens returns all token entries.
func (database *Database) GetAllTokens() ([]types.AgentToken, error) {
	rows, err := database.db.Query("SELECT token FROM tokens")
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
		tokens = append(tokens, tokenProp)
	}
	return tokens, nil
}

// RevokeToken marks a token as revoked.
func (database *Database) RevokeToken(tokenData types.AgentToken) error {
	if tokenData.Revoked {
		return nil
	}

	tokenData.Revoked = true
	_, err := database.db.Exec(`
        UPDATE tokens SET revoked = ? WHERE token = ?
    `, true, tokenData.Token)
	if err != nil {
		return fmt.Errorf("RevokeToken: error updating token: %w", err)
	}
	return nil
}

// Only used for legacy database migration
func (database *Database) MigrateToken(tokenData types.AgentToken) error {
	_, err := database.db.Exec(`
        INSERT INTO tokens (token, comment, created_at, revoked)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(token) DO UPDATE SET
          comment = excluded.comment,
          created_at = excluded.created_at,
          revoked = excluded.revoked
    `, tokenData.Token, tokenData.Comment, tokenData.CreatedAt, tokenData.Revoked)
	return err
}

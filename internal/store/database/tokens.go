//go:build linux

package database

import (
	"database/sql"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/store/constants"
	"github.com/pbs-plus/pbs-plus/internal/store/database/sqlc"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"github.com/pbs-plus/pbs-plus/internal/utils"
)

func generateWinInstall(token string) string {
	hostname := os.Getenv("PBS_PLUS_HOSTNAME")
	if utils.IsProxyCertValid(hostname) {
		return fmt.Sprintf("irm https://%s%s/plus/agent/install/win?t=%s | iex", hostname, constants.ServerAPIExtPort, token)
	}

	return fmt.Sprintf(`[System.Net.ServicePointManager]::ServerCertificateValidationCallback={$true}; `+
		`[Net.ServicePointManager]::SecurityProtocol=[Net.SecurityProtocolType]::Tls12; `+
		`iex(New-Object Net.WebClient).DownloadString("https://%s:%s/plus/agent/install/win?t=%s")`, hostname, strings.TrimPrefix(constants.ServerAPIExtPort, ":"), token)
}

func (database *Database) CreateToken(expiration time.Duration, comment string) error {
	tokenStr, err := database.TokenManager.GenerateToken(expiration)
	if err != nil {
		return fmt.Errorf("CreateToken: error generating token: %w", err)
	}
	now := time.Now().Unix()

	err = database.queries.CreateToken(database.ctx, sqlc.CreateTokenParams{
		Token:     tokenStr,
		Comment:   toNullString(comment),
		CreatedAt: toNullInt64(int(now)),
		Revoked:   toNullBool(false),
	})
	if err != nil {
		return fmt.Errorf("CreateToken: error inserting token: %w", err)
	}
	return nil
}

func (database *Database) GetToken(tokenStr string) (AgentToken, error) {
	row, err := database.readQueries.GetToken(database.ctx, tokenStr)
	if err != nil {
		if err == sql.ErrNoRows {
			return AgentToken{}, fmt.Errorf("GetToken: not found")
		}
		return AgentToken{}, fmt.Errorf("GetToken: error fetching token: %w", err)
	}

	tokenProp := AgentToken{
		Token:     row.Token,
		Comment:   fromNullString(row.Comment),
		CreatedAt: fromNullInt64(row.CreatedAt),
		Revoked:   fromNullBool(row.Revoked),
	}

	if err := database.TokenManager.ValidateToken(tokenStr); err != nil {
		tokenProp.Revoked = true
	}

	tokenProp.Duration = database.TokenManager.GetTokenRemainingDuration(tokenStr).String()
	tokenProp.WinInstall = generateWinInstall(tokenProp.Token)

	return tokenProp, nil
}

func (database *Database) GetAllTokens(includeRevoked bool) ([]AgentToken, error) {
	var rows []sqlc.Token
	var err error

	if includeRevoked {
		rows, err = database.readQueries.ListAllTokens(database.ctx)
	} else {
		rows, err = database.readQueries.ListNonRevokedTokens(database.ctx)
	}
	if err != nil {
		return nil, fmt.Errorf("GetAllTokens: error querying tokens: %w", err)
	}

	var tokens []AgentToken
	for _, row := range rows {
		tokenProp, err := database.GetToken(row.Token)
		if err != nil {
			syslog.L.Error(err).WithField("id", row.Token).Write()
			continue
		}
		if !includeRevoked && tokenProp.Revoked {
			continue
		}
		tokenProp.Duration = database.TokenManager.GetTokenRemainingDuration(row.Token).String()
		tokenProp.WinInstall = generateWinInstall(tokenProp.Token)
		tokens = append(tokens, tokenProp)
	}

	return tokens, nil
}

func (database *Database) RevokeToken(tokenData AgentToken) error {
	current, err := database.GetToken(tokenData.Token)
	if err != nil {
		return fmt.Errorf("RevokeToken: fetch current token: %w", err)
	}

	if !current.Revoked {
		err = database.queries.RevokeToken(database.ctx, current.Token)
		if err != nil {
			return fmt.Errorf("RevokeToken: error updating token: %w", err)
		}
	}

	if err := database.pruneOldTokensKeepLastValid(20); err != nil {
		return fmt.Errorf("RevokeToken: prune error: %w", err)
	}

	return nil
}

func (database *Database) pruneOldTokensKeepLastValid(keep int) error {
	rows, err := database.readQueries.ListAllTokensWithDetails(database.ctx)
	if err != nil {
		return fmt.Errorf("pruneOldTokensKeepLastValid: query tokens: %w", err)
	}

	type rowInfo struct {
		Token     string
		CreatedAt int64
		Revoked   bool
		ValidNow  bool
	}

	var all []rowInfo
	for _, row := range rows {
		valid := database.TokenManager.ValidateToken(row.Token) == nil
		all = append(all, rowInfo{
			Token:     row.Token,
			CreatedAt: int64(fromNullInt64(row.CreatedAt)),
			Revoked:   fromNullBool(row.Revoked),
			ValidNow:  valid,
		})
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

	tx, err := database.NewTransaction()
	if err != nil {
		return fmt.Errorf("pruneOldTokensKeepLastValid: begin tx: %w", err)
	}

	qtx := database.queries.WithTx(tx.Tx)
	for _, t := range toDelete {
		if err := qtx.DeleteToken(database.ctx, t); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("pruneOldTokensKeepLastValid: delete %s: %w", t, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("pruneOldTokensKeepLastValid: commit: %w", err)
	}

	return nil
}

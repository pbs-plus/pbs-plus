package coredb

import (
	"github.com/pbs-plus/pbs-plus/internal/conf"
	"github.com/pbs-plus/pbs-plus/internal/crypto"
	"github.com/pbs-plus/pbs-plus/internal/log"
)

func init() {
	crypto.SetSealKeyPath(conf.SecretsKeyPath)
}

func Encrypt(plaintext string) (string, error) {
	return crypto.Seal(plaintext)
}

func Decrypt(ciphertext string) (string, error) {
	return crypto.Unseal(ciphertext)
}

func (d *Store) MigrateSecrets() error {
	if err := crypto.MigrateNaclKeyIfExists(); err != nil {
		log.Error(err, "database: failed to migrate nacl key")
		return err
	}

	if crypto.IsMigrated() {
		return nil
	}

	if !crypto.NaclKeyExists() {
		if err := crypto.MarkMigrated(); err != nil {
			log.Error(err, "database: failed to mark fresh install as migrated")
		}
		return nil
	}
	log.Info("database: migrating secrets from nacl-box to aes-256-gcm")

	rows, err := d.Reader().QueryContext(d.ctx, "SELECT target_name, secret FROM target_s3 WHERE secret != ''")
	if err != nil {
		return err
	}
	defer rows.Close()

	var migrated int
	for rows.Next() {
		var name, encrypted string
		if err := rows.Scan(&name, &encrypted); err != nil {
			continue
		}

		plaintext, err := crypto.TryDecryptNacl(encrypted)
		if err != nil {
			continue
		}

		reencrypted, err := crypto.Seal(plaintext)
		if err != nil {
			continue
		}

		_, err = d.Writer().ExecContext(d.ctx, "UPDATE target_s3 SET secret = ? WHERE target_name = ?", reencrypted, name)
		if err != nil {
			continue
		}
		migrated++
	}
	log.Info("database: migrated secrets", "count", migrated)

	if err := crypto.MarkMigrated(); err != nil {
		log.Error(err, "database: failed to mark migration complete")
		return err
	}

	return nil
}

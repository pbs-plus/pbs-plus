package database

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

func (d *Database) MigrateSecrets() error {
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

	rows, err := d.readDb.QueryContext(d.ctx, "SELECT name, secret_s3 FROM targets WHERE secret_s3 != '' AND secret_s3 IS NOT NULL")
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

		_, err = d.writeDb.ExecContext(d.ctx, "UPDATE targets SET secret_s3 = ? WHERE name = ?", reencrypted, name)
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

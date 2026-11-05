ALTER TABLE target_dovecot ADD COLUMN api_port INTEGER NOT NULL DEFAULT 0 CHECK (api_port BETWEEN 0 AND 65535);
ALTER TABLE backup_dovecot_options ADD COLUMN scope TEXT NOT NULL DEFAULT 'user';
ALTER TABLE restore_dovecot_options ADD COLUMN destination_mailbox TEXT NOT NULL DEFAULT '';
ALTER TABLE restore_dovecot_options ADD COLUMN messages TEXT NOT NULL DEFAULT '';

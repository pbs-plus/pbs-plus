ALTER TABLE restore_dovecot_options DROP COLUMN messages;
ALTER TABLE restore_dovecot_options DROP COLUMN destination_mailbox;
ALTER TABLE backup_dovecot_options DROP COLUMN scope;
ALTER TABLE target_dovecot DROP COLUMN api_port;

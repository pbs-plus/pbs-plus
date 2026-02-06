package config

type ScriptsConfig struct {
	AllowedCommands []string `toml:"allowed_commands"`
	AllowedPaths    []string `toml:"allowed_paths"`
}

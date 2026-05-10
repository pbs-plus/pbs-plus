package conf

import "os"

// EnvConfig holds values loaded from environment variables at startup.
// All lookups happen once during init; zero runtime os.Getenv calls in hot paths.
type EnvConfig struct {
	Debug              bool
	Hostname           string
	StdoutOnly         bool
	ClientNofile       string
	DisableAutoUpdate  bool
	InsideContainer    bool
	UpdateIntervalMin  string
	ForceDisableIRM    bool
	PprofEnabled       bool
	LogDedupWindow     string
	InitServerURL      string
	InitBootstrapToken string
}

// Env is the singleton environment config, loaded once at startup.
var Env = loadEnvConfig()

func loadEnvConfig() EnvConfig {
	return EnvConfig{
		Debug:              os.Getenv("DEBUG") == "true",
		Hostname:           os.Getenv("PBS_PLUS_HOSTNAME"),
		StdoutOnly:         os.Getenv("PBS_PLUS_STDOUT_ONLY") == "true",
		ClientNofile:       os.Getenv("PBS_PLUS_CLIENT_NOFILE"),
		DisableAutoUpdate:  os.Getenv("PBS_PLUS_DISABLE_AUTO_UPDATE") == "true",
		InsideContainer:    os.Getenv("PBS_PLUS__I_AM_INSIDE_CONTAINER") == "true",
		UpdateIntervalMin:  os.Getenv("PBS_PLUS_UPDATE_INTERVAL_MINUTES"),
		ForceDisableIRM:    os.Getenv("PBS_PLUS_FORCE_DISABLE_IRM_GENERATE") == "true",
		PprofEnabled:       os.Getenv("PBS_PLUS_PPROF") == "true",
		LogDedupWindow:     os.Getenv("LOG_DEDUP_WINDOW"),
		InitServerURL:      os.Getenv("PBS_PLUS_INIT_SERVER_URL"),
		InitBootstrapToken: os.Getenv("PBS_PLUS_INIT_BOOTSTRAP_TOKEN"),
	}
}

module github.com/pbs-plus/pbs-plus

go 1.25.5

require (
	github.com/KimMachineGun/automemlimit v0.7.4
	github.com/Masterminds/semver v1.5.0
	github.com/billgraziano/dpapi v0.5.0
	github.com/bradfitz/gomemcache v0.0.0-20250403215159-8d39553ac7cf
	github.com/containers/winquit v1.1.0
	github.com/fsnotify/fsnotify v1.9.0
	github.com/fxamacker/cbor/v2 v2.9.0
	github.com/gobwas/glob v0.2.3
	github.com/gofrs/flock v0.12.1
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/golang-migrate/migrate/v4 v4.19.0
	github.com/hanwen/go-fuse/v2 v2.8.0
	github.com/jpillora/overseer v1.1.6
	github.com/kardianos/service v1.2.4
	github.com/minio/minio-go/v7 v7.0.95
	github.com/mxk/go-vss v1.2.0
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58
	github.com/prometheus/client_golang v1.23.0
	github.com/puzpuzpuz/xsync/v3 v3.5.1
	github.com/quic-go/quic-go v0.57.1
	github.com/rs/zerolog v1.34.0
	github.com/stretchr/testify v1.11.1
	golang.org/x/crypto v0.44.0
	golang.org/x/exp v0.0.0-20250911091902-df9299821621
	golang.org/x/sys v0.38.0
	modernc.org/sqlite v1.39.0
)

require (
	cdr.dev/slog v1.6.1 // indirect
	github.com/StackExchange/wmi v0.0.0-20190523213315-cbe66965904d // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/coder/clistat v1.1.2 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/elastic/go-sysinfo v1.15.4 // indirect
	github.com/elastic/go-windows v1.0.2 // indirect
	github.com/go-ini/ini v1.67.0 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/jpillora/s3 v1.1.4 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/klauspost/cpuid/v2 v2.3.0 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-colorable v0.1.14 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/minio/crc64nvme v1.1.1 // indirect
	github.com/minio/md5-simd v1.1.2 // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/prometheus/client_model v0.6.2 // indirect
	github.com/prometheus/common v0.65.0 // indirect
	github.com/prometheus/procfs v0.17.0 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.14.1 // indirect
	github.com/rs/xid v1.6.0 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/afero v1.12.0 // indirect
	github.com/tinylib/msgp v1.4.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.opentelemetry.io/otel v1.37.0 // indirect
	go.opentelemetry.io/otel/trace v1.37.0 // indirect
	golang.org/x/mod v0.30.0 // indirect
	golang.org/x/net v0.47.0 // indirect
	golang.org/x/sync v0.18.0 // indirect
	golang.org/x/telemetry v0.0.0-20251111182119-bc8e575c7b54 // indirect
	golang.org/x/text v0.31.0 // indirect
	golang.org/x/tools v0.39.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
	google.golang.org/protobuf v1.36.7 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	howett.net/plist v1.0.0 // indirect
	modernc.org/libc v1.66.9 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
	tailscale.com v1.80.3 // indirect
)

replace github.com/hanwen/go-fuse/v2 v2.7.2 => github.com/pbs-plus/go-fuse/v2 v2.1.2

tool golang.org/x/tools/cmd/deadcode

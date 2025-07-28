module github.com/pbs-plus/pbs-plus

go 1.24.0

require (
	github.com/KimMachineGun/automemlimit v0.7.2
	github.com/Microsoft/go-winio v0.6.2
	github.com/billgraziano/dpapi v0.5.0
	github.com/containers/winquit v1.1.0
	github.com/fsnotify/fsnotify v1.9.0
	github.com/gobwas/glob v0.2.3
	github.com/gofrs/flock v0.12.1
	github.com/golang-jwt/jwt v3.2.2+incompatible
	github.com/golang-migrate/migrate/v4 v4.18.3
	github.com/hanwen/go-fuse/v2 v2.7.2
	github.com/kardianos/service v1.2.2
	github.com/mxk/go-vss v1.2.0
	github.com/pkg/errors v0.9.1
	github.com/puzpuzpuz/xsync/v3 v3.5.1
	github.com/rs/zerolog v1.34.0
	github.com/spf13/cobra v1.9.1
	github.com/stretchr/testify v1.10.0
	github.com/xtaci/smux v1.5.34
	github.com/zeebo/xxh3 v1.0.2
	golang.org/x/crypto v0.38.0
	golang.org/x/exp v0.0.0-20250620022241-b7579e27df2b
	golang.org/x/sys v0.34.0
	golang.org/x/time v0.11.0
	modernc.org/sqlite v1.38.1
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/dustin/go-humanize v1.0.1 // indirect
	github.com/go-ole/go-ole v1.3.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/klauspost/cpuid/v2 v2.0.9 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/ncruces/go-strftime v0.1.9 // indirect
	github.com/pbnjay/memory v0.0.0-20210728143218-7b4eea64cf58 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/remyoudompheng/bigfft v0.0.0-20230129092748-24d4a6f8daec // indirect
	github.com/rogpeppe/go-internal v1.13.1 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
	modernc.org/libc v1.66.3 // indirect
	modernc.org/mathutil v1.7.1 // indirect
	modernc.org/memory v1.11.0 // indirect
)

replace github.com/hanwen/go-fuse/v2 v2.7.2 => github.com/pbs-plus/go-fuse/v2 v2.1.2

replace github.com/xtaci/smux v1.5.34 => github.com/pbs-plus/smux v0.0.0-20250322005336-855507aa64bf

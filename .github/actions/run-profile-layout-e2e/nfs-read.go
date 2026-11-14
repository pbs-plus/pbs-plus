package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	nfs "github.com/willscott/go-nfs-client/nfs"
	"github.com/willscott/go-nfs-client/nfs/rpc"
)

func run() (err error) {
	if len(os.Args) != 5 {
		return errors.New("usage: nfs-read address share path expected-content")
	}
	client, err := rpc.DialTCP("tcp", os.Args[1], false)
	if err != nil {
		return err
	}
	defer client.Close()
	mount := &nfs.Mount{Client: client}
	target, err := mount.Mount(os.Args[2], rpc.NewAuthUnix("layout-e2e", 0, 0).Auth())
	if err != nil {
		return fmt.Errorf("mount share: %w", err)
	}
	if _, err := target.ReadDirPlus("."); err != nil {
		return fmt.Errorf("list share root: %w", err)
	}
	file, err := target.Open(os.Args[3])
	if err != nil {
		return fmt.Errorf("open %s: %w", os.Args[3], err)
	}
	defer func() { err = errors.Join(err, file.Close()) }()
	body, err := io.ReadAll(io.LimitReader(file, 4096))
	if err != nil {
		return err
	}
	if string(body) != os.Args[4] {
		return fmt.Errorf("NFS contents = %q, want %q", body, os.Args[4])
	}
	return nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

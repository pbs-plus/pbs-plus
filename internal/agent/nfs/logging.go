package nfs

import (
	"github.com/sonroyaalmerol/pbs-plus/internal/syslog"
	nfs "github.com/willscott/go-nfs"
)

type nfsLogger struct {
	nfs.Logger
}

func (l *nfsLogger) Info(v ...interface{}) {
	v = append([]interface{}{"[NFS.Info] "}, v...)
	syslog.L.Info(v...)
}

func (l *nfsLogger) Infof(format string, v ...interface{}) {
	syslog.L.Infof("[NFS.Info] "+format, v...)
}

func (l *nfsLogger) Print(v ...interface{}) {
	v = append([]interface{}{"[NFS.Print] "}, v...)
	syslog.L.Info(v...)
}

func (l *nfsLogger) Printf(format string, v ...interface{}) {
	syslog.L.Infof("[NFS.Print] "+format, v...)
}

func (l *nfsLogger) Debug(v ...interface{}) {
	v = append([]interface{}{"[NFS.Debug] "}, v...)
	syslog.L.Info(v...)
}

func (l *nfsLogger) Debugf(format string, v ...interface{}) {
	syslog.L.Infof("[NFS.Debug] "+format, v...)
}

func (l *nfsLogger) Error(v ...interface{}) {
	v = append([]interface{}{"[NFS.Error] "}, v...)
	syslog.L.Error(v...)
}

func (l *nfsLogger) Errorf(format string, v ...interface{}) {
	syslog.L.Errorf("[NFS.Error] "+format, v...)
}

func (l *nfsLogger) Panic(v ...interface{}) {
	v = append([]interface{}{"[NFS.Panic] "}, v...)
	syslog.L.Error(v...)
}

func (l *nfsLogger) Panicf(format string, v ...interface{}) {
	syslog.L.Errorf("[NFS.Panic] "+format, v...)
}

func (l *nfsLogger) Trace(v ...interface{}) {
	v = append([]interface{}{"[NFS.Trace] "}, v...)
	syslog.L.Info(v...)
}

func (l *nfsLogger) Tracef(format string, v ...interface{}) {
	syslog.L.Infof("[NFS.Trace] "+format, v...)
}

func (l *nfsLogger) Warn(v ...interface{}) {
	v = append([]interface{}{"[NFS.Warn] "}, v...)
	syslog.L.Warn(v...)
}

func (l *nfsLogger) Warnf(format string, v ...interface{}) {
	syslog.L.Warnf("[NFS.Warn] "+format, v...)
}

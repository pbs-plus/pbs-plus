package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/pbs-plus/pbs-plus/internal/operator"
	"github.com/pbs-plus/pbs-plus/internal/syslog"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

var Version = "v0.0.0"

func main() {
	metricsAddr := flag.String("metrics-addr", ":8080", "The address the metric endpoint binds to.")
	enableLeaderElection := flag.Bool("enable-leader-election", false, "Enable leader election for controller manager.")
	leaderElectionNamespace := flag.String("leader-election-namespace", "", "Namespace for leader election")
	serverURL := flag.String("server-url", "", "PBS Plus server URL (e.g., https://pbs.example.com:8008)")
	bootstrapTokenSecret := flag.String("bootstrap-token-secret", "pbs-plus-bootstrap", "Secret containing bootstrap token")
	namespace := flag.String("namespace", "", "Namespace to watch (empty for all namespaces)")
	agentImage := flag.String("agent-image", "ghcr.io/pbs-plus/pbs-plus-agent:latest", "Agent container image")
	snapshotClass := flag.String("snapshot-class", "", "Default VolumeSnapshotClass to use (auto-detected if empty)")
	flag.Parse()

	if *serverURL == "" {
		fmt.Fprintln(os.Stderr, "--server-url is required")
		os.Exit(1)
	}

	syslog.L.Info().WithMessage("Starting PBS Plus Kubernetes Operator").WithField("version", Version).Write()

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	config, err := rest.InClusterConfig()
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to get in-cluster config").Write()
		os.Exit(1)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		syslog.L.Error(err).WithMessage("Failed to create kubernetes clientset").Write()
		os.Exit(1)
	}

	op := operator.New(operator.Config{
		ServerURL:               *serverURL,
		BootstrapTokenSecret:    *bootstrapTokenSecret,
		Namespace:               *namespace,
		AgentImage:              *agentImage,
		SnapshotClass:           *snapshotClass,
		MetricsAddr:             *metricsAddr,
		EnableLeaderElection:    *enableLeaderElection,
		LeaderElectionNamespace: *leaderElectionNamespace,
		Clientset:               clientset,
	})

	if err := op.Run(ctx); err != nil {
		syslog.L.Error(err).WithMessage("Operator exited with error").Write()
		os.Exit(1)
	}
}

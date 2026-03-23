package operator

import (
	"context"
	"sync"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

const (
	BackupAnnotation    = "pbs-plus.io/backup"
	SnapshotAnnotation  = "pbs-plus.io/snapshot"
	ScopeAnnotation     = "pbs-plus.io/scope"
	LastBackupTimestamp = "pbs-plus.io/last-backup-timestamp"
	ManagedByLabel      = "app.kubernetes.io/managed-by"
	ManagedByValue      = "pbs-plus-operator"
)

type Config struct {
	ServerURL               string
	BootstrapTokenSecret    string
	Namespace               string
	AgentImage              string
	SnapshotClass           string
	MetricsAddr             string
	EnableLeaderElection    bool
	LeaderElectionNamespace string
	Clientset               kubernetes.Interface
}

type Operator struct {
	config          Config
	pvcTracker      *PVCBackupTracker
	snapshotManager *SnapshotManager
	podManager      *PodManager
	informerFactory informers.SharedInformerFactory
}

func New(cfg Config) *Operator {
	return &Operator{config: cfg}
}

func (o *Operator) Run(ctx context.Context) error {
	namespace := o.config.Namespace
	if namespace == "" {
		namespace = metav1.NamespaceAll
	}

	resyncPeriod := 5 * time.Minute
	o.informerFactory = informers.NewSharedInformerFactoryWithOptions(
		o.config.Clientset,
		resyncPeriod,
		informers.WithNamespace(namespace),
	)

	o.pvcTracker = NewPVCBackupTracker()
	o.snapshotManager = NewSnapshotManager(o.config.Clientset, o.config.SnapshotClass)
	o.podManager = NewPodManager(
		o.config.Clientset,
		o.config.AgentImage,
		o.config.ServerURL,
		o.config.BootstrapTokenSecret,
	)

	pvcInformer := o.informerFactory.Core().V1().PersistentVolumeClaims().Informer()
	pvcInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    o.handlePVCAdd,
		UpdateFunc: o.handlePVCUpdate,
		DeleteFunc: o.handlePVCDelete,
	})

	podInformer := o.informerFactory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		DeleteFunc: o.handlePodDelete,
	})

	o.informerFactory.Start(ctx.Done())

	syslog.L.Info().
		WithMessage("Waiting for cache sync").
		Write()

	if !cache.WaitForCacheSync(ctx.Done(), pvcInformer.HasSynced, podInformer.HasSynced) {
		return nil
	}
	syslog.L.Info().
		WithMessage("Operator started successfully").
		WithField("namespace", namespace).
		Write()

	<-ctx.Done()

	syslog.L.Info().WithMessage("Operator shutting down").Write()
	return nil
}

func (o *Operator) handlePVCAdd(obj interface{}) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return
	}
	o.processPVC(pvc, nil)
}

func (o *Operator) handlePVCUpdate(oldObj, newObj interface{}) {
	oldPVC, ok := oldObj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return
	}
	newPVC, ok := newObj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return
	}
	o.processPVC(newPVC, oldPVC)
}

func (o *Operator) handlePVCDelete(obj interface{}) {
	pvc, ok := obj.(*corev1.PersistentVolumeClaim)
	if !ok {
		return
	}

	key := keyFunc(pvc)
	if o.pvcTracker.IsTracked(key) {
		syslog.L.Info().
			WithMessage("PVC deleted, cleaning up backup resources").
			WithField("pvc", key).
			Write()

		o.pvcTracker.Untrack(key)
		o.podManager.CleanupForPVC(context.Background(), pvc)
	}
}

func (o *Operator) handlePodDelete(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return
	}

	if !IsManagedByOperator(pod) {
		return
	}

	pvcKey := pod.Annotations["pbs-plus.io/pvc-ref"]
	if pvcKey == "" {
		return
	}

	syslog.L.Info().
		WithMessage("Backup pod deleted").
		WithField("pod", pod.Namespace+"/"+pod.Name).
		WithField("pvc", pvcKey).
		Write()
}

func (o *Operator) processPVC(pvc, oldPVC *corev1.PersistentVolumeClaim) {
	ctx := context.Background()
	key := keyFunc(pvc)

	backupEnabled := pvc.Annotations[BackupAnnotation] == "true"
	wasEnabled := false
	if oldPVC != nil {
		wasEnabled = oldPVC.Annotations[BackupAnnotation] == "true"
	}

	if !backupEnabled {
		if wasEnabled {
			syslog.L.Info().
				WithMessage("Backup annotation removed, cleaning up").
				WithField("pvc", key).
				Write()
			o.pvcTracker.Untrack(key)
			o.podManager.CleanupForPVC(ctx, pvc)
		}
		return
	}

	// Always use snapshot-mode for ReadWriteOnce volumes
	forceSnapshot := pvc.Annotations[SnapshotAnnotation] == "true"
	isRWO := isReadWriteOnce(pvc)

	useSnapshot := isRWO || forceSnapshot

	o.pvcTracker.Track(key, PVCBackupInfo{
		Name:          pvc.Name,
		Namespace:     pvc.Namespace,
		UseSnapshot:   useSnapshot,
		ForceSnapshot: forceSnapshot,
	})

	if oldPVC != nil {
		oldForceSnapshot := oldPVC.Annotations[SnapshotAnnotation] == "true"
		oldIsRWO := isReadWriteOnce(oldPVC)

		// If snapshot mode changed, cleanup and recreate
		if oldIsRWO != isRWO || oldForceSnapshot != forceSnapshot {
			syslog.L.Info().
				WithMessage("Snapshot mode changed, recreating backup pod").
				WithField("pvc", key).
				WithField("useSnapshot", useSnapshot).
				Write()
			o.podManager.CleanupForPVC(ctx, pvc)
		}
	}

	syslog.L.Info().
		WithMessage("Processing PVC for backup").
		WithField("pvc", key).
		WithField("useSnapshot", useSnapshot).
		Write()

	if err := o.ensureBackupPod(ctx, pvc, useSnapshot); err != nil {
		syslog.L.Error(err).
			WithMessage("Failed to ensure backup pod").
			WithField("pvc", key).
			Write()
	}
}

func (o *Operator) ensureBackupPod(ctx context.Context, pvc *corev1.PersistentVolumeClaim, useSnapshot bool) error {
	// Check if pod already exists
	existing, err := o.podManager.GetBackupPod(ctx, pvc)
	if err == nil && existing != nil {
		// Pod exists, check if it needs update
		if existing.Status.Phase == corev1.PodRunning {
			return nil
		}
	}

	var pvcToMount *corev1.PersistentVolumeClaim

	if useSnapshot {
		snapshotPVC, _, err := o.snapshotManager.CreateSnapshotAndRestore(ctx, pvc)
		if err != nil {
			return err
		}
		pvcToMount = snapshotPVC
	} else {
		pvcToMount = pvc
	}

	return o.podManager.CreateBackupPod(ctx, pvcToMount, pvc, useSnapshot)
}

func keyFunc(obj metav1.Object) string {
	return obj.GetNamespace() + "/" + obj.GetName()
}

func isReadWriteOnce(pvc *corev1.PersistentVolumeClaim) bool {
	accessModes := pvc.Spec.AccessModes
	for _, mode := range accessModes {
		if mode == corev1.ReadWriteOnce {
			return true
		}
	}
	return false
}

func IsManagedByOperator(pod *corev1.Pod) bool {
	return pod.Labels[ManagedByLabel] == ManagedByValue
}

type PVCBackupInfo struct {
	Name          string
	Namespace     string
	UseSnapshot   bool
	ForceSnapshot bool
}

type PVCBackupTracker struct {
	mu   sync.RWMutex
	pvcs map[string]PVCBackupInfo
}

func NewPVCBackupTracker() *PVCBackupTracker {
	return &PVCBackupTracker{
		pvcs: make(map[string]PVCBackupInfo),
	}
}

func (t *PVCBackupTracker) Track(key string, info PVCBackupInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.pvcs[key] = info
}

func (t *PVCBackupTracker) Untrack(key string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.pvcs, key)
}

func (t *PVCBackupTracker) IsTracked(key string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	_, exists := t.pvcs[key]
	return exists
}

func (t *PVCBackupTracker) Get(key string) (PVCBackupInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	info, exists := t.pvcs[key]
	return info, exists
}

func (t *PVCBackupTracker) List() []PVCBackupInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	result := make([]PVCBackupInfo, 0, len(t.pvcs))
	for _, info := range t.pvcs {
		result = append(result, info)
	}
	return result
}

func ListPVCsWithBackupAnnotation(factory informers.SharedInformerFactory, namespace string) ([]*corev1.PersistentVolumeClaim, error) {
	pvcLister := factory.Core().V1().PersistentVolumeClaims().Lister()

	var pvcs []*corev1.PersistentVolumeClaim
	var err error

	if namespace != "" {
		pvcs, err = pvcLister.PersistentVolumeClaims(namespace).List(labels.Everything())
	} else {
		pvcs, err = pvcLister.List(labels.Everything())
	}

	if err != nil {
		return nil, err
	}

	var result []*corev1.PersistentVolumeClaim
	for _, pvc := range pvcs {
		if pvc.Annotations[BackupAnnotation] == "true" {
			result = append(result, pvc)
		}
	}
	return result, nil
}

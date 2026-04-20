package operator

import (
	"context"
	"fmt"
	"time"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

const (
	SnapshotFinalizer          = "pbs-plus.io/snapshot-protection"
	SnapshotPVCLabel           = "pbs-plus.io/snapshot-pvc"
	SnapshotTimestampLabel     = "pbs-plus.io/snapshot-timestamp"
	SnapshotOriginalPVCLabel   = "pbs-plus.io/original-pvc"
	VolumeSnapshotGroupVersion = "snapshot.storage.k8s.io/v1"
	VolumeSnapshotClassAnn     = "pbs-plus.io/volumesnapshotclass"
)

type SnapshotManager struct {
	clientset     kubernetes.Interface
	restClient    rest.Interface
	snapshotClass string
	scheme        *runtime.Scheme
}

func NewSnapshotManager(clientset kubernetes.Interface, snapshotClass string) *SnapshotManager {
	return &SnapshotManager{
		clientset:     clientset,
		restClient:    clientset.Discovery().RESTClient(),
		snapshotClass: snapshotClass,
		scheme:        runtime.NewScheme(),
	}
}

func (sm *SnapshotManager) CreateSnapshotAndRestore(ctx context.Context, originalPVC *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, func(), error) {
	namespace := originalPVC.Namespace
	timestamp := time.Now().Unix()
	snapshotName := fmt.Sprintf("pbs-snapshot-%s-%d", originalPVC.Name, timestamp)
	restoredPVCName := fmt.Sprintf("pbs-restored-%s-%d", originalPVC.Name, timestamp)

	syslog.L.Info().
		WithMessage("Creating snapshot for PVC").
		WithField("originalPVC", namespace+"/"+originalPVC.Name).
		WithField("snapshotName", snapshotName).
		Write()

	if err := sm.createVolumeSnapshot(ctx, namespace, snapshotName, originalPVC); err != nil {
		return nil, nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	if err := sm.waitForSnapshotReady(ctx, namespace, snapshotName); err != nil {
		sm.deleteVolumeSnapshot(ctx, namespace, snapshotName)
		return nil, nil, fmt.Errorf("failed waiting for snapshot: %w", err)
	}

	syslog.L.Info().
		WithMessage("Creating restored PVC from snapshot").
		WithField("restoredPVC", namespace+"/"+restoredPVCName).
		Write()

	restoredPVC, err := sm.createRestoredPVC(ctx, namespace, restoredPVCName, snapshotName, originalPVC)
	if err != nil {
		sm.deleteVolumeSnapshot(ctx, namespace, snapshotName)
		return nil, nil, fmt.Errorf("failed to create restored PVC: %w", err)
	}

	if err := sm.waitForPVCReady(ctx, namespace, restoredPVCName); err != nil {
		sm.deletePVC(ctx, namespace, restoredPVCName)
		sm.deleteVolumeSnapshot(ctx, namespace, snapshotName)
		return nil, nil, fmt.Errorf("failed waiting for Restored PVC: %w", err)
	}

	cleanup := func() {
		cleanupCtx := context.Background()
		syslog.L.Info().
			WithMessage("Cleaning up snapshot resources").
			WithField("restoredPVC", restoredPVCName).
			WithField("snapshot", snapshotName).
			Write()

		sm.deletePVC(cleanupCtx, namespace, restoredPVCName)
		sm.deleteVolumeSnapshot(cleanupCtx, namespace, snapshotName)
	}

	return restoredPVC, cleanup, nil
}

func (sm *SnapshotManager) createVolumeSnapshot(ctx context.Context, namespace, name string, sourcePVC *corev1.PersistentVolumeClaim) error {
	snapshotClass := sm.getSnapshotClass(sourcePVC)

	snapshot := &VolumeSnapshot{
		TypeMeta: metav1.TypeMeta{
			APIVersion: VolumeSnapshotGroupVersion,
			Kind:       "VolumeSnapshot",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedByLabel:           ManagedByValue,
				SnapshotOriginalPVCLabel: sourcePVC.Name,
			},
		},
		Spec: VolumeSnapshotSpec{
			Source: VolumeSnapshotSource{
				PersistentVolumeClaimName: &sourcePVC.Name,
			},
		},
	}

	if snapshotClass != "" {
		snapshot.Spec.VolumeSnapshotClassName = &snapshotClass
	}

	return sm.restClient.Post().
		AbsPath("/apis/snapshot.storage.k8s.io/v1").
		Namespace(namespace).
		Resource("volumesnapshots").
		Body(snapshot).
		Do(ctx).
		Error()
}

func (sm *SnapshotManager) createRestoredPVC(ctx context.Context, namespace, name, snapshotName string, originalPVC *corev1.PersistentVolumeClaim) (*corev1.PersistentVolumeClaim, error) {
	storageClass := originalPVC.Spec.StorageClassName
	storageRequest := originalPVC.Spec.Resources.Requests[corev1.ResourceStorage]

	pvc := &corev1.PersistentVolumeClaim{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "PersistentVolumeClaim",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				ManagedByLabel:         ManagedByValue,
				SnapshotPVCLabel:       "true",
				SnapshotTimestampLabel: fmt.Sprintf("%d", time.Now().Unix()),
			},
			Annotations: map[string]string{
				"pbs-plus.io/original-snapshot": snapshotName,
				"pbs-plus.io/original-pvc":      originalPVC.Name,
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: storageRequest,
				},
			},
			DataSource: &corev1.TypedLocalObjectReference{
				APIGroup: new("snapshot.storage.k8s.io"),
				Kind:     "VolumeSnapshot",
				Name:     snapshotName,
			},
		},
	}

	if storageClass != nil {
		pvc.Spec.StorageClassName = storageClass
	}

	created, err := sm.clientset.CoreV1().PersistentVolumeClaims(namespace).Create(ctx, pvc, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to create restored PVC: %w", err)
	}

	return created, nil
}

func (sm *SnapshotManager) waitForSnapshotReady(ctx context.Context, namespace, name string) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		snapshot, err := sm.getVolumeSnapshot(ctx, namespace, name)
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		if snapshot.Status != nil && snapshot.Status.ReadyToUse != nil && *snapshot.Status.ReadyToUse {
			return true, nil
		}

		return false, nil
	})
}

func (sm *SnapshotManager) waitForPVCReady(ctx context.Context, namespace, name string) error {
	return wait.PollUntilContextTimeout(ctx, 2*time.Second, 5*time.Minute, true, func(ctx context.Context) (bool, error) {
		pvc, err := sm.clientset.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if errors.IsNotFound(err) {
				return false, nil
			}
			return false, err
		}

		if pvc.Status.Phase == corev1.ClaimBound {
			return true, nil
		}

		return false, nil
	})
}

func (sm *SnapshotManager) getSnapshotClass(pvc *corev1.PersistentVolumeClaim) string {
	if sm.snapshotClass != "" {
		return sm.snapshotClass
	}

	if ann, ok := pvc.Annotations[VolumeSnapshotClassAnn]; ok && ann != "" {
		return ann
	}

	return ""
}

func (sm *SnapshotManager) deleteVolumeSnapshot(ctx context.Context, namespace, name string) {
	err := sm.restClient.Delete().
		AbsPath("/apis/snapshot.storage.k8s.io/v1").
		Namespace(namespace).
		Resource("volumesnapshots").
		Name(name).
		Do(ctx).
		Error()
	if err != nil {
		syslog.L.Error(err).
			WithMessage("Failed to delete volume snapshot").
			WithField("snapshot", namespace+"/"+name).
			Write()
	}
}

func (sm *SnapshotManager) deletePVC(ctx context.Context, namespace, name string) {
	err := sm.clientset.CoreV1().PersistentVolumeClaims(namespace).Delete(ctx, name, metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		syslog.L.Error(err).
			WithMessage("Failed to delete PVC").
			WithField("pvc", namespace+"/"+name).
			Write()
	}
}

func (sm *SnapshotManager) getVolumeSnapshot(ctx context.Context, namespace, name string) (*VolumeSnapshot, error) {
	result := &VolumeSnapshot{}
	err := sm.restClient.Get().
		AbsPath("/apis/snapshot.storage.k8s.io/v1").
		Namespace(namespace).
		Resource("volumesnapshots").
		Name(name).
		Do(ctx).
		Into(result)
	return result, err
}

//go:fix inline
func strPtr(s string) *string {
	return new(s)
}

type VolumeSnapshot struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata"`
	Spec              VolumeSnapshotSpec    `json:"spec"`
	Status            *VolumeSnapshotStatus `json:"status,omitempty"`
}

func (in *VolumeSnapshot) DeepCopyObject() runtime.Object {
	if in == nil {
		return nil
	}
	out := new(VolumeSnapshot)
	in.DeepCopyInto(out)
	return out
}

func (in *VolumeSnapshot) DeepCopyInto(out *VolumeSnapshot) {
	*out = *in
	out.TypeMeta = in.TypeMeta
	in.ObjectMeta.DeepCopyInto(&out.ObjectMeta)
	out.Spec = in.Spec
	if in.Status != nil {
		out.Status = &VolumeSnapshotStatus{}
		in.Status.DeepCopyInto(out.Status)
	}
}

type VolumeSnapshotSpec struct {
	Source                  VolumeSnapshotSource `json:"source"`
	VolumeSnapshotClassName *string              `json:"volumeSnapshotClassName,omitempty"`
}

func (in *VolumeSnapshotSpec) DeepCopyInto(out *VolumeSnapshotSpec) {
	*out = *in
	if in.VolumeSnapshotClassName != nil {
		in, out := &in.VolumeSnapshotClassName, &out.VolumeSnapshotClassName
		*out = new(string)
		**out = **in
	}
}

type VolumeSnapshotSource struct {
	PersistentVolumeClaimName *string `json:"persistentVolumeClaimName,omitempty"`
}

type VolumeSnapshotStatus struct {
	ReadyToUse *bool `json:"readyToUse,omitempty"`
}

func (in *VolumeSnapshotStatus) DeepCopyInto(out *VolumeSnapshotStatus) {
	*out = *in
	if in.ReadyToUse != nil {
		in, out := &in.ReadyToUse, &out.ReadyToUse
		*out = new(bool)
		**out = **in
	}
}

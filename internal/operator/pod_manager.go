package operator

import (
	"context"
	"fmt"
	"strings"

	"github.com/pbs-plus/pbs-plus/internal/syslog"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	AgentPodPrefix    = "pbs-backup-"
	PVCMountPath      = "/backup"
	RegistryPath      = "/etc/pbs-plus-agent"
	RegistryStatePath = "/var/lib/pbs-plus-agent"
	RegistryLogPath   = "/var/log/pbs-plus-agent"
	ContainerName     = "pbs-plus-agent"
	ContainerPortName = "agent-port"
	ContainerPort     = 8018
)

type PodManager struct {
	clientset            kubernetes.Interface
	agentImage           string
	serverURL            string
	bootstrapTokenSecret string
}

func NewPodManager(clientset kubernetes.Interface, agentImage, serverURL, bootstrapTokenSecret string) *PodManager {
	return &PodManager{
		clientset:            clientset,
		agentImage:           agentImage,
		serverURL:            serverURL,
		bootstrapTokenSecret: bootstrapTokenSecret,
	}
}

func (pm *PodManager) CreateBackupPod(ctx context.Context, pvcToMount, originalPVC *corev1.PersistentVolumeClaim, useSnapshot bool) error {
	namespace := pvcToMount.Namespace
	podName := pm.getPodName(originalPVC)

	syslog.L.Info().
		WithMessage("Creating backup pod").
		WithField("pod", namespace+"/"+podName).
		WithField("pvc", pvcToMount.Name).
		WithField("useSnapshot", useSnapshot).
		Write()

	pod := pm.buildPodSpec(podName, namespace, pvcToMount, originalPVC, useSnapshot)

	_, err := pm.clientset.CoreV1().Pods(namespace).Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		if errors.IsAlreadyExists(err) {
			syslog.L.Info().
				WithMessage("Backup pod already exists").
				WithField("pod", namespace+"/"+podName).
				Write()
			return nil
		}
		return fmt.Errorf("failed to create backup pod: %w", err)
	}

	syslog.L.Info().
		WithMessage("Backup pod created successfully").
		WithField("pod", namespace+"/"+podName).
		Write()

	return nil
}

func (pm *PodManager) GetBackupPod(ctx context.Context, pvc *corev1.PersistentVolumeClaim) (*corev1.Pod, error) {
	namespace := pvc.Namespace
	podName := pm.getPodName(pvc)

	pod, err := pm.clientset.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	return pod, nil
}

func (pm *PodManager) CleanupForPVC(ctx context.Context, pvc *corev1.PersistentVolumeClaim) error {
	namespace := pvc.Namespace
	podName := pm.getPodName(pvc)

	syslog.L.Info().
		WithMessage("Cleaning up backup pod").
		WithField("pod", namespace+"/"+podName).
		Write()

	err := pm.clientset.CoreV1().Pods(namespace).Delete(ctx, podName, metav1.DeleteOptions{
		GracePeriodSeconds: new(int64(30)),
	})
	if err != nil && !errors.IsNotFound(err) {
		return fmt.Errorf("failed to delete backup pod: %w", err)
	}

	return nil
}

func (pm *PodManager) buildPodSpec(podName, namespace string, pvcToMount, originalPVC *corev1.PersistentVolumeClaim, useSnapshot bool) *corev1.Pod {
	readOnlyFalse := false
	pod := &corev1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":      "pbs-plus-backup-agent",
				"app.kubernetes.io/component": "backup-agent",
				ManagedByLabel:                ManagedByValue,
				"pbs-plus.io/original-pvc":    originalPVC.Name,
			},
			Annotations: map[string]string{
				"pbs-plus.io/original-pvc-name": originalPVC.Name,
				"pbs-plus.io/original-pvc-uid":  string(originalPVC.UID),
				"pbs-plus.io/use-snapshot":      fmt.Sprintf("%v", useSnapshot),
				"pbs-plus.io/pvc-ref":           namespace + "/" + originalPVC.Name,
			},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{
				{
					Name:            ContainerName,
					Image:           pm.agentImage,
					ImagePullPolicy: corev1.PullIfNotPresent,
					Env: []corev1.EnvVar{
						{
							Name:  "PBS_PLUS__I_AM_INSIDE_CONTAINER",
							Value: "true",
						},
						{
							Name:  "PBS_PLUS_DISABLE_AUTO_UPDATE",
							Value: "true",
						},
						{
							Name: "PBS_PLUS_INIT_SERVER_URL",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: pm.bootstrapTokenSecret,
									},
									Key: "server-url",
								},
							},
						},
						{
							Name: "PBS_PLUS_INIT_BOOTSTRAP_TOKEN",
							ValueFrom: &corev1.EnvVarSource{
								SecretKeyRef: &corev1.SecretKeySelector{
									LocalObjectReference: corev1.LocalObjectReference{
										Name: pm.bootstrapTokenSecret,
									},
									Key: "bootstrap-token",
								},
							},
						},
						{
							Name: "PBS_PLUS_HOSTNAME",
							ValueFrom: &corev1.EnvVarSource{
								FieldRef: &corev1.ObjectFieldSelector{
									FieldPath: "metadata.name",
								},
							},
						},
					},
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "pvc-backup",
							MountPath: PVCMountPath,
							ReadOnly:  false,
						},
						{
							Name:      "registry-config",
							MountPath: RegistryPath,
						},
						{
							Name:      "registry-state",
							MountPath: RegistryStatePath,
						},
						{
							Name:      "registry-logs",
							MountPath: RegistryLogPath,
						},
					},
					Ports: []corev1.ContainerPort{
						{
							Name:          ContainerPortName,
							ContainerPort: ContainerPort,
							Protocol:      corev1.ProtocolTCP,
						},
					},
					SecurityContext: &corev1.SecurityContext{
						ReadOnlyRootFilesystem: &readOnlyFalse,
						Capabilities: &corev1.Capabilities{
							Add: []corev1.Capability{
								"DAC_READ_SEARCH",
								"DAC_OVERRIDE",
							},
						},
					},
					Resources: corev1.ResourceRequirements{
						Requests: corev1.ResourceList{
							corev1.ResourceCPU:    createQuantity("100m"),
							corev1.ResourceMemory: createQuantity("128Mi"),
						},
						Limits: corev1.ResourceList{
							corev1.ResourceCPU:    createQuantity("500m"),
							corev1.ResourceMemory: createQuantity("512Mi"),
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "pvc-backup",
					VolumeSource: corev1.VolumeSource{
						PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
							ClaimName: pvcToMount.Name,
						},
					},
				},
				{
					Name: "registry-config",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "registry-state",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
				{
					Name: "registry-logs",
					VolumeSource: corev1.VolumeSource{
						EmptyDir: &corev1.EmptyDirVolumeSource{},
					},
				},
			},
			RestartPolicy: corev1.RestartPolicyAlways,
			DNSPolicy:     corev1.DNSClusterFirst,
		},
	}

	if useSnapshot {
		pod.Annotations["pbs-plus.io/snapshot-pvc"] = pvcToMount.Name
		pod.Labels["pbs-plus.io/snapshot-backup"] = "true"
	}

	return pod
}

func (pm *PodManager) getPodName(pvc *corev1.PersistentVolumeClaim) string {
	sanitizedName := strings.ReplaceAll(pvc.Name, ".", "-")
	sanitizedName = strings.ToLower(sanitizedName)
	return AgentPodPrefix + sanitizedName
}

//go:fix inline
func int64Ptr(i int64) *int64 {
	return new(i)
}

func createQuantity(s string) resource.Quantity {
	return resource.MustParse(s)
}

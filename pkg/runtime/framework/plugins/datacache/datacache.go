/*
Copyright 2025 The Kubeflow Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package datacache

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apiruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	corev1ac "k8s.io/client-go/applyconfigurations/core/v1"
	metav1ac "k8s.io/client-go/applyconfigurations/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	lwsv1 "sigs.k8s.io/lws/api/leaderworkerset/v1"

	"github.com/go-logr/logr"
	trainer "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	"github.com/kubeflow/trainer/v2/pkg/runtime"
	"github.com/kubeflow/trainer/v2/pkg/runtime/framework"
)

// CacheConfig holds cache configuration parsed from TrainJob.
type CacheConfig struct {
	SchemaName  string
	TableName   string
	CacheImage  string
	IAMRole     string
	MetadataLoc string
	ClusterSize int32
	HeadCPU     string
	HeadMem     string
	WorkerCPU   string
	WorkerMem   string
}

// DataCache implements plugins for creating cache resources.
type DataCache struct {
	client client.Client
	scheme *apiruntime.Scheme
	logger logr.Logger
}

var _ framework.ComponentBuilderPlugin = (*DataCache)(nil)
var _ framework.WatchExtensionPlugin = (*DataCache)(nil)

// +kubebuilder:rbac:groups=leaderworkerset.x-k8s.io,resources=leaderworkersets,verbs=create;get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=services,verbs=create;get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=serviceaccounts,verbs=create;get;list;watch;update;patch

// New creates a new DataCache plugin.
func New(ctx context.Context, c client.Client, _ client.FieldIndexer) (framework.Plugin, error) {
	return &DataCache{
		client: c,
		scheme: c.Scheme(),
		logger: ctrl.LoggerFrom(ctx).WithValues("pluginName", Name),
	}, nil
}

// Name returns the plugin name.
func (d *DataCache) Name() string {
	return Name
}

// ReconcilerBuilders returns builders for watching LeaderWorkerSet resources.
func (d *DataCache) ReconcilerBuilders() []runtime.ReconcilerBuilder {
	if _, err := d.client.RESTMapper().RESTMapping(
		schema.GroupKind{Group: lwsv1.GroupVersion.Group, Kind: "LeaderWorkerSet"},
		lwsv1.GroupVersion.Version,
	); err != nil {
		d.logger.Error(err, "LeaderWorkerSet CRDs must be installed in advance")
	}
	return []runtime.ReconcilerBuilder{
		func(b *builder.Builder, cl client.Client, c cache.Cache) *builder.Builder {
			return b.Watches(
				&lwsv1.LeaderWorkerSet{},
				handler.EnqueueRequestForOwner(
					d.client.Scheme(), d.client.RESTMapper(), &trainer.TrainJob{}, handler.OnlyControllerOwner(),
				),
			)
		},
	}
}

// Build creates cache-related resources (LeaderWorkerSet, ServiceAccount, Service).
func (d *DataCache) Build(ctx context.Context, info *runtime.Info, trainJob *trainer.TrainJob) ([]apiruntime.ApplyConfiguration, error) {
	if info == nil || trainJob == nil {
		return nil, nil
	}

	// Check if cache dataset is configured
	if !hasCacheDataset(trainJob) {
		return nil, nil
	}

	config, err := parseCacheConfig(trainJob)
	if err != nil {
		return nil, fmt.Errorf("failed to parse cache config: %w", err)
	}

	d.logger.Info("Building cache resources",
		"trainJob", trainJob.Name,
		"schema", config.SchemaName,
		"table", config.TableName)

	ownerRef := metav1ac.OwnerReference().
		WithAPIVersion(trainer.GroupVersion.String()).
		WithKind(trainer.TrainJobKind).
		WithName(trainJob.Name).
		WithUID(trainJob.UID).
		WithController(true).
		WithBlockOwnerDeletion(true)

	// Build and apply LeaderWorkerSet directly since its ApplyConfiguration
	// doesn't implement runtime.ApplyConfiguration interface
	if err := d.applyLeaderWorkerSet(ctx, trainJob, config); err != nil {
		return nil, fmt.Errorf("failed to apply LeaderWorkerSet: %w", err)
	}

	serviceAccount := d.buildServiceAccount(trainJob, config, ownerRef)
	service := d.buildService(trainJob, config, ownerRef)

	return []apiruntime.ApplyConfiguration{
		serviceAccount,
		service,
	}, nil
}

// hasCacheDataset checks if the TrainJob has a cache:// dataset URI.
func hasCacheDataset(trainJob *trainer.TrainJob) bool {
	if trainJob == nil ||
		trainJob.Spec.Initializer == nil ||
		trainJob.Spec.Initializer.Dataset == nil ||
		trainJob.Spec.Initializer.Dataset.StorageUri == nil {
		return false
	}
	return strings.HasPrefix(*trainJob.Spec.Initializer.Dataset.StorageUri, CacheScheme+"://")
}

// parseCacheConfig extracts cache configuration from TrainJob.
func parseCacheConfig(trainJob *trainer.TrainJob) (*CacheConfig, error) {
	storageUri := *trainJob.Spec.Initializer.Dataset.StorageUri

	// Parse schema and table from URI: cache://schema/table
	uriPath := strings.TrimPrefix(storageUri, CacheScheme+"://")
	parts := strings.SplitN(uriPath, "/", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid cache URI format: %s, expected cache://schema/table", storageUri)
	}

	config := &CacheConfig{
		SchemaName:  parts[0],
		TableName:   parts[1],
		ClusterSize: DefaultClusterSize,
		HeadCPU:     DefaultHeadCPU,
		HeadMem:     DefaultHeadMem,
		WorkerCPU:   DefaultWorkerCPU,
		WorkerMem:   DefaultWorkerMem,
	}

	// Extract additional config from environment variables in the dataset initializer
	for _, envVar := range trainJob.Spec.Initializer.Dataset.Env {
		switch envVar.Name {
		case "CACHE_IMAGE":
			config.CacheImage = envVar.Value
		case "IAM_ROLE":
			config.IAMRole = envVar.Value
		case "METADATA_LOC":
			config.MetadataLoc = envVar.Value
		case "CLUSTER_SIZE":
			if size, err := parseIntOrDefault(envVar.Value, DefaultClusterSize); err == nil {
				config.ClusterSize = size
			}
		case "HEAD_CPU":
			config.HeadCPU = envVar.Value
		case "HEAD_MEM":
			config.HeadMem = envVar.Value
		case "WORKER_CPU":
			config.WorkerCPU = envVar.Value
		case "WORKER_MEM":
			config.WorkerMem = envVar.Value
		}
	}

	return config, nil
}

func parseIntOrDefault(s string, defaultVal int32) (int32, error) {
	if s == "" {
		return defaultVal, nil
	}
	var val int32
	_, err := fmt.Sscanf(s, "%d", &val)
	if err != nil {
		return defaultVal, err
	}
	return val, nil
}

// buildServiceAccount creates a ServiceAccount for cache pods.
func (d *DataCache) buildServiceAccount(trainJob *trainer.TrainJob, config *CacheConfig, ownerRef *metav1ac.OwnerReferenceApplyConfiguration) *corev1ac.ServiceAccountApplyConfiguration {
	saName := fmt.Sprintf("%s-cache", trainJob.Name)
	annotations := map[string]string{
		"eks.amazonaws.com/sts-regional-endpoints": "true",
	}
	if config.IAMRole != "" {
		annotations["eks.amazonaws.com/role-arn"] = config.IAMRole
	}

	return corev1ac.ServiceAccount(saName, trainJob.Namespace).
		WithAnnotations(annotations).
		WithOwnerReferences(ownerRef)
}

// buildService creates a Service for the cache head.
func (d *DataCache) buildService(trainJob *trainer.TrainJob, config *CacheConfig, ownerRef *metav1ac.OwnerReferenceApplyConfiguration) *corev1ac.ServiceApplyConfiguration {
	svcName := fmt.Sprintf("%s-cache-service", trainJob.Name)
	headLabel := fmt.Sprintf("%s-cache-head", trainJob.Name)

	return corev1ac.Service(svcName, trainJob.Namespace).
		WithOwnerReferences(ownerRef).
		WithSpec(corev1ac.ServiceSpec().
			WithSelector(map[string]string{LabelCacheHead: headLabel}).
			WithPorts(corev1ac.ServicePort().
				WithProtocol(corev1.ProtocolTCP).
				WithPort(GRPCPort).
				WithTargetPort(intstr.FromInt32(GRPCPort))))
}

// applyLeaderWorkerSet creates or updates the LeaderWorkerSet using SSA.
func (d *DataCache) applyLeaderWorkerSet(ctx context.Context, trainJob *trainer.TrainJob, config *CacheConfig) error {
	lwsName := fmt.Sprintf("%s-cache", trainJob.Name)
	saName := fmt.Sprintf("%s-cache", trainJob.Name)
	headLabel := fmt.Sprintf("%s-cache-head", trainJob.Name)

	// Build environment variables
	var envVars []corev1.EnvVar
	if config.MetadataLoc != "" {
		envVars = append(envVars, corev1.EnvVar{Name: EnvMetadataLoc, Value: config.MetadataLoc})
	}
	if config.TableName != "" {
		envVars = append(envVars, corev1.EnvVar{Name: EnvTableName, Value: config.TableName})
	}
	if config.SchemaName != "" {
		envVars = append(envVars, corev1.EnvVar{Name: EnvSchemaName, Value: config.SchemaName})
	}

	// Head container
	headContainer := corev1.Container{
		Name:    "head",
		Image:   config.CacheImage,
		Command: []string{"head"},
		Args:    []string{"0.0.0.0", "50051"},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(config.HeadCPU),
				corev1.ResourceMemory: resource.MustParse(config.HeadMem),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(config.HeadCPU),
				corev1.ResourceMemory: resource.MustParse(config.HeadMem),
			},
		},
		Env: envVars,
		Ports: []corev1.ContainerPort{
			{ContainerPort: GRPCPort, Name: "grpc"},
			{ContainerPort: HealthPort, Name: "health"},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/ready",
					Port: intstr.FromInt32(HealthPort),
				},
			},
			InitialDelaySeconds: DefaultReadinessInitialDelaySeconds,
			PeriodSeconds:       DefaultReadinessPeriodSeconds,
			TimeoutSeconds:      DefaultReadinessTimeoutSeconds,
			FailureThreshold:    DefaultReadinessFailureThreshold,
		},
	}

	// Worker container
	workerContainer := corev1.Container{
		Name:    "worker",
		Image:   config.CacheImage,
		Command: []string{"worker"},
		Args:    []string{"0.0.0.0", "50051"},
		Resources: corev1.ResourceRequirements{
			Limits: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(config.WorkerCPU),
				corev1.ResourceMemory: resource.MustParse(config.WorkerMem),
			},
			Requests: corev1.ResourceList{
				corev1.ResourceCPU:    resource.MustParse(config.WorkerCPU),
				corev1.ResourceMemory: resource.MustParse(config.WorkerMem),
			},
		},
		Env: envVars,
		Ports: []corev1.ContainerPort{
			{ContainerPort: GRPCPort, Name: "grpc"},
			{ContainerPort: HealthPort, Name: "health"},
		},
		ReadinessProbe: &corev1.Probe{
			ProbeHandler: corev1.ProbeHandler{
				HTTPGet: &corev1.HTTPGetAction{
					Path: "/ready",
					Port: intstr.FromInt32(HealthPort),
				},
			},
			InitialDelaySeconds: DefaultReadinessInitialDelaySeconds,
			PeriodSeconds:       DefaultReadinessPeriodSeconds,
			TimeoutSeconds:      DefaultReadinessTimeoutSeconds,
			FailureThreshold:    DefaultReadinessFailureThreshold,
		},
	}

	lws := &lwsv1.LeaderWorkerSet{
		TypeMeta: metav1.TypeMeta{
			APIVersion: lwsv1.GroupVersion.String(),
			Kind:       "LeaderWorkerSet",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      lwsName,
			Namespace: trainJob.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         trainer.GroupVersion.String(),
					Kind:               trainer.TrainJobKind,
					Name:               trainJob.Name,
					UID:                trainJob.UID,
					Controller:         ptr.To(true),
					BlockOwnerDeletion: ptr.To(true),
				},
			},
		},
		Spec: lwsv1.LeaderWorkerSetSpec{
			Replicas: ptr.To[int32](1),
			LeaderWorkerTemplate: lwsv1.LeaderWorkerTemplate{
				Size: ptr.To(config.ClusterSize),
				LeaderTemplate: &corev1.PodTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{LabelCacheHead: headLabel},
					},
					Spec: corev1.PodSpec{
						ServiceAccountName: saName,
						Containers:         []corev1.Container{headContainer},
					},
				},
				WorkerTemplate: corev1.PodTemplateSpec{
					Spec: corev1.PodSpec{
						ServiceAccountName: saName,
						Containers:         []corev1.Container{workerContainer},
					},
				},
			},
		},
	}

	return d.client.Patch(ctx, lws, client.Apply, client.FieldOwner("trainer"), client.ForceOwnership)
}

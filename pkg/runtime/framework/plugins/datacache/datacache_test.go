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
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	trainer "github.com/kubeflow/trainer/v2/pkg/apis/trainer/v1alpha1"
	trainerruntime "github.com/kubeflow/trainer/v2/pkg/runtime"
	"github.com/kubeflow/trainer/v2/pkg/runtime/framework"
	utiltesting "github.com/kubeflow/trainer/v2/pkg/util/testing"
)

func TestDataCacheName(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = trainer.AddToScheme(scheme)

	fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
	plugin, err := New(context.Background(), fakeClient, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if plugin.Name() != Name {
		t.Errorf("expected plugin name %s, got %s", Name, plugin.Name())
	}
}

func TestHasCacheDataset(t *testing.T) {
	cases := map[string]struct {
		trainJob *trainer.TrainJob
		want     bool
	}{
		"nil trainJob": {
			trainJob: nil,
			want:     false,
		},
		"nil initializer": {
			trainJob: utiltesting.MakeTrainJobWrapper(metav1.NamespaceDefault, "test-job").
				Obj(),
			want: false,
		},
		"nil dataset initializer": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: nil,
					},
				},
			},
			want: false,
		},
		"nil storage URI": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: nil,
						},
					},
				},
			},
			want: false,
		},
		"s3 scheme should not match": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("s3://bucket/path"),
						},
					},
				},
			},
			want: false,
		},
		"hf scheme should not match": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("hf://dataset/name"),
						},
					},
				},
			},
			want: false,
		},
		"cache scheme should match": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("cache://schema/table"),
						},
					},
				},
			},
			want: true,
		},
		"cache scheme with complex path should match": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("cache://my_schema/my_table"),
						},
					},
				},
			},
			want: true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := hasCacheDataset(tc.trainJob)
			if got != tc.want {
				t.Errorf("hasCacheDataset() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestParseCacheConfig(t *testing.T) {
	cases := map[string]struct {
		trainJob   *trainer.TrainJob
		wantConfig *CacheConfig
		wantErr    bool
	}{
		"valid cache URI": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("cache://test_schema/test_table"),
						},
					},
				},
			},
			wantConfig: &CacheConfig{
				SchemaName:  "test_schema",
				TableName:   "test_table",
				ClusterSize: DefaultClusterSize,
				HeadCPU:     DefaultHeadCPU,
				HeadMem:     DefaultHeadMem,
				WorkerCPU:   DefaultWorkerCPU,
				WorkerMem:   DefaultWorkerMem,
			},
			wantErr: false,
		},
		"invalid cache URI - no table": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("cache://schema_only"),
						},
					},
				},
			},
			wantConfig: nil,
			wantErr:    true,
		},
		"cache URI with env vars": {
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("cache://my_schema/my_table"),
							Env: []corev1.EnvVar{
								{Name: "CACHE_IMAGE", Value: "my-cache:latest"},
								{Name: "IAM_ROLE", Value: "arn:aws:iam::123456789:role/my-role"},
								{Name: "METADATA_LOC", Value: "s3://bucket/metadata"},
								{Name: "CLUSTER_SIZE", Value: "5"},
								{Name: "HEAD_CPU", Value: "2"},
								{Name: "HEAD_MEM", Value: "4Gi"},
								{Name: "WORKER_CPU", Value: "4"},
								{Name: "WORKER_MEM", Value: "8Gi"},
							},
						},
					},
				},
			},
			wantConfig: &CacheConfig{
				SchemaName:  "my_schema",
				TableName:   "my_table",
				CacheImage:  "my-cache:latest",
				IAMRole:     "arn:aws:iam::123456789:role/my-role",
				MetadataLoc: "s3://bucket/metadata",
				ClusterSize: 5,
				HeadCPU:     "2",
				HeadMem:     "4Gi",
				WorkerCPU:   "4",
				WorkerMem:   "8Gi",
			},
			wantErr: false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseCacheConfig(tc.trainJob)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if diff := cmp.Diff(tc.wantConfig, got); diff != "" {
				t.Errorf("parseCacheConfig() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func TestDataCacheBuild(t *testing.T) {
	cases := map[string]struct {
		info        *trainerruntime.Info
		trainJob    *trainer.TrainJob
		wantObjects int // Number of ApplyConfiguration objects expected
		wantErr     bool
	}{
		"nil info returns nil": {
			info:        nil,
			trainJob:    &trainer.TrainJob{},
			wantObjects: 0,
			wantErr:     false,
		},
		"nil trainJob returns nil": {
			info:        trainerruntime.NewInfo(),
			trainJob:    nil,
			wantObjects: 0,
			wantErr:     false,
		},
		"no cache dataset returns nil": {
			info: trainerruntime.NewInfo(),
			trainJob: utiltesting.MakeTrainJobWrapper(metav1.NamespaceDefault, "test-job").
				Obj(),
			wantObjects: 0,
			wantErr:     false,
		},
		"s3 dataset returns nil": {
			info: trainerruntime.NewInfo(),
			trainJob: &trainer.TrainJob{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-job",
					Namespace: metav1.NamespaceDefault,
				},
				Spec: trainer.TrainJobSpec{
					Initializer: &trainer.Initializer{
						Dataset: &trainer.DatasetInitializer{
							StorageUri: ptr.To("s3://bucket/path"),
						},
					},
				},
			},
			wantObjects: 0,
			wantErr:     false,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			scheme := runtime.NewScheme()
			_ = trainer.AddToScheme(scheme)

			fakeClient := fake.NewClientBuilder().WithScheme(scheme).Build()
			plugin, err := New(context.Background(), fakeClient, nil)
			if err != nil {
				t.Fatalf("unexpected error creating plugin: %v", err)
			}

			dcPlugin, ok := plugin.(framework.ComponentBuilderPlugin)
			if !ok {
				t.Fatalf("plugin does not implement ComponentBuilderPlugin")
			}

			objects, err := dcPlugin.Build(context.Background(), tc.info, tc.trainJob)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
				return
			}
			if len(objects) != tc.wantObjects {
				t.Errorf("Build() returned %d objects, want %d", len(objects), tc.wantObjects)
			}
		})
	}
}

func TestParseIntOrDefault(t *testing.T) {
	cases := map[string]struct {
		input      string
		defaultVal int32
		want       int32
		wantErr    bool
	}{
		"empty string returns default": {
			input:      "",
			defaultVal: 10,
			want:       10,
			wantErr:    false,
		},
		"valid int string": {
			input:      "5",
			defaultVal: 10,
			want:       5,
			wantErr:    false,
		},
		"invalid string returns default with error": {
			input:      "not-a-number",
			defaultVal: 10,
			want:       10,
			wantErr:    true,
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := parseIntOrDefault(tc.input, tc.defaultVal)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error but got none")
				}
			}
			if got != tc.want {
				t.Errorf("parseIntOrDefault(%q, %d) = %d, want %d", tc.input, tc.defaultVal, got, tc.want)
			}
		})
	}
}

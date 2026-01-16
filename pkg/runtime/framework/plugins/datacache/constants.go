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

const (
	// Name is the name of the DataCache plugin.
	Name = "DataCache"

	// CacheScheme is the URI scheme for cache datasets.
	CacheScheme = "cache"

	// Default container ports
	GRPCPort   = 50051
	HealthPort = 8080

	// Default resource values
	DefaultClusterSize = 3
	DefaultHeadCPU     = "1"
	DefaultHeadMem     = "1Gi"
	DefaultWorkerCPU   = "2"
	DefaultWorkerMem   = "2Gi"

	// Default readiness probe values
	DefaultReadinessInitialDelaySeconds = 5
	DefaultReadinessPeriodSeconds       = 10
	DefaultReadinessTimeoutSeconds      = 5
	DefaultReadinessFailureThreshold    = 3

	// Environment variable names
	EnvMetadataLoc = "METADATA_LOC"
	EnvTableName   = "TABLE_NAME"
	EnvSchemaName  = "SCHEMA_NAME"

	// Label/Annotation keys
	LabelCacheHead = "app"
)

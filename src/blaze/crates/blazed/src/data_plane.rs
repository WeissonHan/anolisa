// SPDX-License-Identifier: Apache-2.0
//! Standard file-backed implementation of the build-time provider contract.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use blaze_core::storage::{AcquireOpts, StorageProvider, StorageSlot};
use blaze_provider_api::{
    AbortRequest, AbortResult, CommitRequest, CommittedLease, DataPlaneProvider, FinalizeRequest,
    FinalizedLease, InspectRequest, LeaseBinding, LeaseState, PROVIDER_CONTRACT_VERSION,
    PrepareRequest, PrepareSource, PreparedLease, PreparedResources, ProviderCapabilities,
    ProviderDescriptor, ProviderError, ReleaseRequest, ReleaseResult, StopRequest, StoppedLease,
};
use uuid::Uuid;

#[derive(Debug, Clone)]
struct FileLease {
    binding: LeaseBinding,
    storage: StorageSlot,
    restore_payload_dir: Option<std::path::PathBuf>,
}

impl FileLease {
    fn resources(&self) -> PreparedResources {
        PreparedResources::PathBacked {
            storage: self.storage.clone(),
            restore_payload_dir: self.restore_payload_dir.clone(),
        }
    }
}

/// File-backed provider used by the standard `blazed` binary.
pub(crate) struct FileDataPlaneProvider {
    descriptor: ProviderDescriptor,
    storage: Arc<dyn StorageProvider>,
    leases: Mutex<HashMap<Uuid, FileLease>>,
}

impl FileDataPlaneProvider {
    /// Wrap the existing file storage implementation in the lifecycle contract.
    pub(crate) fn new(storage: Arc<dyn StorageProvider>) -> Self {
        Self {
            descriptor: ProviderDescriptor {
                contract_version: PROVIDER_CONTRACT_VERSION,
                provider_instance_id: Uuid::new_v4(),
            },
            storage,
            leases: Mutex::new(HashMap::new()),
        }
    }

    fn leases(&self) -> Result<MutexGuard<'_, HashMap<Uuid, FileLease>>, ProviderError> {
        self.leases
            .lock()
            .map_err(|_| ProviderError::OutcomeUnknown)
    }

    fn existing_prepare(
        &self,
        request: &PrepareRequest,
    ) -> Result<Option<PreparedLease>, ProviderError> {
        let leases = self.leases()?;
        let Some(lease) = leases.get(&request.context.lease_id) else {
            return Ok(None);
        };
        if lease.binding.context != request.context || lease.binding.state != LeaseState::Prepared {
            return Err(ProviderError::Conflict);
        }
        Ok(Some(PreparedLease {
            binding: lease.binding,
            resources: lease.resources(),
        }))
    }

    fn current(&self, binding: LeaseBinding) -> Result<FileLease, ProviderError> {
        let leases = self.leases()?;
        let lease = leases
            .get(&binding.context.lease_id)
            .ok_or(ProviderError::Conflict)?;
        if lease.binding != binding {
            return Err(ProviderError::Conflict);
        }
        Ok(lease.clone())
    }

    fn advance(
        &self,
        binding: LeaseBinding,
        expected: LeaseState,
        next: LeaseState,
    ) -> Result<LeaseBinding, ProviderError> {
        let mut leases = self.leases()?;
        let lease = leases
            .get_mut(&binding.context.lease_id)
            .ok_or(ProviderError::Conflict)?;
        if lease.binding != binding || binding.state != expected {
            return Err(ProviderError::Conflict);
        }
        lease.binding.generation = lease
            .binding
            .generation
            .checked_add(1)
            .ok_or(ProviderError::OutcomeUnknown)?;
        lease.binding.state = next;
        Ok(lease.binding)
    }

    async fn retain_failed_acquire(
        &self,
        binding: LeaseBinding,
        source: blaze_core::BlazeError,
        residual: Option<StorageSlot>,
    ) -> ProviderError {
        let Some(storage) = residual else {
            tracing::warn!(error = %source, "file provider preparation failed without residual resources");
            return ProviderError::Unavailable;
        };
        if self.storage.release(storage.clone()).await.is_ok() {
            tracing::warn!(error = %source, "file provider preparation failed and was compensated");
            return ProviderError::Unavailable;
        }
        let retained = FileLease {
            binding,
            storage,
            restore_payload_dir: None,
        };
        match self.leases() {
            Ok(mut leases) => {
                leases.insert(binding.context.lease_id, retained);
            }
            Err(error) => return error,
        }
        tracing::error!(error = %source, "file provider preparation outcome requires inspection");
        ProviderError::OutcomeUnknown
    }

    async fn release_binding(
        &self,
        binding: LeaseBinding,
        expected: &[LeaseState],
    ) -> Result<LeaseBinding, ProviderError> {
        let lease = self.current(binding)?;
        if !expected.contains(&binding.state) {
            return Err(ProviderError::Conflict);
        }
        self.storage
            .release_by_id(&lease.storage.id)
            .await
            .map_err(|error| {
                tracing::error!(%error, "file provider release remains incomplete");
                ProviderError::OutcomeUnknown
            })?;
        let mut leases = self.leases()?;
        let current = leases
            .remove(&binding.context.lease_id)
            .ok_or(ProviderError::OutcomeUnknown)?;
        if current.binding != binding {
            leases.insert(binding.context.lease_id, current);
            return Err(ProviderError::Conflict);
        }
        Ok(LeaseBinding {
            generation: binding
                .generation
                .checked_add(1)
                .ok_or(ProviderError::OutcomeUnknown)?,
            state: LeaseState::Released,
            ..binding
        })
    }
}

#[async_trait]
impl DataPlaneProvider for FileDataPlaneProvider {
    fn descriptor(&self) -> ProviderDescriptor {
        self.descriptor
    }

    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            images: true,
            templates: self.storage.supports_templates(),
            opened_restore_resources: false,
            daemon_managed_storage: true,
        }
    }

    async fn probe(&self) -> Result<(), ProviderError> {
        match self.storage.probe().await {
            Ok(true) => Ok(()),
            Ok(false) => Err(ProviderError::Unavailable),
            Err(error) => {
                tracing::warn!(%error, "file provider probe failed");
                Err(ProviderError::Unavailable)
            }
        }
    }

    async fn prepare(&self, request: PrepareRequest) -> Result<PreparedLease, ProviderError> {
        if let Some(existing) = self.existing_prepare(&request)? {
            return Ok(existing);
        }
        if request.context.instance_id.is_nil()
            || request.context.request_id.is_nil()
            || request.context.operation_id.is_nil()
            || request.context.lease_id.is_nil()
            || request.context.generation == 0
            || request.root_filesystem_bytes == 0
            || request.guest_memory_bytes == 0
        {
            return Err(ProviderError::InvalidResponse);
        }
        let binding = LeaseBinding {
            provider_instance_id: self.descriptor.provider_instance_id,
            context: request.context,
            generation: request.context.generation,
            state: LeaseState::Prepared,
        };
        let opts = AcquireOpts {
            instance_id: request.context.instance_id.to_string(),
            rootfs_size: request.root_filesystem_bytes,
            mem_size: request.guest_memory_bytes,
        };
        let (storage, restore_payload_dir) = match request.source {
            PrepareSource::Image { .. } => match self.storage.acquire(&opts).await {
                Ok(storage) => (storage, None),
                Err(error) => {
                    let (source, residual) = error.into_parts();
                    return Err(self.retain_failed_acquire(binding, source, residual).await);
                }
            },
            PrepareSource::Template(source) => {
                if !self.storage.supports_templates() {
                    return Err(ProviderError::Unsupported);
                }
                match self.storage.acquire_template(&opts, source.storage).await {
                    Ok(materialized) => (materialized.storage, Some(materialized.payload_dir)),
                    Err(error) => {
                        let (source, residual) = error.into_parts();
                        return Err(self.retain_failed_acquire(binding, source, residual).await);
                    }
                }
            }
        };
        let lease = FileLease {
            binding,
            storage,
            restore_payload_dir,
        };
        let resources = lease.resources();
        let collision = {
            let mut leases = self.leases()?;
            if let std::collections::hash_map::Entry::Vacant(entry) =
                leases.entry(request.context.lease_id)
            {
                entry.insert(lease.clone());
                false
            } else {
                true
            }
        };
        if collision {
            self.storage.release(lease.storage).await.map_err(|error| {
                tracing::error!(%error, "file provider could not compensate a concurrent prepare");
                ProviderError::OutcomeUnknown
            })?;
            return Err(ProviderError::Conflict);
        }
        Ok(PreparedLease { binding, resources })
    }

    async fn inspect(
        &self,
        request: InspectRequest,
    ) -> Result<blaze_provider_api::ObservedLease, ProviderError> {
        let leases = self.leases()?;
        let lease = leases
            .get(&request.context.lease_id)
            .ok_or(ProviderError::Conflict)?;
        if lease.binding.context != request.context {
            return Err(ProviderError::Conflict);
        }
        Ok(blaze_provider_api::ObservedLease {
            binding: lease.binding,
        })
    }

    async fn commit(&self, request: CommitRequest) -> Result<CommittedLease, ProviderError> {
        Ok(CommittedLease {
            binding: self.advance(request.binding, LeaseState::Prepared, LeaseState::Committed)?,
        })
    }

    async fn finalize(&self, request: FinalizeRequest) -> Result<FinalizedLease, ProviderError> {
        if request.public_transition.instance_id != request.binding.context.instance_id
            || request.public_transition.operation_id != request.binding.context.operation_id
        {
            return Err(ProviderError::Conflict);
        }
        Ok(FinalizedLease {
            binding: self.advance(
                request.binding,
                LeaseState::Committed,
                LeaseState::Finalized,
            )?,
        })
    }

    async fn abort(&self, request: AbortRequest) -> Result<AbortResult, ProviderError> {
        Ok(AbortResult {
            binding: self
                .release_binding(
                    request.binding,
                    &[LeaseState::Prepared, LeaseState::Committed],
                )
                .await?,
        })
    }

    async fn stop(&self, request: StopRequest) -> Result<StoppedLease, ProviderError> {
        Ok(StoppedLease {
            binding: self.advance(request.binding, LeaseState::Finalized, LeaseState::Stopped)?,
        })
    }

    async fn release(&self, request: ReleaseRequest) -> Result<ReleaseResult, ProviderError> {
        Ok(ReleaseResult {
            binding: self
                .release_binding(request.binding, &[LeaseState::Stopped])
                .await?,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use blaze_provider_api::{
        AbortRequest, CommitRequest, DataPlaneProvider, InspectRequest, LeaseState, PrepareRequest,
        PrepareSource, RequestContext,
    };
    use blaze_provider_conformance::exercise_create_delete;

    use crate::file_provider::FileStorageProvider;

    use super::*;

    fn request() -> PrepareRequest {
        PrepareRequest {
            context: RequestContext {
                instance_id: Uuid::new_v4(),
                request_id: Uuid::new_v4(),
                operation_id: Uuid::new_v4(),
                lease_id: Uuid::new_v4(),
                generation: 1,
            },
            source: PrepareSource::Image {
                image_digest: "sha256:test".to_string(),
            },
            root_filesystem_bytes: 4096,
            guest_memory_bytes: 4096,
        }
    }

    #[tokio::test]
    async fn file_provider_prepares_inspects_and_aborts_one_lease() {
        let temp = tempfile::tempdir().expect("temp");
        let images = temp.path().join("images");
        let instances = temp.path().join("instances");
        std::fs::create_dir_all(&images).expect("images");
        std::fs::create_dir_all(&instances).expect("instances");
        let storage: Arc<dyn StorageProvider> =
            Arc::new(FileStorageProvider::with_images(images, instances));
        let provider = FileDataPlaneProvider::new(storage);
        let request = request();
        let context = request.context;

        let prepared = provider.prepare(request).await.expect("prepare");
        assert_eq!(prepared.binding.state, LeaseState::Prepared);
        assert_eq!(
            provider
                .inspect(InspectRequest { context })
                .await
                .expect("inspect")
                .binding,
            prepared.binding
        );
        let committed = provider
            .commit(CommitRequest {
                binding: prepared.binding,
            })
            .await
            .expect("commit");
        let released = provider
            .abort(AbortRequest {
                binding: committed.binding,
            })
            .await
            .expect("abort");
        assert_eq!(released.binding.state, LeaseState::Released);
        assert!(provider.inspect(InspectRequest { context }).await.is_err());
    }

    #[tokio::test]
    async fn file_provider_passes_the_create_delete_contract() {
        let temp = tempfile::tempdir().expect("temp");
        let images = temp.path().join("images");
        let instances = temp.path().join("instances");
        std::fs::create_dir_all(&images).expect("images");
        std::fs::create_dir_all(&instances).expect("instances");
        let storage: Arc<dyn StorageProvider> =
            Arc::new(FileStorageProvider::with_images(images, instances));
        let provider = FileDataPlaneProvider::new(storage);

        exercise_create_delete(&provider, request())
            .await
            .expect("file provider lifecycle");
    }
}

// SPDX-License-Identifier: Apache-2.0
//! Reusable response validation for build-time data-plane providers.

#![forbid(unsafe_code)]

mod example_provider;

pub use example_provider::ExampleFileProvider;

use std::collections::HashSet;

use blaze_provider_api::{
    AttachmentAccess, AttachmentRole, AttachmentSharing, CheckpointSubmission, CommitRequest,
    DataPlaneProvider, FinalizeRequest, InspectRequest, InventorySnapshot, LeaseBinding,
    LeaseState, PROVIDER_CONTRACT_VERSION, PrepareRequest, PrepareSource, PreparedLease,
    PreparedResources, ProviderCheckpointRef, ProviderDescriptor, ProviderError,
    ProviderSuspensionRef, PublicTransitionRef, ReconcileAction, ReleaseRequest, RequestContext,
    RetireCheckpointResult, RetireSuspensionResult, StopRequest, SuspensionSubmission,
};
use thiserror::Error;

/// A provider response violated a source-level contract invariant.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ConformanceError {
    /// Provider descriptor cannot identify a compatible implementation.
    #[error("invalid provider descriptor")]
    InvalidDescriptor,
    /// Returned binding does not match the initiating request.
    #[error("provider lease binding does not match the request")]
    BindingMismatch,
    /// Returned state or generation is not the required next transition.
    #[error("provider lease transition is invalid")]
    InvalidTransition,
    /// Prepared resources cannot satisfy the selected source.
    #[error("prepared provider resources are invalid")]
    InvalidResources,
    /// Inventory snapshot or lease identity cannot be trusted.
    #[error("provider inventory is invalid")]
    InvalidInventory,
    /// Checkpoint identity, lineage, or provider content is invalid.
    #[error("provider checkpoint response is invalid")]
    InvalidCheckpoint,
    /// Suspension identity or provider content is invalid.
    #[error("provider suspension response is invalid")]
    InvalidSuspension,
}

/// Failure reported by the reusable create-and-delete contract exercise.
#[derive(Debug, Error)]
pub enum ExerciseError {
    /// The provider rejected or could not complete one operation.
    #[error(transparent)]
    Provider(#[from] ProviderError),
    /// A response violated a provider-independent contract invariant.
    #[error(transparent)]
    Conformance(#[from] ConformanceError),
}

/// Validate a provider descriptor before any mutating call.
pub fn validate_descriptor(descriptor: ProviderDescriptor) -> Result<(), ConformanceError> {
    if descriptor.contract_version != PROVIDER_CONTRACT_VERSION
        || descriptor.provider_instance_id.is_nil()
    {
        return Err(ConformanceError::InvalidDescriptor);
    }
    Ok(())
}

/// Validate a preparation result against the exact initiating request.
pub fn validate_prepared(
    context: RequestContext,
    template_source: bool,
    root_filesystem_bytes: u64,
    guest_memory_bytes: u64,
    prepared: &PreparedLease,
) -> Result<(), ConformanceError> {
    validate_prepared_binding(context, prepared.binding)?;
    match &prepared.resources {
        PreparedResources::PathBacked {
            storage,
            restore_payload_dir,
        } => {
            if storage.id != context.instance_id.to_string() {
                return Err(ConformanceError::InvalidResources);
            }
            if template_source != restore_payload_dir.is_some() {
                return Err(ConformanceError::InvalidResources);
            }
        }
        PreparedResources::OpenedRestore {
            restore_payload_dir,
            attachments,
        } => {
            if !template_source || restore_payload_dir.as_os_str().is_empty() {
                return Err(ConformanceError::InvalidResources);
            }
            validate_opened_attachments(attachments, root_filesystem_bytes, guest_memory_bytes)?;
        }
        PreparedResources::CheckpointRestore { .. }
        | PreparedResources::SuspensionRestore { .. } => {
            return Err(ConformanceError::InvalidResources);
        }
    }
    Ok(())
}

/// Validate that a prepared binding is safe to use for compensation.
///
/// Callers should not pass an untrusted binding to `abort`: a mismatched
/// provider, lease, or generation could identify resources owned by another
/// operation.
pub fn validate_prepared_binding(
    context: RequestContext,
    binding: LeaseBinding,
) -> Result<(), ConformanceError> {
    if context.instance_id.is_nil()
        || context.request_id.is_nil()
        || context.operation_id.is_nil()
        || context.lease_id.is_nil()
        || context.generation == 0
        || binding.provider_instance_id.is_nil()
        || binding.context != context
        || binding.generation != context.generation
        || binding.state != LeaseState::Prepared
    {
        return Err(ConformanceError::BindingMismatch);
    }
    Ok(())
}

/// Validate that one result is the exact next state of the same lease.
pub fn validate_transition(
    previous: LeaseBinding,
    next: LeaseBinding,
    expected: LeaseState,
) -> Result<(), ConformanceError> {
    if next.provider_instance_id != previous.provider_instance_id
        || next.context != previous.context
        || next.generation != previous.generation.saturating_add(1)
        || next.state != expected
    {
        return Err(ConformanceError::InvalidTransition);
    }
    Ok(())
}

/// Validate one frozen inventory identity before requesting any pages.
pub fn validate_inventory_snapshot(
    descriptor: ProviderDescriptor,
    snapshot: InventorySnapshot,
) -> Result<(), ConformanceError> {
    validate_descriptor(descriptor)?;
    if snapshot.provider_instance_id != descriptor.provider_instance_id
        || snapshot.snapshot_id.is_nil()
    {
        return Err(ConformanceError::InvalidInventory);
    }
    Ok(())
}

/// Validate one lease returned by a provider inventory.
pub fn validate_inventory_lease(
    descriptor: ProviderDescriptor,
    binding: LeaseBinding,
) -> Result<(), ConformanceError> {
    if binding.provider_instance_id != descriptor.provider_instance_id
        || binding.context.instance_id.is_nil()
        || binding.context.request_id.is_nil()
        || binding.context.operation_id.is_nil()
        || binding.context.lease_id.is_nil()
        || binding.context.generation == 0
        || binding.generation < binding.context.generation
    {
        return Err(ConformanceError::InvalidInventory);
    }
    Ok(())
}

/// Validate the exact transition required by one reconciliation action.
pub fn validate_reconcile_result(
    previous: LeaseBinding,
    next: LeaseBinding,
    action: ReconcileAction,
) -> Result<(), ConformanceError> {
    let expected = match action {
        ReconcileAction::Adopt { .. } => LeaseState::Finalized,
        ReconcileAction::Quarantine => LeaseState::Quarantined,
        ReconcileAction::Release => LeaseState::Released,
    };
    validate_transition(previous, next, expected)
}

/// Validate one immutable provider capture and its active-lease generation.
pub fn validate_checkpoint_submission(
    previous: LeaseBinding,
    checkpoint_id: uuid::Uuid,
    parent: Option<&ProviderCheckpointRef>,
    submission: &CheckpointSubmission,
) -> Result<(), ConformanceError> {
    validate_transition(previous, submission.binding, LeaseState::Finalized)?;
    let checkpoint = &submission.checkpoint;
    let digest = checkpoint.content_digest.strip_prefix("sha256:");
    if checkpoint_id.is_nil()
        || checkpoint.provider_instance_id != previous.provider_instance_id
        || checkpoint.public_checkpoint_id != checkpoint_id
        || checkpoint.reference_id.is_nil()
        || checkpoint.source_lease_id != previous.context.lease_id
        || checkpoint.source_generation != submission.binding.generation
        || checkpoint.parent_reference_id != parent.map(|parent| parent.reference_id)
        || (!checkpoint.root_filesystem && !checkpoint.guest_memory)
        || !digest.is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(ConformanceError::InvalidCheckpoint);
    }
    Ok(())
}

/// Validate resources prepared from one provider checkpoint.
pub fn validate_checkpoint_restore(
    context: RequestContext,
    checkpoint: &ProviderCheckpointRef,
    root_filesystem_bytes: u64,
    guest_memory_bytes: u64,
    prepared: &PreparedLease,
) -> Result<(), ConformanceError> {
    validate_prepared_binding(context, prepared.binding)?;
    if prepared.binding.provider_instance_id != checkpoint.provider_instance_id {
        return Err(ConformanceError::InvalidCheckpoint);
    }
    match &prepared.resources {
        PreparedResources::CheckpointRestore {
            storage: Some(storage),
            attachments,
        } if attachments.is_empty() && storage.id == context.instance_id.to_string() => {}
        PreparedResources::CheckpointRestore {
            storage: None,
            attachments,
        } => validate_opened_attachments(attachments, root_filesystem_bytes, guest_memory_bytes)?,
        _ => return Err(ConformanceError::InvalidResources),
    }
    Ok(())
}

fn validate_opened_attachments(
    attachments: &[blaze_provider_api::OpenedAttachment],
    root_filesystem_bytes: u64,
    guest_memory_bytes: u64,
) -> Result<(), ConformanceError> {
    if attachments.len() != 2 {
        return Err(ConformanceError::InvalidResources);
    }
    let mut roles = HashSet::new();
    for attachment in attachments {
        if !roles.insert(attachment.role)
            || attachment.access != AttachmentAccess::ReadWrite
            || attachment.sharing != AttachmentSharing::Exclusive
            || attachment.logical_size_bytes == 0
            || !attachment.logical_size_bytes.is_multiple_of(4096)
        {
            return Err(ConformanceError::InvalidResources);
        }
    }
    let root = attachments
        .iter()
        .find(|attachment| attachment.role == AttachmentRole::RootDrive)
        .ok_or(ConformanceError::InvalidResources)?;
    let memory = attachments
        .iter()
        .find(|attachment| attachment.role == AttachmentRole::GuestMemory)
        .ok_or(ConformanceError::InvalidResources)?;
    if root.logical_size_bytes != root_filesystem_bytes
        || memory.logical_size_bytes != guest_memory_bytes
    {
        return Err(ConformanceError::InvalidResources);
    }
    Ok(())
}

/// Validate idempotent retirement of one exact provider reference.
pub fn validate_checkpoint_retirement(
    checkpoint: &ProviderCheckpointRef,
    result: RetireCheckpointResult,
) -> Result<(), ConformanceError> {
    if checkpoint.reference_id.is_nil()
        || result.public_checkpoint_id != checkpoint.public_checkpoint_id
        || result.reference_id != Some(checkpoint.reference_id)
    {
        return Err(ConformanceError::InvalidCheckpoint);
    }
    Ok(())
}

/// Validate one immutable suspension capture and its active-lease generation.
pub fn validate_suspension_submission(
    previous: LeaseBinding,
    suspension_id: uuid::Uuid,
    root_filesystem_bytes: u64,
    guest_memory_bytes: u64,
    submission: &SuspensionSubmission,
) -> Result<(), ConformanceError> {
    validate_transition(previous, submission.binding, LeaseState::Finalized)?;
    let suspension = &submission.suspension;
    validate_suspension_reference(previous.provider_instance_id, suspension)?;
    if suspension_id.is_nil()
        || suspension.suspension_id != suspension_id
        || suspension.source_lease_id != previous.context.lease_id
        || suspension.source_generation != submission.binding.generation
        || suspension.root_filesystem_bytes != root_filesystem_bytes
        || suspension.guest_memory_bytes != guest_memory_bytes
    {
        return Err(ConformanceError::InvalidSuspension);
    }
    Ok(())
}

/// Validate the bounded identity and integrity shape of a suspension reference.
pub fn validate_suspension_reference(
    provider_instance_id: uuid::Uuid,
    suspension: &ProviderSuspensionRef,
) -> Result<(), ConformanceError> {
    let digest = suspension.content_digest.strip_prefix("sha256:");
    if provider_instance_id.is_nil()
        || suspension.provider_instance_id != provider_instance_id
        || suspension.suspension_id.is_nil()
        || suspension.reference_id.is_nil()
        || suspension.source_lease_id.is_nil()
        || suspension.source_generation == 0
        || (!suspension.root_filesystem && !suspension.guest_memory)
        || suspension.root_filesystem_bytes == 0
        || suspension.guest_memory_bytes == 0
        || !digest.is_some_and(|hex| {
            hex.len() == 64
                && hex
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
    {
        return Err(ConformanceError::InvalidSuspension);
    }
    Ok(())
}

/// Validate resources prepared from one immutable suspension reference.
pub fn validate_suspension_restore(
    context: RequestContext,
    suspension: &ProviderSuspensionRef,
    root_filesystem_bytes: u64,
    guest_memory_bytes: u64,
    prepared: &PreparedLease,
) -> Result<(), ConformanceError> {
    validate_prepared_binding(context, prepared.binding)?;
    if prepared.binding.provider_instance_id != suspension.provider_instance_id
        || root_filesystem_bytes != suspension.root_filesystem_bytes
        || guest_memory_bytes != suspension.guest_memory_bytes
    {
        return Err(ConformanceError::InvalidSuspension);
    }
    match &prepared.resources {
        PreparedResources::SuspensionRestore {
            storage: Some(storage),
            attachments,
        } if attachments.is_empty() && storage.id == context.instance_id.to_string() => {}
        PreparedResources::SuspensionRestore {
            storage: None,
            attachments,
        } => validate_opened_attachments(attachments, root_filesystem_bytes, guest_memory_bytes)?,
        _ => return Err(ConformanceError::InvalidResources),
    }
    Ok(())
}

/// Validate idempotent retirement of one exact provider suspension reference.
pub fn validate_suspension_retirement(
    suspension: &ProviderSuspensionRef,
    result: RetireSuspensionResult,
) -> Result<(), ConformanceError> {
    if suspension.reference_id.is_nil()
        || result.suspension_id != suspension.suspension_id
        || result.reference_id != Some(suspension.reference_id)
    {
        return Err(ConformanceError::InvalidSuspension);
    }
    Ok(())
}

/// Map a conformance violation to the public provider error category.
pub fn invalid_response(_: ConformanceError) -> ProviderError {
    ProviderError::InvalidResponse
}

/// Exercise the successful provider lifecycle without starting a backend.
///
/// This helper is intended for isolated provider tests. It verifies probe,
/// prepare, inspection, commit, public-state finalization, stop, and release
/// as one exact lease sequence. A caller remains responsible for testing real
/// backend consumption and every extension-defined compensation behavior.
pub async fn exercise_create_delete(
    provider: &(dyn DataPlaneProvider + Send + Sync),
    request: PrepareRequest,
) -> Result<(), ExerciseError> {
    let descriptor = provider.descriptor();
    validate_descriptor(descriptor)?;
    let capabilities = provider.capabilities();
    let template_source = matches!(&request.source, PrepareSource::Template(_));
    if (template_source && !capabilities.templates) || (!template_source && !capabilities.images) {
        return Err(ProviderError::Unsupported.into());
    }

    provider.probe().await?;
    let context = request.context;
    let root_filesystem_bytes = request.root_filesystem_bytes;
    let guest_memory_bytes = request.guest_memory_bytes;
    let prepared = provider.prepare(request).await?;
    validate_prepared(
        context,
        template_source,
        root_filesystem_bytes,
        guest_memory_bytes,
        &prepared,
    )?;
    if prepared.binding.provider_instance_id != descriptor.provider_instance_id {
        return Err(ConformanceError::BindingMismatch.into());
    }

    let observed = provider.inspect(InspectRequest { context }).await?;
    if observed.binding != prepared.binding {
        return Err(ConformanceError::InvalidTransition.into());
    }

    let committed = provider
        .commit(CommitRequest {
            binding: prepared.binding,
        })
        .await?;
    validate_transition(prepared.binding, committed.binding, LeaseState::Committed)?;

    let finalized = provider
        .finalize(FinalizeRequest {
            binding: committed.binding,
            public_transition: PublicTransitionRef {
                instance_id: context.instance_id,
                operation_id: context.operation_id,
            },
        })
        .await?;
    validate_transition(committed.binding, finalized.binding, LeaseState::Finalized)?;

    let stopped = provider
        .stop(StopRequest {
            binding: finalized.binding,
        })
        .await?;
    validate_transition(finalized.binding, stopped.binding, LeaseState::Stopped)?;

    let released = provider
        .release(ReleaseRequest {
            binding: stopped.binding,
        })
        .await?;
    validate_transition(stopped.binding, released.binding, LeaseState::Released)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::*;
    use blaze_core::storage::StorageSlot;
    use uuid::Uuid;

    fn binding(state: LeaseState, generation: u64) -> LeaseBinding {
        LeaseBinding {
            provider_instance_id: Uuid::new_v4(),
            context: RequestContext {
                instance_id: Uuid::new_v4(),
                request_id: Uuid::new_v4(),
                operation_id: Uuid::new_v4(),
                lease_id: Uuid::new_v4(),
                generation: 1,
            },
            generation,
            state,
        }
    }

    #[test]
    fn transition_requires_same_binding_and_next_generation() {
        let previous = binding(LeaseState::Prepared, 1);
        let next = LeaseBinding {
            generation: 2,
            state: LeaseState::Committed,
            ..previous
        };
        assert_eq!(
            validate_transition(previous, next, LeaseState::Committed),
            Ok(())
        );

        let stale = LeaseBinding {
            generation: 1,
            ..next
        };
        assert_eq!(
            validate_transition(previous, stale, LeaseState::Committed),
            Err(ConformanceError::InvalidTransition)
        );
    }

    #[test]
    fn inventory_and_reconciliation_reject_identity_drift() {
        let previous = binding(LeaseState::Finalized, 4);
        let descriptor = ProviderDescriptor {
            contract_version: PROVIDER_CONTRACT_VERSION,
            provider_instance_id: previous.provider_instance_id,
        };
        validate_inventory_snapshot(
            descriptor,
            InventorySnapshot {
                provider_instance_id: descriptor.provider_instance_id,
                snapshot_id: Uuid::new_v4(),
            },
        )
        .expect("snapshot");
        validate_inventory_lease(descriptor, previous).expect("inventory lease");

        let quarantined = LeaseBinding {
            generation: 5,
            state: LeaseState::Quarantined,
            ..previous
        };
        validate_reconcile_result(previous, quarantined, ReconcileAction::Quarantine)
            .expect("quarantine transition");

        let wrong_provider = LeaseBinding {
            provider_instance_id: Uuid::new_v4(),
            ..previous
        };
        assert_eq!(
            validate_inventory_lease(descriptor, wrong_provider),
            Err(ConformanceError::InvalidInventory)
        );
    }

    #[test]
    fn suspension_capture_and_restore_require_exact_identity_and_extents() {
        let previous = binding(LeaseState::Finalized, 4);
        let suspension_id = Uuid::new_v4();
        let suspension = ProviderSuspensionRef {
            provider_instance_id: previous.provider_instance_id,
            suspension_id,
            reference_id: Uuid::new_v4(),
            content_digest: format!("sha256:{}", "b".repeat(64)),
            source_lease_id: previous.context.lease_id,
            source_generation: 5,
            root_filesystem: true,
            guest_memory: true,
            root_filesystem_bytes: 4096,
            guest_memory_bytes: 8192,
        };
        let submission = SuspensionSubmission {
            binding: LeaseBinding {
                generation: 5,
                state: LeaseState::Finalized,
                ..previous
            },
            suspension: suspension.clone(),
        };
        validate_suspension_submission(previous, suspension_id, 4096, 8192, &submission)
            .expect("suspension capture");

        let context = RequestContext {
            instance_id: previous.context.instance_id,
            request_id: Uuid::new_v4(),
            operation_id: Uuid::new_v4(),
            lease_id: Uuid::new_v4(),
            generation: 1,
        };
        let prepared = PreparedLease {
            binding: LeaseBinding {
                provider_instance_id: previous.provider_instance_id,
                context,
                generation: 1,
                state: LeaseState::Prepared,
            },
            resources: PreparedResources::SuspensionRestore {
                storage: Some(StorageSlot {
                    id: context.instance_id.to_string(),
                    rootfs_path: PathBuf::new(),
                    mem_path: PathBuf::new(),
                    mem_diff_path: PathBuf::new(),
                    rootfs_diff_path: PathBuf::new(),
                    instance_dir: PathBuf::new(),
                }),
                attachments: Vec::new(),
            },
        };
        validate_suspension_restore(context, &suspension, 4096, 8192, &prepared)
            .expect("suspension restore");
        assert_eq!(
            validate_suspension_restore(context, &suspension, 4097, 8192, &prepared),
            Err(ConformanceError::InvalidSuspension)
        );

        let mut wrong = suspension.clone();
        wrong.guest_memory_bytes += 1;
        assert_eq!(
            validate_suspension_submission(
                previous,
                suspension_id,
                4096,
                8192,
                &SuspensionSubmission {
                    binding: submission.binding,
                    suspension: wrong,
                },
            ),
            Err(ConformanceError::InvalidSuspension)
        );
    }
}

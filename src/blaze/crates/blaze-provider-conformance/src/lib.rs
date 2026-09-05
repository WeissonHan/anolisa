// SPDX-License-Identifier: Apache-2.0
//! Reusable response validation for build-time data-plane providers.

#![forbid(unsafe_code)]

mod example_provider;

pub use example_provider::ExampleFileProvider;

use std::collections::HashSet;

use blaze_provider_api::{
    AttachmentAccess, AttachmentRole, AttachmentSharing, CommitRequest, DataPlaneProvider,
    FinalizeRequest, InspectRequest, LeaseBinding, LeaseState, PROVIDER_CONTRACT_VERSION,
    PrepareRequest, PrepareSource, PreparedLease, PreparedResources, ProviderDescriptor,
    ProviderError, PublicTransitionRef, ReleaseRequest, RequestContext, StopRequest,
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
            if !template_source
                || restore_payload_dir.as_os_str().is_empty()
                || attachments.len() != 2
            {
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
    use super::*;
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
}

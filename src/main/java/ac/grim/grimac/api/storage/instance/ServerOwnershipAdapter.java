package ac.grim.grimac.api.storage.instance;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Optional;
import java.util.UUID;

/**
 * Backend-specific primitive for authoritative DB-writer ownership.
 *
 * <p>This is intentionally not modeled as a normal Entity upsert: claim and
 * renew require compare-and-set semantics and affected-row style results.</p>
 */
@ApiStatus.Experimental
public interface ServerOwnershipAdapter {

    void ensureStore(@NotNull StoreId id) throws BackendException;

    long dbNowEpochMs() throws BackendException;

    @NotNull OwnershipClaimResult claimOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs,
            @NotNull ServerOwnershipMetadata metadata) throws BackendException;

    @NotNull OwnershipRenewResult renewOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs) throws BackendException;

    boolean closeOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            @NotNull String reason) throws BackendException;

    @NotNull Optional<ServerOwnershipSnapshot> readOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId) throws BackendException;

    /**
     * Claim an absent row when expected is null, or replace exactly the expected inactive grant.
     * Compare the predecessor, check expiry against backend time and write as one atomic operation.
     * On mismatch return denied with the current row (possibly absent), without changing it.
     */
    default OwnershipClaimResult claimRecovery(StoreId id, OwnershipToken candidate, @Nullable OwnershipToken expected,
                                              long ttlMs, ServerOwnershipMetadata metadata) throws BackendException {
        throw unsupported("recovery claim");
    }

    /**
     * Insert or update an OPEN startup. Reject identity mismatches and an already closed startup.
     * An older heartbeat cannot overwrite a newer heartbeat or manifest. Equal heartbeat times may
     * publish a new manifest. A non-null owner's instance and startup must identify the payload.
     * All non-null writer tokens in these methods require the exact open, unexpired grant at the
     * mutation's linearization point, using backend time. Reject invalid grants with BackendException.
     * A null writer is reserved for deployments explicitly operating without lease enforcement.
     */
    default void publishStartup(StartupStores stores, @Nullable OwnershipToken owner, ServerStartupRecord row) throws BackendException {
        throw unsupported("startup publication");
    }

    /**
     * Validate the destination and apply the conditional session move in the same atomic operation.
     * The destination startup must exist and be OPEN. A non-null owner must name that startup and its instance.
     * A null owner is reserved for heartbeat-only deployments. The destination row still supplies the instance.
     * The previous-startup and already-moved predicates of {@link EntityOps.ReassignSessionOp} are unchanged.
     */
    default long reassignSession(StartupStores stores, @Nullable OwnershipToken owner, EntityOps.ReassignSessionOp op)
            throws BackendException {
        throw unsupported("owned session reassignment");
    }

    /**
     * Insert a session or merge it into the stored row with the session entity's normal merge rules.
     * The row's startup must be non-null, exist, be OPEN and belong to the owner's instance and to the
     * row's instance when the row names one. A non-null owner must name the row's startup.
     * A stored player cannot change. A stored null startup adopts the row's startup. Otherwise an OPEN
     * write requires the stored startup to equal the row's. A terminal write may complete a retained
     * session of another startup of the same instance; the stored startup is kept. A stored terminal
     * row is left unchanged. Checks, first insert, close, repair and reassignment of the affected
     * startup and session are serialized by the backend. Reject every refused write with BackendException.
     */
    default void publishSession(StartupStores stores, @Nullable OwnershipToken owner, SessionRecord row)
            throws BackendException {
        throw unsupported("session publication");
    }

    /**
     * Close this startup's open sessions at backend time and its OPEN startup row as graceful, atomically.
     * Return the number of sessions closed, or zero if the startup is missing or already closed.
     * Reject mismatching instance/startup IDs, including a token naming a different startup.
     */
    default long closeStartup(StartupStores stores, @Nullable OwnershipToken owner, UUID instanceId, UUID startupId) throws BackendException {
        throw unsupported("graceful startup close");
    }

    /**
     * Atomically validate the sweeper, require an OPEN target of the given instance and establish staleness.
     * Enforced mode requires that no live lease of the target instance names the target startup.
     * Heartbeat-only mode (null sweeper) requires backend time minus the target heartbeat to exceed the TTL.
     * Close each open session at its own last activity and the startup as stale at the latest of its start,
     * heartbeat and, in enforced mode, last renewal of the matching target startup. Return the number of
     * sessions closed. Validate the sweeper even for its own startup, then return zero for own, live,
     * missing or already closed targets. Reject mismatched instance IDs on an existing target.
     * All checks and both mutations share a single linearization point, including concurrent owner creation.
     */
    default long repairStaleStartup(StartupStores stores, @Nullable OwnershipToken sweeper, UUID targetInstanceId,
                                    UUID targetStartupId, long staleHeartbeatTtlMs) throws BackendException {
        throw unsupported("stale startup repair");
    }

    /**
     * Checks that this backend instance can perform the atomic startup and session operations above,
     * including in heartbeat-only mode. Callers binding startup storage call it once, so a deployment that
     * cannot fence its session writes fails at boot instead of refusing every write later.
     */
    default void requireAtomicStartupOperations() throws BackendException {}

    private static BackendException unsupported(String operation) {
        return new BackendException("Backend does not support atomic " + operation);
    }
}

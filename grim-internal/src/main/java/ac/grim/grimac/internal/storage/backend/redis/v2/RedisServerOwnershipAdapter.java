package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipRenewResult;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.ServerOwnershipSnapshot;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.JedisPool;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Redis implementation of the server ownership lease primitive. Mutations use
 * Lua so the read-condition-write decision is atomic.
 */
@ApiStatus.Internal
public final class RedisServerOwnershipAdapter implements ServerOwnershipAdapter {

    private static final String CLAIM_SCRIPT = """
        local previous = redis.call('HGETALL', KEYS[1])
        local clock = redis.call('TIME')
        local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
        local expires = now + tonumber(ARGV[4])
        local closed = redis.call('HGET', KEYS[1], 'closed_at_ms')
        local oldExpires = tonumber(redis.call('HGET', KEYS[1], 'lease_expires_at_ms') or '0')
        local owner = redis.call('HGET', KEYS[1], 'owner_startup_id')
        local fence = redis.call('HGET', KEYS[1], 'fence')
        local inactive = not closed or tonumber(closed) ~= 0 or oldExpires <= now
        local allowed = inactive or (owner == ARGV[2] and fence == ARGV[3])
        if ARGV[9] == 'absent' then
          allowed = #previous == 0
        elseif ARGV[9] == 'exact' then
          allowed = #previous > 0 and inactive
            and redis.call('HGET', KEYS[1], 'persistent_id') == ARGV[10]
            and owner == ARGV[11] and fence == ARGV[12]
        end
        if allowed then
          redis.call('HSET', KEYS[1],
            'persistent_id', ARGV[1],
            'owner_startup_id', ARGV[2],
            'fence', ARGV[3],
            'lease_expires_at_ms', tostring(expires),
            'last_renewed_at_ms', tostring(now),
            'closed_at_ms', '0',
            'server_name', ARGV[5],
            'hostname', ARGV[6],
            'grim_version', ARGV[7],
            'server_version', ARGV[8])
          redis.call('HDEL', KEYS[1], 'close_reason')
          return {1, tostring(now), tostring(expires), previous}
        end
        return {0, tostring(now), tostring(expires), previous}
        """;

    private static final String RENEW_SCRIPT = """
        local clock = redis.call('TIME')
        local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
        local owner = redis.call('HGET', KEYS[1], 'owner_startup_id')
        local fence = redis.call('HGET', KEYS[1], 'fence')
        local closed = tonumber(redis.call('HGET', KEYS[1], 'closed_at_ms') or '0')
        local expires = tonumber(redis.call('HGET', KEYS[1], 'lease_expires_at_ms') or '0')
        if redis.call('HGET', KEYS[1], 'persistent_id') == ARGV[4]
           and owner == ARGV[1] and fence == ARGV[2] and closed == 0 and expires > now then
          local renewedUntil = now + tonumber(ARGV[3])
          redis.call('HSET', KEYS[1],
            'lease_expires_at_ms', tostring(renewedUntil),
            'last_renewed_at_ms', tostring(now),
            'closed_at_ms', '0')
          redis.call('HDEL', KEYS[1], 'close_reason')
          return {1, tostring(now), tostring(renewedUntil)}
        end
        return {0, tostring(now), '0'}
        """;

    private static final String CLOSE_SCRIPT = """
        local owner = redis.call('HGET', KEYS[1], 'owner_startup_id')
        local fence = redis.call('HGET', KEYS[1], 'fence')
        local closed = tonumber(redis.call('HGET', KEYS[1], 'closed_at_ms') or '0')
        if redis.call('HGET', KEYS[1], 'persistent_id') == ARGV[4]
           and owner == ARGV[1] and fence == ARGV[2] and closed == 0 then
          local clock = redis.call('TIME')
          local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
          redis.call('HSET', KEYS[1],
            'closed_at_ms', tostring(now),
            'close_reason', ARGV[3])
          return 1
        end
        return 0
        """;

    private final @NotNull JedisPool pool;
    private final @NotNull String keyPrefix;
    private final RedisStartupMutations startups;
    @SuppressWarnings("unused")
    private final @NotNull Logger logger;

    public RedisServerOwnershipAdapter(
            @NotNull JedisPool pool,
            @NotNull String keyPrefix,
            @NotNull Logger logger) {
        this.pool = pool;
        this.keyPrefix = keyPrefix;
        this.logger = logger;
        this.startups = new RedisStartupMutations(pool, keyPrefix);
    }

    @Override
    public void ensureStore(@NotNull StoreId id) {
        // Redis keys are created lazily by claimOwnership.
    }

    @Override
    public long dbNowEpochMs() throws BackendException {
        try (var j = pool.getResource()) {
            @SuppressWarnings("unchecked")
            List<String> time = (List<String>) j.time();
            long seconds = Long.parseLong(time.get(0));
            long micros = Long.parseLong(time.get(1));
            return seconds * 1000L + micros / 1000L;
        } catch (RuntimeException e) {
            throw new BackendException("failed to read Redis time", e);
        }
    }

    @Override
    public @NotNull OwnershipClaimResult claimOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs,
            @NotNull ServerOwnershipMetadata metadata) throws BackendException {
        return claim(id, new OwnershipToken(persistentId, startupId, fence), ttlMs, metadata, List.of());
    }

    @Override
    public OwnershipClaimResult claimRecovery(StoreId id, OwnershipToken candidate, @Nullable OwnershipToken expected,
                                              long ttlMs, ServerOwnershipMetadata metadata) throws BackendException {
        List<String> predecessor = expected == null ? List.of("absent")
                : List.of("exact", expected.instanceId().toString(), expected.startupId().toString(), expected.fence().toString());
        return claim(id, candidate, ttlMs, metadata, predecessor);
    }

    private OwnershipClaimResult claim(StoreId id, OwnershipToken candidate, long ttlMs, ServerOwnershipMetadata metadata,
                                       List<String> predecessor) throws BackendException {
        try {
            UUID persistentId = candidate.instanceId();
            UUID startupId = candidate.startupId();
            UUID fence = candidate.fence();
            var args = new ArrayList<>(List.of(
                    persistentId.toString(), startupId.toString(), fence.toString(), Long.toString(ttlMs),
                    nullToEmpty(metadata.serverName()), nullToEmpty(metadata.hostname()),
                    nullToEmpty(metadata.grimVersion()), nullToEmpty(metadata.serverVersionString())));
            args.addAll(predecessor);
            Object result;
            try (var j = pool.getResource()) {
                result = j.eval(CLAIM_SCRIPT, List.of(key(id, persistentId)), args);
            }
            if (!(result instanceof List<?> reply) || reply.size() != 4
                    || !(reply.get(3) instanceof List<?> fields) || fields.size() % 2 != 0) {
                throw new IllegalStateException("invalid Redis ownership claim response");
            }
            long now = number(reply.get(1));
            Map<String, String> hash = new java.util.HashMap<>();
            for (int i = 0; i < fields.size(); i += 2) {
                hash.put((String) fields.get(i), (String) fields.get(i + 1));
            }
            ServerOwnershipSnapshot previous = hash.isEmpty() ? null : snapshot(hash);
            if (number(reply.get(0)) == 1L) {
                return OwnershipClaimResult.claimed(
                        persistentId, startupId, fence, now, number(reply.get(2)), previous);
            }
            if (number(reply.get(0)) != 0L) {
                throw new IllegalStateException("invalid Redis ownership claim status");
            }
            return OwnershipClaimResult.denied(persistentId, startupId, fence, now, previous);
        } catch (RuntimeException e) {
            throw new BackendException("failed to claim Redis server ownership", e);
        }
    }

    @Override
    public void publishStartup(StartupStores stores, @Nullable OwnershipToken owner, ServerStartupRecord row) throws BackendException {
        startups.publish(stores, owner, row);
    }

    @Override
    public long reassignSession(StartupStores stores, @Nullable OwnershipToken owner, EntityOps.ReassignSessionOp op)
            throws BackendException {
        return startups.reassign(stores, owner, op);
    }

    @Override
    public void publishSession(StartupStores stores, @Nullable OwnershipToken owner, SessionRecord row) throws BackendException {
        startups.publishSession(stores, owner, row);
    }

    @Override
    public long closeStartup(StartupStores stores, @Nullable OwnershipToken owner, UUID instanceId, UUID startupId) throws BackendException {
        return startups.close(stores, owner, instanceId, startupId, null);
    }

    @Override
    public long repairStaleStartup(StartupStores stores, @Nullable OwnershipToken sweeper, UUID instanceId, UUID startupId,
                                   long staleHeartbeatTtlMs) throws BackendException {
        return startups.close(stores, sweeper, instanceId, startupId, staleHeartbeatTtlMs);
    }

    @Override
    public @NotNull OwnershipRenewResult renewOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs) throws BackendException {
        try {
            Object result;
            try (var j = pool.getResource()) {
                result = j.eval(RENEW_SCRIPT,
                        List.of(key(id, persistentId)),
                        List.of(startupId.toString(), fence.toString(), Long.toString(ttlMs), persistentId.toString()));
            }
            if (!(result instanceof List<?> reply) || reply.size() != 3) {
                throw new IllegalStateException("invalid Redis ownership renewal response");
            }
            long now = number(reply.get(1));
            if (number(reply.get(0)) == 1L) {
                return OwnershipRenewResult.renewed(persistentId, startupId, fence, now, number(reply.get(2)));
            }
            return OwnershipRenewResult.lost(persistentId, startupId, fence, now);
        } catch (RuntimeException e) {
            throw new BackendException("failed to renew Redis server ownership", e);
        }
    }

    @Override
    public boolean closeOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            @NotNull String reason) throws BackendException {
        try {
            Object result;
            try (var j = pool.getResource()) {
                result = j.eval(CLOSE_SCRIPT,
                        List.of(key(id, persistentId)),
                        List.of(startupId.toString(), fence.toString(), reason, persistentId.toString()));
            }
            return number(result) == 1L;
        } catch (RuntimeException e) {
            throw new BackendException("failed to close Redis server ownership", e);
        }
    }

    @Override
    public @NotNull Optional<ServerOwnershipSnapshot> readOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId) throws BackendException {
        try (var j = pool.getResource()) {
            Map<String, String> h = j.hgetAll(key(id, persistentId));
            if (h == null || h.isEmpty()) return Optional.empty();
            return Optional.of(snapshot(h));
        } catch (RuntimeException e) {
            throw new BackendException("failed to read Redis server ownership", e);
        }
    }

    private @NotNull String key(@NotNull StoreId id, @NotNull UUID persistentId) {
        return keyPrefix + id.name() + ":" + persistentId;
    }

    private static @NotNull ServerOwnershipSnapshot snapshot(@NotNull Map<String, String> h) {
        return new ServerOwnershipSnapshot(
                UUID.fromString(h.get("persistent_id")),
                UUID.fromString(h.get("owner_startup_id")),
                UUID.fromString(h.get("fence")),
                parseLong(h.get("lease_expires_at_ms")),
                parseLong(h.get("last_renewed_at_ms")),
                parseLong(h.get("closed_at_ms")),
                emptyToNull(h.get("close_reason")),
                emptyToNull(h.get("server_name")),
                emptyToNull(h.get("hostname")),
                emptyToNull(h.get("grim_version")),
                emptyToNull(h.get("server_version")));
    }

    private static long parseLong(String raw) {
        return raw == null || raw.isEmpty() ? 0L : Long.parseLong(raw);
    }

    private static long number(Object value) {
        return value instanceof Number n ? n.longValue() : Long.parseLong(String.valueOf(value));
    }

    private static @NotNull String nullToEmpty(String value) {
        return value == null ? "" : value;
    }

    private static String emptyToNull(String value) {
        return value == null || value.isEmpty() ? null : value;
    }
}

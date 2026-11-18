package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.jetbrains.annotations.Nullable;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Native lifecycle operations for the V2 entity layout. The ownership, startup, session and
 * index keys must share this pool's standalone Redis database and prefix. The router must establish
 * that all stores belong to the same physical backend before calling. The existing layout does
 * not co-locate its keys in Redis Cluster, so scripts explicitly refuse Cluster. The Redis ACL
 * must allow the topology probe (INFO cluster) as well as the normal hash and index commands.
 */
final class RedisStartupMutations {
    private static final String GUARD = """
        if string.find(redis.call('INFO', 'cluster'), 'cluster_enabled:1', 1, true) then
          error('atomic startup operations require standalone Redis')
        end
        local clock = redis.call('TIME')
        local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
        local fenced = ARGV[1] ~= ''
        local function live(key)
          return redis.call('HGET', key, 'closed_at_ms') == '0'
            and tonumber(redis.call('HGET', key, 'lease_expires_at_ms') or '0') > now
        end
        if fenced and (not live(KEYS[1])
          or redis.call('HGET', KEYS[1], 'persistent_id') ~= ARGV[1]
          or redis.call('HGET', KEYS[1], 'owner_startup_id') ~= ARGV[2]
          or redis.call('HGET', KEYS[1], 'fence') ~= ARGV[3]) then
          error('invalid or expired Redis ownership fence')
        end
        local function requireIndex(key)
          local kind = redis.call('TYPE', key).ok
          if kind ~= 'none' and kind ~= 'zset' then error('invalid Redis startup/session index') end
        end
        """;

    private static final String PUBLISH = GUARD + """
        local row, id, instance, server = KEYS[2], ARGV[4], ARGV[5], ARGV[6]
        local oldInstance = redis.call('HGET', row, 'instance_id')
        local oldServer = redis.call('HGET', row, 'server_name')
        if redis.call('EXISTS', row) == 1 then
          if oldInstance ~= instance or redis.call('HGET', row, 'startup_id') ~= id then
            error('Redis startup identity mismatch')
          end
          if redis.call('HGET', row, 'closed_at') ~= '0' then error('Redis startup is closed') end
          if tonumber(redis.call('HGET', row, 'last_heartbeat')) > tonumber(ARGV[8]) then return 0 end
        end
        local oldServerIndex = nil
        if oldServer and oldServer ~= server then
          oldServerIndex = ARGV[9] .. oldServer
          requireIndex(oldServerIndex)
        end
        requireIndex(KEYS[3])
        requireIndex(KEYS[4])
        requireIndex(KEYS[5])
        if oldServerIndex then redis.call('ZREM', oldServerIndex, id) end
        redis.call('HSET', row, 'startup_id', id, 'instance_id', instance, 'server_name', server,
          'started_at', ARGV[7], 'last_heartbeat', ARGV[8], 'closed_at', '0')
        redis.call('HDEL', row, 'grim_version', 'server_version', 'hostname', 'close_reason', 'verbose_manifest')
        for i = 10, #ARGV, 2 do redis.call('HSET', row, ARGV[i], ARGV[i + 1]) end
        redis.call('ZADD', KEYS[3], 0, id)
        redis.call('ZADD', KEYS[4], ARGV[8], id)
        redis.call('ZADD', KEYS[5], 0, id)
        return 1
        """;

    // KEYS[2] is the startup row. ARGV[4] is its ID, ARGV[5] the owner's instance and ARGV[6] the row's ('' when absent).
    private static final String OPEN_STARTUP = """
        local function requireOpenStartup(row)
          if redis.call('EXISTS', row) == 0 then error('Redis startup is missing') end
          if redis.call('HGET', row, 'startup_id') ~= ARGV[4] then error('Redis startup identity mismatch') end
          if redis.call('HGET', row, 'closed_at') ~= '0' then error('Redis startup is closed') end
          local instance = redis.call('HGET', row, 'instance_id')
          if (ARGV[5] ~= '' and instance ~= ARGV[5]) or (ARGV[6] ~= '' and instance ~= ARGV[6]) then
            error('Redis startup instance mismatch')
          end
          return instance
        end
        """;

    private static final String REASSIGN = GUARD + OPEN_STARTUP + """
        requireOpenStartup(KEYS[2])
        local destination = ARGV[4]
        local old = redis.call('HGET', KEYS[3], 'startup_id')
        if not old or old == destination then return 0 end
        local acceptable = false
        for i = 9, #ARGV do
          if old == ARGV[i] then acceptable = true; break end
        end
        if not acceptable then return 0 end
        local previousIndex = ARGV[8] .. old
        requireIndex(previousIndex)
        requireIndex(KEYS[4])
        redis.call('HSET', KEYS[3], 'startup_id', destination, 'closed_at', '0')
        redis.call('ZREM', previousIndex, ARGV[7])
        redis.call('ZADD', KEYS[4], 0, ARGV[7])
        return 1
        """;

    // KEYS: ownership, startup row, session row, retained startup row. ARGV[7] is '1' to require the
    // retained startup to share the instance, ARGV[8] a refusal decided from the snapshot ('' to proceed),
    // ARGV[9..12] the counts of snapshot fields, next fields, index removals and index additions.
    private static final String PUBLISH_SESSION = GUARD + OPEN_STARTUP + """
        local instance = requireOpenStartup(KEYS[2])
        local kind = redis.call('TYPE', KEYS[3]).ok
        if kind ~= 'none' and kind ~= 'hash' then error('invalid Redis session row') end
        local current = redis.call('HGETALL', KEYS[3])
        local expected = tonumber(ARGV[9])
        if #current ~= expected * 2 then return -1 end
        local stored = {}
        for i = 1, #current, 2 do stored[current[i]] = current[i + 1] end
        local at = 13
        for _ = 1, expected do
          if stored[ARGV[at]] ~= ARGV[at + 1] then return -1 end
          at = at + 2
        end
        -- The snapshot is current, so its decision is made at this linearization point.
        if ARGV[8] ~= '' then error(ARGV[8]) end
        if ARGV[7] ~= '' and (redis.call('EXISTS', KEYS[4]) == 0
            or redis.call('HGET', KEYS[4], 'instance_id') ~= instance) then
          error('Redis session belongs to another instance')
        end
        local nextAt, fields = at, tonumber(ARGV[10])
        local removeAt, removals = nextAt + fields * 2, tonumber(ARGV[11])
        local addAt, additions = removeAt + removals * 2, tonumber(ARGV[12])
        for i = 0, removals - 1 do requireIndex(ARGV[removeAt + i * 2]) end
        for i = 0, additions - 1 do requireIndex(ARGV[addAt + i * 3]) end
        if fields == 0 then return 0 end
        for i = 0, removals - 1 do redis.call('ZREM', ARGV[removeAt + i * 2], ARGV[removeAt + i * 2 + 1]) end
        redis.call('DEL', KEYS[3])
        for i = 0, fields - 1 do redis.call('HSET', KEYS[3], ARGV[nextAt + i * 2], ARGV[nextAt + i * 2 + 1]) end
        for i = 0, additions - 1 do
          redis.call('ZADD', ARGV[addAt + i * 3], ARGV[addAt + i * 3 + 1], ARGV[addAt + i * 3 + 2])
        end
        return 1
        """;

    private static final Entity<?, ?, ?> SESSIONS = V2BuiltinKinds.sessions();

    private static final String CLOSE = GUARD + """
        local row, targetInstance, startup = KEYS[3], ARGV[4], ARGV[5]
        if redis.call('EXISTS', row) == 0 then return 0 end
        if redis.call('HGET', row, 'instance_id') ~= targetInstance
          or redis.call('HGET', row, 'startup_id') ~= startup then error('Redis startup identity mismatch') end
        if redis.call('HGET', row, 'closed_at') ~= '0' then return 0 end
        local stale = ARGV[8] ~= ''
        local heartbeat = tonumber(redis.call('HGET', row, 'last_heartbeat'))
        if not heartbeat then error('invalid Redis startup heartbeat') end
        local closeAt = now
        if stale then
          local lastRenewed = 0
          if fenced then
            if startup == ARGV[2] then return 0 end
            if redis.call('HGET', KEYS[2], 'owner_startup_id') == startup then
              if redis.call('HGET', KEYS[2], 'persistent_id') ~= targetInstance then
                error('Redis target lease identity mismatch')
              end
              if live(KEYS[2]) then return 0 end
              lastRenewed = tonumber(redis.call('HGET', KEYS[2], 'last_renewed_at_ms') or '0')
            end
          elseif now - heartbeat <= tonumber(ARGV[8]) then return 0 end
          closeAt = math.max(tonumber(redis.call('HGET', row, 'started_at')), heartbeat, lastRenewed)
        end
        if closeAt <= 0 then error('invalid Redis startup close time') end
        local closedIndex = ARGV[7] .. tostring(closeAt)
        requireIndex(KEYS[4])
        requireIndex(KEYS[5])
        requireIndex(KEYS[6])
        requireIndex(closedIndex)
        local sessions = redis.call('ZRANGEBYSCORE', KEYS[4], 0, 0)
        local changes = {}
        -- Redis does not roll back a script error. Read and validate all touched rows before writing.
        for _, id in ipairs(sessions) do
          local session = ARGV[6] .. id
          if redis.call('HGET', session, 'startup_id') == startup
            and redis.call('HGET', session, 'closed_at') == '0' then
            local ended = now
            if stale then ended = tonumber(redis.call('HGET', session, 'last_activity')) end
            if not ended or ended <= 0 then error('invalid Redis session close time') end
            table.insert(changes, {session, id, tostring(ended)})
          end
        end
        for _, change in ipairs(changes) do
          redis.call('HSET', change[1], 'closed_at', change[3])
          redis.call('ZADD', KEYS[4], change[3], change[2])
        end
        redis.call('HSET', row, 'closed_at', tostring(closeAt), 'close_reason', stale and 'stale' or 'graceful')
        redis.call('ZADD', KEYS[5], closeAt, startup)
        redis.call('ZREM', KEYS[6], startup)
        redis.call('ZADD', closedIndex, heartbeat, startup)
        return #changes
        """;

    private final JedisPool pool;
    private final String prefix;

    RedisStartupMutations(JedisPool pool, String prefix) {
        this.pool = pool;
        this.prefix = prefix;
    }

    void publish(StartupStores stores, @Nullable OwnershipToken owner, ServerStartupRecord row) throws BackendException {
        requireIdentity(owner, row.instanceId(), row.startupId());
        if (row.isClosed()) throw new BackendException("Redis startup publication requires an open row");
        String startups = store(stores.startups());
        String byServer = index(startups, "by_server_name");
        var args = token(owner);
        add(args, row.startupId(), row.instanceId(), row.serverName(), row.startedEpochMs(), row.lastHeartbeatEpochMs(), byServer);
        addOptional(args, "grim_version", row.grimVersion());
        addOptional(args, "server_version", row.serverVersionString());
        addOptional(args, "hostname", row.hostname());
        addOptional(args, "verbose_manifest", row.verboseManifest());
        eval(PUBLISH, List.of(ownership(stores, row.instanceId()), startups + row.startupId(),
                index(startups, "by_instance_open") + row.instanceId(), index(startups, "by_open_heartbeat") + "0",
                byServer + row.serverName()), args);
    }

    long reassign(StartupStores stores, @Nullable OwnershipToken owner, EntityOps.ReassignSessionOp op) throws BackendException {
        if (owner != null && !owner.startupId().equals(op.startupId())) {
            throw new BackendException("Redis session destination does not match the ownership token");
        }
        String startup = store(stores.startups()) + op.startupId();
        String sessions = store(stores.sessions());
        String byStartup = index(sessions, "by_startup_open");
        var args = token(owner);
        add(args, op.startupId(), owner == null ? "" : owner.instanceId(), "", op.sessionId(), byStartup);
        for (UUID previous : op.previousStartups()) add(args, previous);
        return eval(REASSIGN, List.of(owner == null ? startup : ownership(stores, owner.instanceId()), startup,
                sessions + op.sessionId(), byStartup + op.startupId()), args);
    }

    void publishSession(StartupStores stores, @Nullable OwnershipToken owner, SessionRecord row) throws BackendException {
        try {
            publishSessionChecked(stores, owner, row);
        } catch (RuntimeException malformed) {
            // A malformed stored hash must reach callers as a refused write, like every other refusal.
            throw new BackendException("Redis session publication failed: " + malformed.getMessage(), malformed);
        }
    }

    private void publishSessionChecked(StartupStores stores, @Nullable OwnershipToken owner, SessionRecord row)
            throws BackendException {
        UUID startupId = row.startupId();
        if (startupId == null) throw new BackendException("Redis session publication requires a startup");
        if (owner != null) requireIdentity(owner, owner.instanceId(), startupId);
        String startups = store(stores.startups());
        String sessions = store(stores.sessions());
        String sessionKey = sessions + RedisKeys.encode(row.sessionId());
        for (int attempt = 0; attempt < 32; attempt++) {
            Map<byte[], byte[]> existing;
            try (var jedis = pool.getResource()) {
                existing = jedis.hgetAll(bytes(sessionKey));
            } catch (RuntimeException failure) {
                throw new BackendException("Redis session read failed", failure);
            }
            String refusal = "";
            UUID retained = null;
            Map<byte[], byte[]> next = null;
            if (!existing.isEmpty()) {
                String player = text(existing, "player_uuid");
                String storedStartup = text(existing, "startup_id");
                String closedAt = text(existing, "closed_at");
                if (player != null && !player.equals(row.playerUuid().toString())) {
                    refusal = "Redis session belongs to another player";
                } else if (storedStartup != null && !storedStartup.equals(startupId.toString())) {
                    if (!row.isClosed()) refusal = "Redis session belongs to another startup";
                    else retained = UUID.fromString(storedStartup);
                }
                boolean terminal = closedAt != null && !closedAt.isEmpty() && Long.parseLong(closedAt) != SessionRecord.OPEN;
                if (refusal.isEmpty() && !terminal) next = RedisEntityAdapter.mergeHash(SESSIONS, existing, row);
            } else {
                next = RedisEntityAdapter.mergeHash(SESSIONS, existing, row);
            }
            var before = next == null ? List.<RedisEntityAdapter.IndexEntry>of()
                    : RedisEntityAdapter.indexEntries(sessions, SESSIONS, existing);
            var after = next == null ? List.<RedisEntityAdapter.IndexEntry>of()
                    : RedisEntityAdapter.indexEntries(sessions, SESSIONS, next);
            var args = token(owner);
            add(args, startupId, owner == null ? "" : owner.instanceId(), row.instanceId() == null ? "" : row.instanceId(),
                    retained == null ? "" : "1", refusal, existing.size(), next == null ? 0 : next.size(),
                    before.size(), after.size());
            for (var field : existing.entrySet()) add(args, field.getKey(), field.getValue());
            if (next != null) for (var field : next.entrySet()) add(args, field.getKey(), field.getValue());
            for (var index : before) add(args, index.key(), index.member());
            for (var index : after) add(args, index.key(), Double.toString(index.score()), index.member());
            String startup = startups + startupId;
            long result = eval(PUBLISH_SESSION, List.of(owner == null ? startup : ownership(stores, owner.instanceId()),
                    startup, sessionKey, retained == null ? startup : startups + retained), args);
            if (result >= 0) return;
        }
        throw new BackendException("Redis session publication conflicted repeatedly");
    }

    private static @Nullable String text(Map<byte[], byte[]> hash, String field) {
        byte[] raw = RedisEntityAdapter.hashGet(hash, field);
        return raw == null ? null : new String(raw, StandardCharsets.UTF_8);
    }

    long close(StartupStores stores, @Nullable OwnershipToken actor, UUID instanceId, UUID startupId,
               @Nullable Long staleTtlMs) throws BackendException {
        if (staleTtlMs == null) requireIdentity(actor, instanceId, startupId);
        String startups = store(stores.startups());
        String sessions = store(stores.sessions());
        String byHeartbeat = index(startups, "by_open_heartbeat");
        String targetLease = ownership(stores, instanceId);
        var args = token(actor);
        add(args, instanceId, startupId, sessions, byHeartbeat, staleTtlMs == null ? "" : staleTtlMs);
        return eval(CLOSE, List.of(actor == null ? targetLease : ownership(stores, actor.instanceId()), targetLease,
                startups + startupId, index(sessions, "by_startup_open") + startupId,
                index(startups, "by_instance_open") + instanceId, byHeartbeat + "0"), args);
    }

    private long eval(String script, List<String> keys, List<byte[]> args) throws BackendException {
        try (var jedis = pool.getResource()) {
            return (Long) jedis.eval(bytes(script), keys.stream().map(RedisStartupMutations::bytes).toList(), args);
        } catch (RuntimeException failure) {
            throw new BackendException("Redis atomic startup/session mutation failed", failure);
        }
    }

    private String ownership(StartupStores stores, UUID instance) {
        return store(stores.ownership()) + instance;
    }

    private String store(StoreId id) {
        return prefix + id.name() + ":";
    }

    private static String index(String store, String name) {
        return store + "__idx:" + name + ":eq:";
    }

    private static void requireIdentity(@Nullable OwnershipToken token, UUID instance, UUID startup) throws BackendException {
        if (token != null && (!token.instanceId().equals(instance) || !token.startupId().equals(startup))) {
            throw new BackendException("Redis startup does not match the ownership token");
        }
    }

    private static List<byte[]> token(@Nullable OwnershipToken owner) {
        List<byte[]> args = new ArrayList<>();
        if (owner == null) add(args, "", "", "");
        else add(args, owner.instanceId(), owner.startupId(), owner.fence());
        return args;
    }

    private static void add(List<byte[]> args, Object... values) {
        for (Object value : values) args.add(value instanceof byte[] data ? data : bytes(value.toString()));
    }

    private static void addOptional(List<byte[]> args, String key, @Nullable Object value) {
        if (value != null) add(args, key, value);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}

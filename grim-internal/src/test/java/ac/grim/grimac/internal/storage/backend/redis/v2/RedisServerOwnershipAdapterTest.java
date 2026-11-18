package ac.grim.grimac.internal.storage.backend.redis.v2;

import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import redis.clients.jedis.DefaultJedisClientConfig;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.params.ShutdownParams;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class RedisServerOwnershipAdapterTest {
    private static final StoreId STORE = StoreId.grim("ownership");
    private static final StartupStores STORES = new StartupStores(STORE, StoreId.grim("startups"), StoreId.grim("sessions"));
    private static final String LOOPBACK = "127.0.0.1";
    private static final String REDIS_PASSWORD = UUID.randomUUID().toString();
    private static final DefaultJedisClientConfig REDIS_CLIENT_CONFIG = DefaultJedisClientConfig.builder()
            .password(REDIS_PASSWORD).timeoutMillis(1_000).build();
    private static final Set<ProcessHandle> REDIS_CHILDREN = new HashSet<>();
    private static Process redis;
    private static int port;
    private final UUID persistent = UUID.randomUUID();
    private final String prefix = "ownership-test:" + UUID.randomUUID() + ":";

    @BeforeAll
    static void startRedis(@TempDir Path directory) throws Exception {
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getByName(LOOPBACK))) {
            port = socket.getLocalPort();
        }
        try {
            redis = new ProcessBuilder("redis-server", "--bind", LOOPBACK, "--port", Integer.toString(port),
                    "--save", "", "--appendonly", "no", "--dir", ".", "--requirepass", REDIS_PASSWORD)
                    .directory(directory.toFile())
                    .redirectErrorStream(true).redirectOutput(directory.resolve("redis.log").toFile()).start();
        } catch (java.io.IOException unavailable) {
            assumeTrue(false, "redis-server is required for the real Redis ownership tests");
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        JedisException lastFailure = null;
        while (System.nanoTime() < deadline && redis.isAlive()) {
            redis.descendants().forEach(REDIS_CHILDREN::add);
            try (Jedis jedis = connectRedis()) {
                if ("PONG".equals(jedis.ping())) {
                    redis.descendants().forEach(REDIS_CHILDREN::add);
                    return;
                }
            } catch (JedisException starting) {
                lastFailure = starting;
            }
            Thread.sleep(20);
        }
        fail("Isolated Redis did not start on loopback port " + port, lastFailure);
    }

    @AfterAll
    static void stopRedis() throws Exception {
        if (redis == null) return;
        redis.descendants().forEach(REDIS_CHILDREN::add);
        try (Jedis jedis = connectRedis()) {
            // Jedis authenticates this connection with the fixture password before sending SHUTDOWN.
            jedis.shutdown(ShutdownParams.shutdownParams().nosave());
        } catch (JedisException unavailable) {
            // Startup may have failed or lost the port. Only the captured process tree is safe to terminate.
        }
        try {
            awaitRedisExit();
        } catch (TimeoutException timedOut) {
            redis.descendants().forEach(REDIS_CHILDREN::add);
            REDIS_CHILDREN.forEach(ProcessHandle::destroyForcibly);
            redis.destroyForcibly();
            awaitRedisExit();
        }
    }

    private static void awaitRedisExit() throws InterruptedException, ExecutionException, TimeoutException {
        // On Windows the launcher can exit while a child still holds redis.log open.
        var exits = Stream.concat(REDIS_CHILDREN.stream(), Stream.of(redis.toHandle()))
                .map(ProcessHandle::onExit).toArray(CompletableFuture[]::new);
        CompletableFuture.allOf(exits).get(5, TimeUnit.SECONDS);
    }

    private static Jedis connectRedis() {
        return new Jedis(new HostAndPort(LOOPBACK, port), REDIS_CLIENT_CONFIG);
    }

    @Test
    void atomicClaimReturnsAnInterveningOwnersFullSnapshot() throws Exception {
        UUID oldOwner = UUID.randomUUID();
        seed(oldOwner, UUID.randomUUID(), 1L);
        UUID intervening = UUID.randomUUID();
        UUID token = UUID.randomUUID();
        RedisServerOwnershipAdapter adapter = adapter(j -> seed(intervening, token, 2L), null);

        OwnershipClaimResult result = adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());

        assertTrue(result.claimed());
        assertEquals(intervening, result.previousOwner().ownerStartupId());
        assertEquals(token, result.previousOwner().fence());
        assertEquals(2L, result.previousOwner().leaseExpiresAtEpochMs());
        assertEquals("test", result.previousOwner().serverName());
        assertEquals(20_000, result.leaseExpiresAtEpochMs() - result.dbNowEpochMs());
    }

    @Test
    void rejectionCannotBecomeSuccessWhenTheRowDisappearsAfterTheScript() throws Exception {
        UUID owner = UUID.randomUUID();
        seed(owner, UUID.randomUUID(), System.currentTimeMillis() + 60_000);
        RedisServerOwnershipAdapter adapter = adapter(null, j -> {
            try (Jedis connection = connectRedis()) { connection.del(key()); }
        });
        OwnershipClaimResult result = adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());
        assertFalse(result.claimed());
        assertEquals(owner, result.currentOwner().ownerStartupId());
    }

    @Test
    void newClaimRenewCloseAndReclaimUseTheSameContract() throws Exception {
        RedisServerOwnershipAdapter adapter = adapter(null, null);
        UUID startup = UUID.randomUUID();
        UUID fence = UUID.randomUUID();
        OwnershipClaimResult first = adapter.claimOwnership(STORE, persistent, startup, fence, 20_000, metadata());
        assertTrue(first.claimed());
        assertNull(first.previousOwner());
        assertTrue(adapter.renewOwnership(STORE, persistent, startup, fence, 20_000).renewed());
        assertTrue(adapter.closeOwnership(STORE, persistent, startup, fence, "shutdown"));
        OwnershipClaimResult next = adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());
        assertTrue(next.claimed());
        assertEquals(startup, next.previousOwner().ownerStartupId());
        assertEquals("shutdown", next.previousOwner().closeReason());
        assertFalse(adapter.renewOwnership(STORE, persistent, startup, fence, 20_000).renewed());
    }

    @Test
    void expiredLeaseRejectsRenewalAndCanBeReclaimedWithFreshIdentity() throws Exception {
        RedisServerOwnershipAdapter adapter = adapter(null, null);
        UUID oldStartup = UUID.randomUUID();
        UUID oldFence = UUID.randomUUID();
        // A persisted lease whose deadline passed while the DB was unavailable.
        seed(oldStartup, oldFence, 1L);
        assertFalse(adapter.renewOwnership(STORE, persistent, oldStartup, oldFence, 20_000).renewed());
        UUID newStartup = UUID.randomUUID();
        UUID newFence = UUID.randomUUID();
        OwnershipClaimResult claim = adapter.claimOwnership(STORE, persistent, newStartup, newFence, 20_000, metadata());
        assertTrue(claim.claimed());
        assertEquals(oldStartup, claim.previousOwner().ownerStartupId());
        assertEquals(oldFence, claim.previousOwner().fence());
        assertTrue(adapter.renewOwnership(STORE, persistent, newStartup, newFence, 20_000).renewed());
        assertFalse(adapter.closeOwnership(STORE, persistent, oldStartup, oldFence, "stale"));
    }

    @Test
    void renewalIgnoresAClockSampleThatPredatesTheRedisMutation() throws Exception {
        UUID startup = UUID.randomUUID();
        UUID fence = UUID.randomUUID();
        seed(startup, fence, 1L);
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenAnswer(call -> {
            Jedis connection = spy(connectRedis());
            doReturn(List.of("0", "0")).when(connection).time();
            return connection;
        });
        var adapter = new RedisServerOwnershipAdapter(pool, prefix, Logger.getAnonymousLogger());

        var renewal = adapter.renewOwnership(STORE, persistent, startup, fence, 20_000);

        assertFalse(renewal.renewed());
        assertTrue(renewal.dbNowEpochMs() > 1);
        assertEquals(1, adapter.readOwnership(STORE, persistent).orElseThrow().leaseExpiresAtEpochMs());
    }

    @Test
    void recoveryChecksThePredecessorEvenWhenTheInterveningLeaseHasExpired() throws Exception {
        var first = token(persistent);
        var intervening = token(persistent);
        var candidate = token(persistent);
        seed(first.startupId(), first.fence(), 1);
        var adapter = adapter(j -> seed(intervening.startupId(), intervening.fence(), 2), null);
        var rejected = adapter.claimRecovery(STORE, candidate, first, 20_000, metadata());
        assertFalse(rejected.claimed());
        assertEquals(intervening, OwnershipToken.of(rejected.currentOwner()));
        assertEquals(2, adapter.readOwnership(STORE, persistent).orElseThrow().leaseExpiresAtEpochMs());

        adapter = adapter(null, null);
        assertFalse(adapter.claimRecovery(STORE, candidate, null, 20_000, metadata()).claimed());
        assertTrue(adapter.claimRecovery(STORE, candidate, intervening, 20_000, metadata()).claimed());
        assertFalse(adapter.claimRecovery(STORE, candidate, candidate, 20_000, metadata()).claimed());
        var absent = token(UUID.randomUUID());
        var missing = adapter.claimRecovery(STORE, absent, first, 20_000, metadata());
        assertFalse(missing.claimed());
        assertNull(missing.currentOwner());
        assertTrue(adapter.claimRecovery(STORE, absent, null, 20_000, metadata()).claimed());
    }

    @Test
    void startupPublicationChecksFenceIdentityRecencyAndTerminalState() throws Exception {
        var adapter = adapter(null, null);
        var owner = claim(adapter, persistent);
        var entities = entities();
        adapter.publishStartup(STORES, owner, startup(owner, 10, 30, new byte[]{0, -1}));
        adapter.publishStartup(STORES, owner, startup(owner, 10, 20, new byte[]{2}));
        assertEquals(30, readStartup(entities, owner).lastHeartbeatEpochMs());
        assertArrayEquals(new byte[]{0, -1}, readStartup(entities, owner).verboseManifest());
        adapter.publishStartup(STORES, owner, startup(owner, 10, 30, new byte[]{3}));
        assertArrayEquals(new byte[]{3}, readStartup(entities, owner).verboseManifest());
        assertThrows(BackendException.class, () -> adapter.publishStartup(STORES, token(persistent), startup(owner, 10, 40, null)));
        assertThrows(BackendException.class, () -> adapter.publishStartup(STORES, null,
                startup(new OwnershipToken(UUID.randomUUID(), owner.startupId(), owner.fence()), 10, 40, null)));

        UUID session = writeSession(entities, owner, 25, 0);
        long before = adapter.dbNowEpochMs();
        assertEquals(1, adapter.closeStartup(STORES, owner, persistent, owner.startupId()));
        var terminal = readStartup(entities, owner);
        assertEquals("graceful", terminal.closeReason());
        assertTrue(terminal.closedAtEpochMs() >= before);
        assertEquals(terminal.closedAtEpochMs(), readSession(entities, session).closedAtEpochMs());
        assertEquals(0, adapter.closeStartup(STORES, owner, persistent, owner.startupId()));
        assertThrows(BackendException.class, () -> adapter.publishStartup(STORES, owner, startup(owner, 10, 40, null)));
        assertEquals(0, openStartupCount(entities));
    }

    @Test
    void reassignmentChecksTheDestinationFenceAndPreservesAnAlreadyTargetedClosure() throws Exception {
        var adapter = adapter(null, null);
        var owner = claim(adapter, persistent);
        var previous = token(persistent);
        var entities = entities();
        UUID id = writeSession(entities, previous, 20, 20);
        var move = new EntityOps.ReassignSessionOp(Categories.SESSION, id,
                Set.of(previous.startupId()), owner.startupId());
        assertThrows(BackendException.class, () -> adapter.reassignSession(STORES, owner, move));
        adapter.publishStartup(STORES, owner, startup(owner, 10, 30, null));
        var stale = new OwnershipToken(persistent, owner.startupId(), UUID.randomUUID());
        assertThrows(BackendException.class, () -> adapter.reassignSession(STORES, stale, move));
        assertEquals(previous.startupId(), readSession(entities, id).startupId());
        assertEquals(1, adapter.reassignSession(STORES, owner, move));
        assertEquals(owner.startupId(), readSession(entities, id).startupId());
        assertEquals(0, readSession(entities, id).closedAtEpochMs());
        entities.execute(STORES.sessions(), V2BuiltinKinds.sessions(), new EntityOps.SetIfSentinelOp(
                Categories.SESSION, null, id, "closed_at", 0L, 45L, null));
        assertEquals(0, adapter.reassignSession(STORES, owner, move));
        assertEquals(45, readSession(entities, id).closedAtEpochMs());
    }

    @Test
    void heartbeatOnlyReassignmentRequiresAnOpenDestination() throws Exception {
        var adapter = adapter(null, null);
        var previous = token(persistent);
        var next = token(persistent);
        var entities = entities();
        UUID id = writeSession(entities, previous, 20, 0);
        var move = new EntityOps.ReassignSessionOp(Categories.SESSION, id, Set.of(previous.startupId()), next.startupId());
        assertThrows(BackendException.class, () -> adapter.reassignSession(STORES, null, move));
        adapter.publishStartup(STORES, null, startup(next, 10, 30, null));
        assertEquals(1, adapter.reassignSession(STORES, null, move));
        assertEquals(next.startupId(), readSession(entities, id).startupId());
        UUID late = writeSession(entities, previous, 21, 0);
        assertEquals(1, adapter.closeStartup(STORES, null, persistent, next.startupId()));
        assertThrows(BackendException.class, () -> adapter.reassignSession(STORES, null,
                new EntityOps.ReassignSessionOp(Categories.SESSION, late, Set.of(previous.startupId()), next.startupId())));
        assertEquals(previous.startupId(), readSession(entities, late).startupId());
    }

    @Test
    void sessionPublicationChecksTheGrantStartupAndStoredRowAtomically() throws Exception {
        var adapter = adapter(null, null);
        var owner = claim(adapter, persistent);
        var prior = token(persistent);
        var foreign = token(UUID.randomUUID());
        var entities = entities();
        UUID player = UUID.randomUUID();
        UUID id = UUID.randomUUID();
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, session(id, player, owner, 15, 0)));
        adapter.publishStartup(STORES, owner, startup(owner, 10, 30, null));
        adapter.publishStartup(STORES, null, startup(prior, 5, 9, null));
        adapter.publishStartup(STORES, null, startup(foreign, 5, 9, null));
        var stale = new OwnershipToken(persistent, owner.startupId(), UUID.randomUUID());
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, stale, session(id, player, owner, 15, 0)));
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, session(id, player, prior, 15, 0)));

        adapter.publishSession(STORES, owner, session(id, player, owner, 15, 0));
        adapter.publishSession(STORES, owner, session(id, player, owner, 18, 0));
        assertEquals(18, readSession(entities, id).lastActivityEpochMs());
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, session(id, UUID.randomUUID(), owner, 19, 0)));

        UUID retained = UUID.randomUUID();
        UUID other = UUID.randomUUID();
        adapter.publishSession(STORES, null, session(retained, player, prior, 15, 0));
        adapter.publishSession(STORES, null, session(other, player, foreign, 15, 0));
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, session(retained, player, owner, 16, 0)));
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, session(other, player, owner, 16, 17)));
        adapter.publishSession(STORES, owner, session(retained, player, owner, 16, 17));
        assertEquals(prior.startupId(), readSession(entities, retained).startupId());
        assertEquals(17, readSession(entities, retained).closedAtEpochMs());
        adapter.publishSession(STORES, null, session(retained, player, prior, 30, 0));
        assertEquals(16, readSession(entities, retained).lastActivityEpochMs());

        // The published row is in the open-session index, so the graceful close reaches it.
        assertEquals(1, adapter.closeStartup(STORES, owner, persistent, owner.startupId()));
        long closedAt = readSession(entities, id).closedAtEpochMs();
        assertTrue(closedAt > 0);
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, session(id, player, owner, 25, 0)));
        UUID late = UUID.randomUUID();
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, null, session(late, player, owner, 25, 0)));
        try (Jedis connection = connectRedis()) {
            assertFalse(connection.exists(prefix + STORES.sessions().name() + ":" + late));
        }
        assertEquals(closedAt, readSession(entities, id).closedAtEpochMs());
    }

    @Test
    void staleRepairChecksBothLeasesAndClosesOnlyItsTargetsOpenSessions() throws Exception {
        var adapter = adapter(null, null);
        var sweeper = claim(adapter, persistent);
        var target = claim(adapter, UUID.randomUUID());
        adapter.publishStartup(STORES, sweeper, startup(sweeper, 10, 30, null));
        adapter.publishStartup(STORES, target, startup(target, 10, 30, null));
        var entities = entities();
        UUID active = writeSession(entities, target, 42, 0);
        UUID alreadyClosed = writeSession(entities, target, 45, 50);
        UUID unrelated = writeSession(entities, sweeper, 52, 0);
        assertEquals(0, adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 0));
        assertEquals(0, adapter.repairStaleStartup(STORES, sweeper, sweeper.instanceId(), sweeper.startupId(), 0));
        assertThrows(BackendException.class, () -> adapter.repairStaleStartup(STORES, sweeper,
                UUID.randomUUID(), target.startupId(), 0));
        try (Jedis connection = connectRedis()) {
            connection.hset(ownershipKey(target), Map.of("lease_expires_at_ms", "1", "last_renewed_at_ms", "85"));
        }
        var wrongFence = new OwnershipToken(persistent, sweeper.startupId(), UUID.randomUUID());
        assertThrows(BackendException.class, () -> adapter.repairStaleStartup(STORES, wrongFence,
                target.instanceId(), target.startupId(), 0));
        assertEquals(0, readSession(entities, active).closedAtEpochMs());
        assertEquals(1, adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 0));
        assertEquals(42, readSession(entities, active).closedAtEpochMs());
        assertEquals(50, readSession(entities, alreadyClosed).closedAtEpochMs());
        assertEquals(0, readSession(entities, unrelated).closedAtEpochMs());
        assertEquals(85, readStartup(entities, target).closedAtEpochMs());
        assertEquals("stale", readStartup(entities, target).closeReason());
        assertEquals(1, openStartupCount(entities));
        assertEquals(0, adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 0));
    }

    @Test
    void successorLeaseDoesNotContributeItsRenewalTimeToTheOldStartup() throws Exception {
        var adapter = adapter(null, null);
        var previous = claim(adapter, persistent);
        var entities = entities();
        adapter.publishStartup(STORES, previous, startup(previous, 10, 30, null));
        UUID session = writeSession(entities, previous, 42, 0);
        assertTrue(adapter.closeOwnership(STORE, persistent, previous.startupId(), previous.fence(), "handoff"));
        var successor = token(persistent);
        assertTrue(adapter.claimRecovery(STORE, successor, previous, 60_000, metadata()).claimed());

        assertEquals(1, adapter.repairStaleStartup(STORES, successor, persistent, previous.startupId(), 0));
        assertEquals(30, readStartup(entities, previous).closedAtEpochMs());
        assertEquals(42, readSession(entities, session).closedAtEpochMs());
    }

    @Test
    void ensuringTheSessionStorePreservesExistingRowsAndTheirExpiration() throws Exception {
        var adapter = adapter(null, null);
        var target = token(persistent);
        adapter.publishStartup(STORES, null, startup(target, 10, 30, null));
        var entities = entities();
        UUID session = writeSession(entities, target, 42, 0);
        String row = prefix + STORES.sessions().name() + ":" + session;
        try (Jedis connection = connectRedis()) {
            assertEquals(1, connection.expire(row, 60));
            entities.ensureStore(STORES.sessions(), V2BuiltinKinds.sessions());
            assertTrue(connection.ttl(row) > 0);
        }

        assertEquals(1, adapter.repairStaleStartup(STORES, null, persistent, target.startupId(), 0));
        assertEquals(42, readSession(entities, session).closedAtEpochMs());
    }

    @Test
    void heartbeatOnlyRepairRespectsTtlWithoutALease() throws Exception {
        var adapter = adapter(null, null);
        var target = token(persistent);
        long heartbeat = adapter.dbNowEpochMs() - 1_000;
        adapter.publishStartup(STORES, null, startup(target, heartbeat - 1, heartbeat, null));
        var entities = entities();
        UUID session = writeSession(entities, target, heartbeat + 1, 0);
        assertEquals(0, adapter.repairStaleStartup(STORES, null, persistent, target.startupId(), 60_000));
        assertEquals(1, adapter.repairStaleStartup(STORES, null, persistent, target.startupId(), 0));
        assertEquals(heartbeat, readStartup(entities, target).closedAtEpochMs());
        assertEquals(heartbeat + 1, readSession(entities, session).closedAtEpochMs());
    }

    @Test
    void malformedSessionPreventsPartialStaleRepair() throws Exception {
        var adapter = adapter(null, null);
        var target = token(persistent);
        adapter.publishStartup(STORES, null, startup(target, 10, 30, null));
        var entities = entities();
        UUID good = writeSession(entities, target, 42, 0);
        UUID corrupt = writeSession(entities, target, 43, 0);
        try (Jedis connection = connectRedis()) {
            connection.hset(prefix + STORES.sessions().name() + ":" + corrupt, "last_activity", "invalid");
        }

        assertThrows(BackendException.class,
                () -> adapter.repairStaleStartup(STORES, null, persistent, target.startupId(), 0));
        assertEquals(0, readStartup(entities, target).closedAtEpochMs());
        assertEquals(0, readSession(entities, good).closedAtEpochMs());
        assertEquals(1, openStartupCount(entities));
    }

    @Test
    void aLeaseInAnotherRedisDatabaseCannotAuthorizeAPublication() throws Exception {
        var owner = claim(adapter(null, null), persistent);
        JedisPool otherDatabase = mock(JedisPool.class);
        when(otherDatabase.getResource()).thenAnswer(call -> {
            var connection = connectRedis();
            connection.select(1);
            return connection;
        });
        var other = new RedisServerOwnershipAdapter(otherDatabase, prefix, Logger.getAnonymousLogger());
        assertThrows(BackendException.class, () -> other.publishStartup(STORES, owner, startup(owner, 10, 30, null)));
        try (Jedis connection = connectRedis()) {
            connection.select(1);
            assertFalse(connection.exists(prefix + STORES.startups().name() + ":" + owner.startupId()));
        }
    }

    private static OwnershipToken token(UUID instance) {
        return new OwnershipToken(instance, UUID.randomUUID(), UUID.randomUUID());
    }

    private OwnershipToken claim(RedisServerOwnershipAdapter adapter, UUID instance) throws Exception {
        var token = token(instance);
        assertTrue(adapter.claimRecovery(STORE, token, null, 60_000, metadata()).claimed());
        return token;
    }

    private String ownershipKey(OwnershipToken token) {
        return prefix + STORE.name() + ":" + token.instanceId();
    }

    private static ServerStartupRecord startup(OwnershipToken token, long started, long heartbeat, byte[] manifest) {
        return new ServerStartupRecord(token.startupId(), token.instanceId(), "test", "1", "paper", null,
                started, heartbeat, 0, null, manifest);
    }

    private RedisEntityAdapter entities() {
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenAnswer(call -> connectRedis());
        return new RedisEntityAdapter(pool, prefix, Logger.getAnonymousLogger());
    }

    private static ServerStartupRecord readStartup(RedisEntityAdapter adapter, OwnershipToken token) throws Exception {
        return adapter.execute(STORES.startups(), V2BuiltinKinds.serverStartups(),
                new EntityOps.GetByIdOp<UUID, ServerStartupRecord>(Categories.SERVER_STARTUP, token.startupId())).orElseThrow();
    }

    private static long openStartupCount(RedisEntityAdapter adapter) throws Exception {
        return adapter.execute(STORES.startups(), V2BuiltinKinds.serverStartups(),
                new EntityOps.CountByIndexOp(Categories.SERVER_STARTUP, "by_open_heartbeat", 0L));
    }

    private static UUID writeSession(RedisEntityAdapter adapter, OwnershipToken token, long activity, long closed) throws Exception {
        UUID id = UUID.randomUUID();
        var record = new SessionRecord(id, UUID.randomUUID(), "test", 1, activity, closed,
                "1", null, 47, "paper", token.instanceId(), token.startupId(), List.of());
        adapter.execute(STORES.sessions(), V2BuiltinKinds.sessions(), new EntityOps.UpsertOp<>(Categories.SESSION, record));
        return id;
    }

    private static SessionRecord session(UUID id, UUID player, OwnershipToken token, long activity, long closed) {
        return new SessionRecord(id, player, "test", 1, activity, closed, "1", null, 47, "paper", null, token.startupId(), List.of());
    }

    private static SessionRecord readSession(RedisEntityAdapter adapter, UUID id) throws Exception {
        return adapter.execute(STORES.sessions(), V2BuiltinKinds.sessions(),
                new EntityOps.GetByIdOp<UUID, SessionRecord>(Categories.SESSION, id)).orElseThrow();
    }

    @Test
    void concurrentClaimsAdmitExactlyOneOwner() throws Exception {
        RedisServerOwnershipAdapter adapter = adapter(null, null);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch start = new CountDownLatch(1);
        Callable<OwnershipClaimResult> claim = () -> {
            ready.countDown();
            assertTrue(start.await(5, TimeUnit.SECONDS));
            return adapter.claimOwnership(STORE, persistent, UUID.randomUUID(), UUID.randomUUID(), 20_000, metadata());
        };
        try {
            Future<OwnershipClaimResult> first = executor.submit(claim);
            Future<OwnershipClaimResult> second = executor.submit(claim);
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            start.countDown();
            OwnershipClaimResult a = first.get(5, TimeUnit.SECONDS);
            OwnershipClaimResult b = second.get(5, TimeUnit.SECONDS);
            assertNotEquals(a.claimed(), b.claimed());
            OwnershipClaimResult winner = a.claimed() ? a : b;
            OwnershipClaimResult loser = a.claimed() ? b : a;
            assertEquals(winner.startupId(), loser.currentOwner().ownerStartupId());
        } finally {
            start.countDown();
            executor.shutdownNow();
        }
    }

    private RedisServerOwnershipAdapter adapter(Consumer<Jedis> before, Consumer<Jedis> after) {
        JedisPool pool = mock(JedisPool.class);
        when(pool.getResource()).thenAnswer(call -> {
            Jedis connection = spy(connectRedis());
            doAnswer(eval -> {
                if (before != null) before.accept(connection);
                Object result = eval.callRealMethod();
                if (after != null) after.accept(connection);
                return result;
            }).when(connection).eval(anyString(), anyList(), anyList());
            return connection;
        });
        return new RedisServerOwnershipAdapter(pool, prefix, Logger.getLogger(getClass().getName()));
    }

    private void seed(UUID owner, UUID fence, long expires) {
        try (Jedis jedis = connectRedis()) {
            jedis.hset(key(), Map.of("persistent_id", persistent.toString(), "owner_startup_id", owner.toString(),
                    "fence", fence.toString(), "lease_expires_at_ms", Long.toString(expires),
                    "last_renewed_at_ms", "1", "closed_at_ms", "0", "server_name", "test"));
        }
    }

    private String key() { return prefix + STORE.name() + ":" + persistent; }
    private static ServerOwnershipMetadata metadata() { return new ServerOwnershipMetadata("test", null, null, null); }
}

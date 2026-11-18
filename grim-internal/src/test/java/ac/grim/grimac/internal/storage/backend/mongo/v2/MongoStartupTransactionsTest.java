package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.event.CommandListener;
import com.mongodb.event.CommandStartedEvent;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;

import ac.grim.grimac.api.storage.model.SessionRecord;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = "grim.test.mongo.ownership.port", matches = "[0-9]+")
class MongoStartupTransactionsTest {
    private static final StartupStores STORES = new StartupStores(
            StoreId.grim("ownership"), StoreId.grim("startups"), StoreId.grim("sessions"));
    private static final long TTL = 120_000L;
    private static final ServerOwnershipMetadata METADATA = new ServerOwnershipMetadata("test", null, null, null);
    private final AtomicReference<Runnable> beforeTargetLock = new AtomicReference<>();
    private MongoClient client;
    private MongoDatabase db;
    private MongoServerOwnershipAdapter adapter;

    @BeforeEach
    void openReplicaSetDatabase() throws Exception {
        int port = Integer.getInteger("grim.test.mongo.ownership.port");
        assertTrue(port >= 1024 && port <= 65535);
        client = MongoClients.create(MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString("mongodb://127.0.0.1:" + port + "/?serverSelectionTimeoutMS=5000"))
                .addCommandListener(new CommandListener() {
                    @Override public void commandStarted(CommandStartedEvent event) {
                        if (event.getCommandName().equals("findAndModify")
                                && event.getCommand().getString("findAndModify").getValue().equals(STORES.ownership().name())
                                && event.getCommand().containsKey("autocommit")) {
                            Runnable barrier = beforeTargetLock.getAndSet(null);
                            if (barrier != null) barrier.run();
                        }
                    }
                }).build());
        db = client.getDatabase("grim_ownership_test_" + UUID.randomUUID().toString().replace("-", ""));
        MongoStartupTransactions.requireReplicaSet(db.runCommand(new Document("hello", 1)));
        adapter = new MongoServerOwnershipAdapter(client, db, Logger.getAnonymousLogger());
        adapter.ensureStore(STORES.ownership());
        db.createCollection(STORES.startups().name());
        db.createCollection(STORES.sessions().name());
    }

    @AfterEach
    void closeIsolatedDatabase() {
        if (client == null) return;
        try {
            if (db != null) db.drop();
        } finally {
            client.close();
        }
    }

    @Test
    void comparesTheExactInactivePredecessorAndFencesStartupAndSessionMutations() throws Exception {
        OwnershipToken owner = claim(UUID.randomUUID());
        OwnershipToken replacement = token(owner.instanceId());
        assertFalse(adapter.claimRecovery(STORES.ownership(), replacement, null, TTL, METADATA).claimed());
        assertFalse(adapter.claimRecovery(STORES.ownership(), replacement, owner, TTL, METADATA).claimed());

        adapter.publishStartup(STORES, owner, startup(owner, 100, 200, 1));
        adapter.publishStartup(STORES, owner, startup(owner, 100, 199, 2));
        assertArrayEquals(new byte[]{1}, startupRow(owner).get("verbose_manifest", Binary.class).getData());
        adapter.publishStartup(STORES, owner, startup(owner, 100, 200, 3));
        assertArrayEquals(new byte[]{3}, startupRow(owner).get("verbose_manifest", Binary.class).getData());

        UUID previous = UUID.randomUUID();
        UUID moving = session(previous, 210, 211);
        var move = new EntityOps.ReassignSessionOp(Categories.SESSION, moving, Set.of(previous, owner.startupId()), owner.startupId());
        assertEquals(1, adapter.reassignSession(STORES, owner, move));
        collection(STORES.sessions()).updateOne(id(moving), Updates.set("closed_at", 212L));
        assertEquals(0, adapter.reassignSession(STORES, owner, move));
        assertEquals(212L, sessionRow(moving).getLong("closed_at"));

        UUID open = session(owner.startupId(), 210, 0);
        UUID unrelated = session(previous, 210, 0);
        assertEquals(1, adapter.closeStartup(STORES, owner, owner.instanceId(), owner.startupId()));
        assertEquals(startupRow(owner).getLong("closed_at"), sessionRow(open).getLong("closed_at"));
        assertEquals("graceful", startupRow(owner).getString("close_reason"));
        assertEquals(0L, sessionRow(unrelated).getLong("closed_at"));
        assertThrows(BackendException.class, () -> adapter.publishStartup(STORES, owner, startup(owner, 100, 201, 4)));

        assertTrue(adapter.closeOwnership(STORES.ownership(), owner.instanceId(), owner.startupId(), owner.fence(), "test"));
        assertFalse(adapter.claimRecovery(STORES.ownership(), replacement, token(owner.instanceId()), TTL, METADATA).claimed());
        assertTrue(adapter.claimRecovery(STORES.ownership(), replacement, owner, TTL, METADATA).claimed());
        assertThrows(BackendException.class, () -> adapter.reassignSession(STORES, owner, move));
    }

    @Test
    void staleRepairUsesTheMatchingRenewalAndClosesOnlyOpenSessions() throws Exception {
        OwnershipToken sweeper = claim(UUID.randomUUID());
        OwnershipToken target = claim(UUID.randomUUID());
        adapter.publishStartup(STORES, target, startup(target, 100, 200, 1));
        UUID first = session(target.startupId(), 220, 0);
        UUID second = session(target.startupId(), 230, 0);
        UUID alreadyClosed = session(target.startupId(), 225, 235);
        assertEquals(0, adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 1));
        assertEquals(0L, sessionRow(first).getLong("closed_at"));

        collection(STORES.ownership()).updateOne(id(target.instanceId()), Updates.combine(
                Updates.set("lease_expires_at_ms", 400L), Updates.set("last_renewed_at_ms", 300L)));
        assertEquals(2, adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 1));
        assertEquals(220L, sessionRow(first).getLong("closed_at"));
        assertEquals(230L, sessionRow(second).getLong("closed_at"));
        assertEquals(235L, sessionRow(alreadyClosed).getLong("closed_at"));
        assertEquals(300L, startupRow(target).getLong("closed_at"));
        assertEquals("stale", startupRow(target).getString("close_reason"));
        assertEquals(0, adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 1));
    }

    @Test
    void staleRepairWithoutATargetLeaseLeavesTheOwnershipKeyAbsent() throws Exception {
        OwnershipToken sweeper = claim(UUID.randomUUID());
        OwnershipToken target = token(UUID.randomUUID());
        adapter.publishStartup(STORES, null, startup(target, 100, 200, 1));
        UUID open = session(target.startupId(), 220, 0);

        assertEquals(1, adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 1));
        assertEquals(200L, startupRow(target).getLong("closed_at"));
        assertEquals(220L, sessionRow(open).getLong("closed_at"));
        assertTrue(adapter.readOwnership(STORES.ownership(), target.instanceId()).isEmpty());
        assertTrue(adapter.claimRecovery(STORES.ownership(), target, null, TTL, METADATA).claimed());
    }

    @Test
    void expiredGrantCannotWriteButItsExactPredecessorCanBeReclaimed() throws Exception {
        OwnershipToken owner = claim(UUID.randomUUID());
        OwnershipToken successor = token(owner.instanceId());
        assertTrue(adapter.renewOwnership(STORES.ownership(), owner.instanceId(), owner.startupId(), owner.fence(), TTL).renewed());
        collection(STORES.ownership()).updateOne(id(owner.instanceId()), Updates.set("lease_expires_at_ms", 1L));

        assertFalse(adapter.renewOwnership(STORES.ownership(), owner.instanceId(), owner.startupId(), owner.fence(), TTL).renewed());
        assertThrows(BackendException.class, () -> adapter.publishStartup(STORES, owner, startup(owner, 100, 200, 1)));
        assertEquals(0, collection(STORES.startups()).countDocuments());
        assertFalse(adapter.claimRecovery(STORES.ownership(), successor, token(owner.instanceId()), TTL, METADATA).claimed());
        assertTrue(adapter.claimRecovery(STORES.ownership(), successor, owner, TTL, METADATA).claimed());
        UUID absent = UUID.randomUUID();
        var missing = adapter.claimRecovery(STORES.ownership(), token(absent), token(absent), TTL, METADATA);
        assertFalse(missing.claimed());
        assertNull(missing.currentOwner());
    }

    @Test
    void heartbeatOnlyRepairKeepsARecentStartupOpen() throws Exception {
        OwnershipToken target = token(UUID.randomUUID());
        long now = adapter.dbNowEpochMs();
        adapter.publishStartup(STORES, null, startup(target, now - 1000, now, 1));
        UUID open = session(target.startupId(), now, 0);

        assertEquals(0, adapter.repairStaleStartup(STORES, null, target.instanceId(), target.startupId(), TTL));
        assertEquals(0L, sessionRow(open).getLong("closed_at"));
        assertEquals(0L, startupRow(target).getLong("closed_at"));
    }

    @Test
    void aTargetClaimAfterTheSweepSnapshotPreventsStaleRepair() throws Exception {
        OwnershipToken sweeper = claim(UUID.randomUUID());
        OwnershipToken target = token(UUID.randomUUID());
        adapter.publishStartup(STORES, null, startup(target, 100, 200, 1));
        UUID open = session(target.startupId(), 220, 0);
        CountDownLatch snapshotTaken = new CountDownLatch(1);
        CountDownLatch releaseSweep = new CountDownLatch(1);
        beforeTargetLock.set(() -> {
            snapshotTaken.countDown();
            await(releaseSweep);
        });
        var result = CompletableFuture.supplyAsync(() -> {
            try {
                return adapter.repairStaleStartup(STORES, sweeper, target.instanceId(), target.startupId(), 1);
            } catch (BackendException e) {
                throw new AssertionError(e);
            }
        });
        try {
            await(snapshotTaken);
            assertTrue(adapter.claimRecovery(STORES.ownership(), target, null, TTL, METADATA).claimed());
        } finally {
            releaseSweep.countDown();
        }
        assertEquals(0, result.get(10, TimeUnit.SECONDS));
        assertEquals(0L, startupRow(target).getLong("closed_at"));
        assertEquals(0L, sessionRow(open).getLong("closed_at"));
    }

    @Test
    void aSessionWriteFailureRollsBackTheStartupClose() throws Exception {
        OwnershipToken target = token(UUID.randomUUID());
        adapter.publishStartup(STORES, null, startup(target, 100, 200, 1));
        UUID open = session(target.startupId(), 220, 0);
        db.runCommand(new Document("collMod", STORES.sessions().name())
                .append("validator", new Document("closed_at", 0L)));
        assertThrows(BackendException.class,
                () -> adapter.repairStaleStartup(STORES, null, target.instanceId(), target.startupId(), 1));
        assertEquals(0L, startupRow(target).getLong("closed_at"));
        assertEquals(0L, sessionRow(open).getLong("closed_at"));
    }

    @Test
    void sessionPublicationChecksTheGrantStartupAndStoredRowInOneTransaction() throws Exception {
        OwnershipToken owner = claim(UUID.randomUUID());
        OwnershipToken prior = token(owner.instanceId());
        OwnershipToken foreign = token(UUID.randomUUID());
        UUID player = UUID.randomUUID();
        UUID id = UUID.randomUUID();
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, record(id, player, owner, 15, 0)));
        adapter.publishStartup(STORES, owner, startup(owner, 10, 30, 1));
        adapter.publishStartup(STORES, null, startup(prior, 5, 9, 1));
        adapter.publishStartup(STORES, null, startup(foreign, 5, 9, 1));
        OwnershipToken stale = new OwnershipToken(owner.instanceId(), owner.startupId(), UUID.randomUUID());
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, stale, record(id, player, owner, 15, 0)));

        adapter.publishSession(STORES, owner, record(id, player, owner, 15, 0));
        adapter.publishSession(STORES, owner, record(id, player, owner, 18, 0));
        assertEquals(18L, sessionRow(id).getLong("last_activity"));
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, record(id, UUID.randomUUID(), owner, 19, 0)));

        UUID retained = UUID.randomUUID();
        UUID other = UUID.randomUUID();
        adapter.publishSession(STORES, null, record(retained, player, prior, 15, 0));
        adapter.publishSession(STORES, null, record(other, player, foreign, 15, 0));
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, record(retained, player, owner, 16, 0)));
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, record(other, player, owner, 16, 17)));
        adapter.publishSession(STORES, owner, record(retained, player, owner, 16, 17));
        assertEquals(prior.startupId(), BsonBinaries.toUuid(sessionRow(retained).get("startup_id")));
        assertEquals(17L, sessionRow(retained).getLong("closed_at"));
        adapter.publishSession(STORES, null, record(retained, player, prior, 30, 0));
        assertEquals(16L, sessionRow(retained).getLong("last_activity"));

        assertEquals(1, adapter.closeStartup(STORES, owner, owner.instanceId(), owner.startupId()));
        long closedAt = sessionRow(id).getLong("closed_at");
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, owner, record(id, player, owner, 25, 0)));
        UUID late = UUID.randomUUID();
        assertThrows(BackendException.class, () -> adapter.publishSession(STORES, null, record(late, player, owner, 25, 0)));
        assertNull(collection(STORES.sessions()).find(id(late)).first());
        assertEquals(closedAt, sessionRow(id).getLong("closed_at"));
    }

    @Test
    void heartbeatOnlyReassignmentRequiresAnOpenDestination() throws Exception {
        OwnershipToken previous = token(UUID.randomUUID());
        OwnershipToken next = token(previous.instanceId());
        UUID moving = session(previous.startupId(), 20, 0);
        var move = new EntityOps.ReassignSessionOp(Categories.SESSION, moving, Set.of(previous.startupId()), next.startupId());
        assertThrows(BackendException.class, () -> adapter.reassignSession(STORES, null, move));
        adapter.publishStartup(STORES, null, startup(next, 10, 30, 1));
        assertEquals(1, adapter.reassignSession(STORES, null, move));
        UUID late = session(previous.startupId(), 21, 0);
        assertEquals(1, adapter.closeStartup(STORES, null, next.instanceId(), next.startupId()));
        assertThrows(BackendException.class, () -> adapter.reassignSession(STORES, null,
                new EntityOps.ReassignSessionOp(Categories.SESSION, late, Set.of(previous.startupId()), next.startupId())));
        assertEquals(previous.startupId(), BsonBinaries.toUuid(sessionRow(late).get("startup_id")));
    }

    private static SessionRecord record(UUID id, UUID player, OwnershipToken token, long activity, long closed) {
        return new SessionRecord(id, player, null, 10L, activity, closed, null, null, -1, null, null, token.startupId(), List.of());
    }

    private OwnershipToken claim(UUID instance) throws Exception {
        var owner = token(instance);
        assertTrue(adapter.claimRecovery(STORES.ownership(), owner, null, TTL, METADATA).claimed());
        return owner;
    }

    private static OwnershipToken token(UUID instance) {
        return new OwnershipToken(instance, UUID.randomUUID(), UUID.randomUUID());
    }

    private static ServerStartupRecord startup(OwnershipToken token, long started, long heartbeat, int manifest) {
        return new ServerStartupRecord(token.startupId(), token.instanceId(), "test", null, null, null,
                started, heartbeat, 0L, null, new byte[]{(byte) manifest});
    }

    private UUID session(UUID startup, long lastActivity, long closed) {
        UUID id = UUID.randomUUID();
        collection(STORES.sessions()).insertOne(new Document("_id", BsonBinaries.uuidBinary(id))
                .append("startup_id", BsonBinaries.uuidBinary(startup)).append("last_activity", lastActivity)
                .append("closed_at", closed));
        return id;
    }

    private Document startupRow(OwnershipToken owner) {
        return Objects.requireNonNull(collection(STORES.startups()).find(id(owner.startupId())).first());
    }

    private Document sessionRow(UUID sessionId) {
        return Objects.requireNonNull(collection(STORES.sessions()).find(id(sessionId)).first());
    }

    private MongoCollection<Document> collection(StoreId id) {
        return db.getCollection(id.name());
    }

    private static org.bson.conversions.Bson id(UUID uuid) {
        return Filters.eq("_id", BsonBinaries.uuidBinary(uuid));
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }
}

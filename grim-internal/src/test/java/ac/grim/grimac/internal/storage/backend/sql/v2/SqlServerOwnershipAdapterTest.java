package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.ServerOwnershipSnapshot;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.MysqlDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.PostgresDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqliteDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import com.mysql.cj.jdbc.MysqlDataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.postgresql.ds.PGSimpleDataSource;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteDataSource;

import javax.sql.DataSource;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SqlServerOwnershipAdapterTest {
    private static final ServerOwnershipMetadata METADATA = new ServerOwnershipMetadata("meg", null, null, null);
    private static final Logger LOGGER = Logger.getAnonymousLogger();

    @TempDir Path directory;

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void onlyOneRecoveryCanReplaceAnInactivePredecessor(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken predecessor = token(UUID.randomUUID());
            db.claim(predecessor, 30_000);
            assertTrue(db.first.closeOwnership(db.stores.ownership(), predecessor.instanceId(),
                    predecessor.startupId(), predecessor.fence(), "stop"));
            OwnershipClaimResult winner = competingRecoveries(db, predecessor.instanceId(), predecessor);
            assertEquals(predecessor, OwnershipToken.of(winner.previousOwner()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void absentRecoveryIsSerializedAndNeverReplacesAForeignExpiredGrant(String engine) throws Exception {
        try (Database db = database(engine)) {
            UUID instanceId = UUID.randomUUID();
            assertNull(competingRecoveries(db, instanceId, null).previousOwner());

            OwnershipToken expired = token(UUID.randomUUID());
            db.claim(expired, -1);
            OwnershipClaimResult denied = db.second.claimRecovery(db.stores.ownership(), token(expired.instanceId()),
                    null, 30_000, METADATA);
            assertFalse(denied.claimed());
            assertEquals(expired, OwnershipToken.of(denied.currentOwner()));
            assertEquals(denied.currentOwner(), db.first.readOwnership(db.stores.ownership(), expired.instanceId()).orElseThrow());

            OwnershipToken missing = token(UUID.randomUUID());
            denied = db.second.claimRecovery(db.stores.ownership(), token(missing.instanceId()), missing, 30_000, METADATA);
            assertFalse(denied.claimed());
            assertNull(denied.currentOwner());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void publicationRequiresTheRightOpenStartupAndPreservesManifestOrder(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken owner = token(UUID.randomUUID());
            db.claim(owner, 30_000);
            OwnershipToken badFence = new OwnershipToken(owner.instanceId(), owner.startupId(), UUID.randomUUID());
            ServerStartupRecord newer = startup(owner, 10, 40, (byte) 1);
            assertThrows(BackendException.class, () -> db.first.publishStartup(db.stores, badFence, newer));
            assertTrue(db.startup(owner.startupId()).isEmpty());

            db.first.publishStartup(db.stores, owner, newer);
            db.second.publishStartup(db.stores, owner, startup(owner, 10, 39, (byte) 2));
            assertEquals(40, db.startup(owner.startupId()).orElseThrow().lastHeartbeatEpochMs());
            assertArrayEquals(new byte[]{1}, db.startup(owner.startupId()).orElseThrow().verboseManifest());
            db.second.publishStartup(db.stores, owner, startup(owner, 10, 40, (byte) 3));
            assertArrayEquals(new byte[]{3}, db.startup(owner.startupId()).orElseThrow().verboseManifest());

            OwnershipToken wrongInstance = new OwnershipToken(UUID.randomUUID(), owner.startupId(), owner.fence());
            assertThrows(BackendException.class, () -> db.second.publishStartup(db.stores, null, startup(wrongInstance, 10, 41, (byte) 4)));
            assertEquals(0L, db.first.closeStartup(db.stores, owner, owner.instanceId(), owner.startupId()));
            assertThrows(BackendException.class, () -> db.second.publishStartup(db.stores, owner, newer));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void handoffReopensOnlyAnEligiblePriorSession(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken owner = token(UUID.randomUUID());
            db.claim(owner, 30_000);
            db.first.publishStartup(db.stores, owner, startup(owner, 10, 20, (byte) 1));
            UUID previous = UUID.randomUUID();
            UUID recovering = db.insertSession(previous, 19L);
            UUID alreadyAtTarget = db.insertSession(owner.startupId(), 26L);
            var reassign = new EntityOps.ReassignSessionOp(Categories.SESSION, recovering, Set.of(previous), owner.startupId());

            OwnershipToken badFence = new OwnershipToken(owner.instanceId(), owner.startupId(), UUID.randomUUID());
            assertThrows(BackendException.class, () -> db.second.reassignSession(db.stores, badFence, reassign));
            assertEquals(19L, db.session(recovering).closedAtEpochMs());
            assertEquals(0L, db.second.reassignSession(db.stores, owner,
                    new EntityOps.ReassignSessionOp(Categories.SESSION, recovering, Set.of(UUID.randomUUID()), owner.startupId())));
            assertEquals(1L, db.second.reassignSession(db.stores, owner, reassign));
            SessionRecord moved = db.session(recovering);
            assertEquals(owner.startupId(), moved.startupId());
            assertEquals(0L, moved.closedAtEpochMs());
            assertEquals(15L, moved.lastActivityEpochMs());

            assertEquals(0L, db.second.reassignSession(db.stores, owner,
                    new EntityOps.ReassignSessionOp(Categories.SESSION, alreadyAtTarget, Set.of(owner.startupId()), owner.startupId())));
            assertEquals(26L, db.session(alreadyAtTarget).closedAtEpochMs());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void sessionPublicationRequiresTheExactGrantAndAnOpenStartup(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken owner = token(UUID.randomUUID());
            db.claim(owner, 30_000);
            db.first.publishStartup(db.stores, owner, startup(owner, 10, 20, (byte) 1));
            UUID id = UUID.randomUUID();
            UUID player = UUID.randomUUID();

            OwnershipToken badFence = new OwnershipToken(owner.instanceId(), owner.startupId(), UUID.randomUUID());
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, badFence, session(id, player, owner.startupId(), 15, 0)));
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, owner, session(id, player, null, 15, 0)));
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, owner, session(id, player, UUID.randomUUID(), 15, 0)));
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, null, session(id, player, UUID.randomUUID(), 15, 0)));
            SessionRecord otherInstance = new SessionRecord(id, player, null, 10L, 15L, 0L, null, null, -1, null,
                    UUID.randomUUID(), owner.startupId(), List.of());
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, owner, otherInstance));
            assertTrue(db.findSession(id).isEmpty());

            db.second.publishSession(db.stores, owner, session(id, player, owner.startupId(), 15, 0));
            db.second.publishSession(db.stores, owner, session(id, player, owner.startupId(), 18, 0));
            assertEquals(18L, db.session(id).lastActivityEpochMs());
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, owner,
                    session(id, UUID.randomUUID(), owner.startupId(), 19, 0)));

            assertEquals(1L, db.first.closeStartup(db.stores, owner, owner.instanceId(), owner.startupId()));
            long closedAt = db.session(id).closedAtEpochMs();
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, owner, session(id, player, owner.startupId(), 25, 0)));
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, owner,
                    session(UUID.randomUUID(), player, owner.startupId(), 25, 0)));
            assertEquals(closedAt, db.session(id).closedAtEpochMs());
            assertEquals(18L, db.session(id).lastActivityEpochMs());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void aConcurrentFirstInsertIsRecheckedInsteadOfMerged(String engine) throws Exception {
        try (Database db = database(engine); Connection racer = db.firstSource.getConnection()) {
            OwnershipToken first = token(UUID.randomUUID());
            OwnershipToken second = token(UUID.randomUUID());
            db.first.publishStartup(db.stores, null, startup(first, 10, 20, (byte) 1));
            db.first.publishStartup(db.stores, null, startup(second, 10, 20, (byte) 1));
            UUID id = UUID.randomUUID();
            UUID player = UUID.randomUUID();
            racer.setAutoCommit(false);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                // An uncommitted terminal insert is invisible to the publisher's read of the absent row.
                SqlEntityAdapter.upsertOn(racer, db.dialect, db.stores.sessions().name(), V2BuiltinKinds.sessions(),
                        session(id, player, first.startupId(), 15, 17));
                Future<?> late = executor.submit(() -> {
                    db.second.publishSession(db.stores, null, session(id, UUID.randomUUID(), second.startupId(), 16, 0));
                    return null;
                });
                assertThrows(TimeoutException.class, () -> late.get(300, TimeUnit.MILLISECONDS));
                racer.commit();
                ExecutionException refused = assertThrows(ExecutionException.class, () -> late.get(10, TimeUnit.SECONDS));
                assertInstanceOf(BackendException.class, refused.getCause());
            } finally {
                executor.shutdownNow();
                racer.rollback();
            }
            SessionRecord stored = db.session(id);
            assertEquals(player, stored.playerUuid());
            assertEquals(first.startupId(), stored.startupId());
            assertEquals(17L, stored.closedAtEpochMs());
            assertEquals(15L, stored.lastActivityEpochMs());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void bufferedFirstInsertCannotReopenAStartupAPeerRepaired(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken sweeper = token(UUID.randomUUID());
            OwnershipToken target = token(UUID.randomUUID());
            db.claim(sweeper, 30_000);
            db.first.publishStartup(db.stores, null, startup(target, 10, 20, (byte) 1));
            assertEquals(0L, db.second.repairStaleStartup(db.stores, sweeper, target.instanceId(), target.startupId(), 60_000));
            assertTrue(db.startup(target.startupId()).orElseThrow().isClosed());

            UUID late = UUID.randomUUID();
            assertThrows(BackendException.class, () -> db.first.publishSession(db.stores, null,
                    session(late, UUID.randomUUID(), target.startupId(), 25, 0)));
            assertTrue(db.findSession(late).isEmpty());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void terminalWriteCompletesOnlyARetainedSessionOfTheSameInstance(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken current = token(UUID.randomUUID());
            OwnershipToken prior = token(current.instanceId());
            OwnershipToken foreign = token(UUID.randomUUID());
            db.claim(current, 30_000);
            db.first.publishStartup(db.stores, null, startup(prior, 5, 9, (byte) 1));
            db.first.publishStartup(db.stores, null, startup(foreign, 5, 9, (byte) 1));
            db.first.publishStartup(db.stores, current, startup(current, 10, 20, (byte) 1));
            UUID player = UUID.randomUUID();
            UUID retained = UUID.randomUUID();
            UUID other = UUID.randomUUID();
            db.second.publishSession(db.stores, null, session(retained, player, prior.startupId(), 15, 0));
            db.second.publishSession(db.stores, null, session(other, player, foreign.startupId(), 15, 0));

            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, current,
                    session(retained, player, current.startupId(), 16, 0)));
            assertThrows(BackendException.class, () -> db.second.publishSession(db.stores, current,
                    session(other, player, current.startupId(), 16, 17)));
            assertEquals(0L, db.session(other).closedAtEpochMs());

            db.second.publishSession(db.stores, current, session(retained, player, current.startupId(), 16, 17));
            SessionRecord closed = db.session(retained);
            assertEquals(prior.startupId(), closed.startupId());
            assertEquals(17L, closed.closedAtEpochMs());
            assertEquals(16L, closed.lastActivityEpochMs());

            // A stored terminal row stays as it is, including later writes from its own startup.
            db.second.publishSession(db.stores, current, session(retained, player, current.startupId(), 30, 31));
            db.second.publishSession(db.stores, null, session(retained, player, prior.startupId(), 32, 0));
            assertEquals(17L, db.session(retained).closedAtEpochMs());
            assertEquals(16L, db.session(retained).lastActivityEpochMs());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void heartbeatOnlyReassignmentRequiresAnOpenDestination(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken previous = token(UUID.randomUUID());
            OwnershipToken next = token(previous.instanceId());
            db.first.publishStartup(db.stores, null, startup(previous, 5, 9, (byte) 1));
            UUID recovering = db.insertSession(previous.startupId(), 0L);
            var move = new EntityOps.ReassignSessionOp(Categories.SESSION, recovering, Set.of(previous.startupId()), next.startupId());
            assertThrows(BackendException.class, () -> db.second.reassignSession(db.stores, null, move));
            assertEquals(previous.startupId(), db.session(recovering).startupId());

            db.first.publishStartup(db.stores, null, startup(next, 10, 20, (byte) 1));
            assertEquals(1L, db.second.reassignSession(db.stores, null, move));
            assertEquals(next.startupId(), db.session(recovering).startupId());

            UUID late = db.insertSession(previous.startupId(), 0L);
            assertEquals(1L, db.first.closeStartup(db.stores, null, next.instanceId(), next.startupId()));
            assertThrows(BackendException.class, () -> db.second.reassignSession(db.stores, null,
                    new EntityOps.ReassignSessionOp(Categories.SESSION, late, Set.of(previous.startupId()), next.startupId())));
            assertEquals(previous.startupId(), db.session(late).startupId());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void gracefulCloseUsesDatabaseTimeAndOnlyClosesItsOpenSessions(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken owner = token(UUID.randomUUID());
            db.claim(owner, 30_000);
            db.first.publishStartup(db.stores, owner, startup(owner, 10, 20, (byte) 1));
            UUID open = db.insertSession(owner.startupId(), 0L);
            UUID closed = db.insertSession(owner.startupId(), 24L);
            UUID other = db.insertSession(UUID.randomUUID(), 0L);
            long before = db.second.dbNowEpochMs();
            assertEquals(1L, db.first.closeStartup(db.stores, owner, owner.instanceId(), owner.startupId()));
            long after = db.second.dbNowEpochMs();

            ServerStartupRecord startup = db.startup(owner.startupId()).orElseThrow();
            assertTrue(startup.closedAtEpochMs() >= before && startup.closedAtEpochMs() <= after);
            assertEquals("graceful", startup.closeReason());
            assertEquals(startup.closedAtEpochMs(), db.session(open).closedAtEpochMs());
            assertEquals(24L, db.session(closed).closedAtEpochMs());
            assertEquals(0L, db.session(other).closedAtEpochMs());
            assertEquals(0L, db.second.closeStartup(db.stores, owner, owner.instanceId(), owner.startupId()));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void staleRepairUsesOnlyTheMatchingLeaseAndSupportsHeartbeatOnlyMode(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken sweeper = token(UUID.randomUUID());
            OwnershipToken live = token(UUID.randomUUID());
            OwnershipToken prior = token(live.instanceId());
            OwnershipToken heartbeatOnly = token(UUID.randomUUID());
            db.claim(sweeper, 30_000);
            OwnershipClaimResult active = db.claim(live, 30_000);
            db.first.publishStartup(db.stores, sweeper, startup(sweeper, 10, 20, (byte) 1));
            db.first.publishStartup(db.stores, live, startup(live, 10, 20, (byte) 1));
            db.first.publishStartup(db.stores, null, startup(prior, 30, 20, (byte) 1));
            db.first.publishStartup(db.stores, null, startup(heartbeatOnly, 10, db.first.dbNowEpochMs(), (byte) 1));
            UUID liveSession = db.insertSession(live.startupId(), 0L);
            UUID priorSession = db.insertSession(prior.startupId(), 0L);

            assertEquals(0L, db.second.repairStaleStartup(db.stores, sweeper, sweeper.instanceId(), sweeper.startupId(), 1L));
            assertEquals(0L, db.second.repairStaleStartup(db.stores, sweeper, live.instanceId(), live.startupId(), 1L));
            assertEquals(0L, db.second.repairStaleStartup(db.stores, null, heartbeatOnly.instanceId(), heartbeatOnly.startupId(), 60_000));
            assertThrows(BackendException.class, () -> db.second.repairStaleStartup(db.stores, sweeper,
                    sweeper.instanceId(), live.startupId(), 1L));

            assertEquals(1L, db.second.repairStaleStartup(db.stores, sweeper, prior.instanceId(), prior.startupId(), 60_000));
            assertEquals(30L, db.startup(prior.startupId()).orElseThrow().closedAtEpochMs());
            assertEquals(15L, db.session(priorSession).closedAtEpochMs());
            assertEquals(0L, db.session(liveSession).closedAtEpochMs());

            assertTrue(db.first.closeOwnership(db.stores.ownership(), live.instanceId(), live.startupId(), live.fence(), "lost"));
            assertEquals(1L, db.second.repairStaleStartup(db.stores, sweeper, live.instanceId(), live.startupId(), 60_000));
            assertEquals(active.dbNowEpochMs(), db.startup(live.startupId()).orElseThrow().closedAtEpochMs());
            assertEquals("stale", db.startup(live.startupId()).orElseThrow().closeReason());
            assertEquals(15L, db.session(liveSession).closedAtEpochMs());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void staleRepairWaitsForAConcurrentTargetClaim(String engine) throws Exception {
        try (Database db = database(engine); Connection claim = db.firstSource.getConnection()) {
            OwnershipToken sweeper = token(UUID.randomUUID());
            OwnershipToken target = token(UUID.randomUUID());
            db.claim(sweeper, 30_000);
            db.first.publishStartup(db.stores, null, startup(target, 10, 20, (byte) 1));
            UUID session = db.insertSession(target.startupId(), 0L);
            long now = db.first.dbNowEpochMs();
            claim.setAutoCommit(false);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                // Hold the exact INSERT of a new owner uncommitted while repair tries to test absence.
                try (PreparedStatement ps = claim.prepareStatement("INSERT INTO " + db.table(db.stores.ownership())
                        + " (persistent_id, owner_startup_id, fence, lease_expires_at_ms, last_renewed_at_ms, closed_at_ms) "
                        + "VALUES (?, ?, ?, ?, ?, 0)")) {
                    ps.setString(1, target.instanceId().toString());
                    ps.setString(2, target.startupId().toString());
                    ps.setString(3, target.fence().toString());
                    ps.setLong(4, now + 30_000);
                    ps.setLong(5, now);
                    ps.executeUpdate();
                }
                CountDownLatch entered = new CountDownLatch(1);
                Future<Long> repair = executor.submit(() -> {
                    entered.countDown();
                    return db.second.repairStaleStartup(db.stores, sweeper, target.instanceId(), target.startupId(), 0L);
                });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> repair.get(150, TimeUnit.MILLISECONDS));
                claim.commit();
                assertEquals(0L, repair.get(10, TimeUnit.SECONDS));
            } finally {
                executor.shutdownNow();
                claim.rollback();
            }
            assertEquals(0L, db.session(session).closedAtEpochMs());
            assertEquals(0L, db.startup(target.startupId()).orElseThrow().closedAtEpochMs());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void callbackRollsBackDmlIfLeaseExpiresDuringWork(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken token = token(UUID.randomUUID());
            long expiry = db.claim(token, 1_500).leaseExpiresAtEpochMs();
            db.first.publishStartup(db.stores, token, startup(token, 10, 20, (byte) 1));
            AtomicBoolean changed = new AtomicBoolean();
            assertThrows(BackendException.class, () -> db.first.withOwnedTransaction(db.stores.ownership(), token, c -> {
                try (Statement stmt = c.createStatement()) {
                    assertEquals(1, stmt.executeUpdate("UPDATE " + db.table(db.stores.startups()) + " SET server_name = 'changed'"));
                }
                changed.set(true);
                awaitDatabaseTime(db.second, expiry);
                return null;
            }, db.stores.startups()));
            assertTrue(changed.get());
            assertEquals("meg", db.startup(token.startupId()).orElseThrow().serverName());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void waitingRenewalCannotReviveAnExpiredGrant(String engine) throws Exception {
        try (Database db = database(engine); Connection blocker = db.firstSource.getConnection()) {
            OwnershipToken owner = token(UUID.randomUUID());
            long expiry = db.claim(owner, 1_500).leaseExpiresAtEpochMs();
            blocker.setAutoCommit(false);
            ExecutorService executor = Executors.newSingleThreadExecutor();
            try {
                String sql = "UPDATE " + db.table(db.stores.ownership()) + " SET fence = fence WHERE persistent_id = ?";
                try (PreparedStatement ps = blocker.prepareStatement(sql)) {
                    ps.setString(1, owner.instanceId().toString());
                    ps.executeUpdate();
                }
                CountDownLatch entered = new CountDownLatch(1);
                var renewal = executor.submit(() -> {
                    entered.countDown();
                    return db.second.renewOwnership(db.stores.ownership(), owner.instanceId(), owner.startupId(), owner.fence(), 30_000);
                });
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                assertThrows(TimeoutException.class, () -> renewal.get(150, TimeUnit.MILLISECONDS));
                awaitDatabaseTime(db.first, expiry);
                blocker.commit();
                assertFalse(renewal.get(10, TimeUnit.SECONDS).renewed());
            } finally {
                executor.shutdownNow();
                blocker.rollback();
            }
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sqlite", "postgres", "mysql"})
    void rollbackLeavesBothRecordsOpenWhenClosingTheStartupFails(String engine) throws Exception {
        try (Database db = database(engine)) {
            OwnershipToken token = token(UUID.randomUUID());
            db.first.publishStartup(db.stores, null, startup(token, 10, 20, (byte) 1));
            UUID session = db.insertSession(token.startupId(), 0L);
            String startups = db.table(db.stores.startups());
            if ("sqlite".equals(engine)) {
                db.execute("CREATE TRIGGER forbid_close BEFORE UPDATE OF closed_at ON " + startups
                        + " BEGIN SELECT RAISE(ABORT, 'test close failure'); END");
            } else {
                db.execute("ALTER TABLE " + startups + " ADD CHECK (closed_at = 0)");
            }
            assertThrows(BackendException.class, () -> db.second.repairStaleStartup(db.stores, null, token.instanceId(), token.startupId(), 0));
            assertEquals(0L, db.session(session).closedAtEpochMs());
            assertEquals(0L, db.startup(token.startupId()).orElseThrow().closedAtEpochMs());
        }
    }

    @Test
    void rejectsNonTransactionalMysqlMutationStoreBeforeInvokingCallback() throws Exception {
        try (Database db = database("mysql")) {
            OwnershipToken token = token(UUID.randomUUID());
            db.claim(token, 30_000);
            StoreId history = StoreId.grim(db.stores.sessions().name() + "_history");
            db.execute("CREATE TABLE " + db.table(history) + " (id INT PRIMARY KEY) ENGINE=MyISAM");
            try {
                AtomicBoolean invoked = new AtomicBoolean();
                assertThrows(BackendException.class, () -> db.first.withOwnedTransaction(db.stores.ownership(), token, c -> {
                    invoked.set(true);
                    return null;
                }, db.stores.startups(), history));
                assertFalse(invoked.get());
            } finally {
                db.execute("DROP TABLE " + db.table(history));
            }
        }
    }

    @Test
    void failedRollbackNeverEnablesAutoCommitOnTheUnfinishedTransaction() throws Exception {
        DataSource ds = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        Statement statement = mock(Statement.class);
        when(ds.getConnection()).thenReturn(connection);
        when(connection.getAutoCommit()).thenReturn(true, false);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeUpdate(anyString())).thenThrow(new SQLException("test mutation failure"));
        doThrow(new SQLException("test rollback failure")).when(connection).rollback();

        SqlServerOwnershipAdapter owner = new SqlServerOwnershipAdapter(ds, new SqliteDialect(), LOGGER);
        BackendException failure = assertThrows(BackendException.class, () -> owner.withOwnedTransaction(
                StoreId.grim("ownership"), token(UUID.randomUUID()), c -> null));
        assertEquals("test rollback failure", failure.getCause().getSuppressed()[0].getMessage());
        verify(connection, never()).setAutoCommit(true);
        verify(connection, never()).commit();
    }

    private static OwnershipClaimResult competingRecoveries(Database db, UUID instanceId, OwnershipToken expected) throws Exception {
        OwnershipToken candidateA = token(instanceId);
        OwnershipToken candidateB = token(instanceId);
        try (RecoveryRace race = new RecoveryRace()) {
            Future<OwnershipClaimResult> a = race.submit(() -> db.first.claimRecovery(db.stores.ownership(), candidateA, expected, 30_000, METADATA));
            Future<OwnershipClaimResult> b = race.submit(() -> db.second.claimRecovery(db.stores.ownership(), candidateB, expected, 30_000, METADATA));
            race.start();
            OwnershipClaimResult resultA = a.get(10, TimeUnit.SECONDS);
            OwnershipClaimResult resultB = b.get(10, TimeUnit.SECONDS);
            assertTrue(resultA.claimed() ^ resultB.claimed());
            OwnershipClaimResult winner = resultA.claimed() ? resultA : resultB;
            OwnershipClaimResult loser = resultA.claimed() ? resultB : resultA;
            ServerOwnershipSnapshot current = db.first.readOwnership(db.stores.ownership(), instanceId).orElseThrow();
            assertEquals(winner.fence(), current.fence());
            assertEquals(current, loser.currentOwner());
            assertNull(loser.previousOwner());
            return winner;
        }
    }

    private Database database(String engine) throws Exception {
        return switch (engine) {
            case "sqlite" -> new Database(sqlite(), sqlite(), new SqliteDialect());
            case "postgres" -> new Database(postgres(), postgres(), new PostgresDialect());
            case "mysql" -> new Database(mysql(), mysql(), new MysqlDialect());
            default -> throw new IllegalArgumentException(engine);
        };
    }

    private DataSource sqlite() {
        SQLiteConfig config = new SQLiteConfig();
        config.setBusyTimeout(5_000);
        config.setJournalMode(SQLiteConfig.JournalMode.WAL);
        SQLiteDataSource dataSource = new SQLiteDataSource(config);
        dataSource.setUrl("jdbc:sqlite:" + directory.resolve("ownership.db"));
        return dataSource;
    }

    private static DataSource postgres() {
        int port = testPort("postgres");
        PGSimpleDataSource ds = new PGSimpleDataSource();
        ds.setServerNames(new String[]{"127.0.0.1"});
        ds.setPortNumbers(new int[]{port});
        ds.setDatabaseName("grim");
        ds.setUser("postgres");
        ds.setPassword("grim-test-postgres");
        ds.setConnectTimeout(3);
        ds.setSocketTimeout(10);
        return ds;
    }

    private static DataSource mysql() {
        int port = testPort("mysql");
        MysqlDataSource ds = new MysqlDataSource();
        ds.setURL("jdbc:mysql://127.0.0.1:" + port + "/grim"
                + "?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=3000&socketTimeout=10000");
        ds.setUser("grim");
        ds.setPassword("grim-test-mysql");
        return ds;
    }

    private static int testPort(String engine) {
        Integer port = Integer.getInteger("grim.test.ownership." + engine + ".port");
        assumeTrue(port != null, "A private loopback SQL test server must be explicitly enabled for " + engine);
        return port;
    }

    private static OwnershipToken token(UUID instanceId) {
        return new OwnershipToken(instanceId, UUID.randomUUID(), UUID.randomUUID());
    }

    private static void awaitDatabaseTime(SqlServerOwnershipAdapter adapter, long expected) throws BackendException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(7);
        while (adapter.dbNowEpochMs() < expected) {
            assertTrue(System.nanoTime() < deadline, "backend clock did not advance to lease expiry");
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(10));
        }
    }

    private static SessionRecord session(UUID id, UUID player, UUID startupId, long lastActivity, long closedAt) {
        return new SessionRecord(id, player, null, 10L, lastActivity, closedAt, null, null, -1, null, null, startupId, List.of());
    }

    private static ServerStartupRecord startup(OwnershipToken owner, long started, long heartbeat, byte manifest) {
        return new ServerStartupRecord(owner.startupId(), owner.instanceId(), "meg", null, null, null,
                started, heartbeat, 0L, null, new byte[]{manifest});
    }

    private static final class Database implements AutoCloseable {
        private final DataSource firstSource;
        private final SqlDialect dialect;
        private final StartupStores stores;
        private final SqlServerOwnershipAdapter first;
        private final SqlServerOwnershipAdapter second;
        private final SqlEntityAdapter entities;

        Database(DataSource firstSource, DataSource secondSource, SqlDialect dialect) throws BackendException {
            this.firstSource = firstSource;
            this.dialect = dialect;
            String suffix = UUID.randomUUID().toString().substring(0, 8);
            this.stores = new StartupStores(StoreId.grim("atomic_owners_" + suffix), StoreId.grim("atomic_startups_" + suffix),
                    StoreId.grim("atomic_sessions_" + suffix));
            this.first = new SqlServerOwnershipAdapter(firstSource, dialect, LOGGER);
            this.second = new SqlServerOwnershipAdapter(secondSource, dialect, LOGGER);
            this.entities = new SqlEntityAdapter(secondSource, dialect, LOGGER);
            first.ensureStore(stores.ownership());
            entities.ensureStore(stores.startups(), V2BuiltinKinds.serverStartups());
            entities.ensureStore(stores.sessions(), V2BuiltinKinds.sessions());
        }

        OwnershipClaimResult claim(OwnershipToken owner, long ttlMs) throws BackendException {
            OwnershipClaimResult result = first.claimOwnership(stores.ownership(), owner.instanceId(), owner.startupId(),
                    owner.fence(), ttlMs, METADATA);
            assertTrue(result.claimed());
            return result;
        }

        Optional<ServerStartupRecord> startup(UUID startupId) throws BackendException {
            return entities.execute(stores.startups(), V2BuiltinKinds.serverStartups(),
                    new EntityOps.GetByIdOp<UUID, ServerStartupRecord>(Categories.SERVER_STARTUP, startupId));
        }

        UUID insertSession(UUID startupId, long closedAt) throws BackendException {
            UUID id = UUID.randomUUID();
            SessionRecord session = new SessionRecord(id, UUID.randomUUID(), null, 10L, 15L, closedAt,
                    null, null, -1, null, null, startupId, List.of());
            entities.execute(stores.sessions(), V2BuiltinKinds.sessions(), new EntityOps.UpsertOp<>(Categories.SESSION, session));
            return id;
        }

        SessionRecord session(UUID id) throws BackendException {
            return findSession(id).orElseThrow();
        }

        Optional<SessionRecord> findSession(UUID id) throws BackendException {
            return entities.execute(stores.sessions(), V2BuiltinKinds.sessions(),
                    new EntityOps.GetByIdOp<UUID, SessionRecord>(Categories.SESSION, id));
        }

        void execute(String sql) throws SQLException {
            try (Connection c = firstSource.getConnection(); Statement stmt = c.createStatement()) {
                stmt.execute(sql);
            }
        }

        String table(StoreId id) {
            return dialect.quoteIdentifier(id.name());
        }

        @Override
        public void close() throws SQLException {
            for (StoreId id : List.of(stores.sessions(), stores.startups(), stores.ownership())) {
                execute("DROP TABLE IF EXISTS " + table(id));
            }
        }
    }

    private static final class RecoveryRace implements AutoCloseable {
        private final ExecutorService executor = Executors.newFixedThreadPool(2);
        private final CountDownLatch ready = new CountDownLatch(2);
        private final CountDownLatch go = new CountDownLatch(1);

        <T> Future<T> submit(Callable<T> work) {
            return executor.submit(() -> {
                ready.countDown();
                assertTrue(go.await(5, TimeUnit.SECONDS));
                return work.call();
            });
        }

        void start() throws InterruptedException {
            assertTrue(ready.await(5, TimeUnit.SECONDS));
            go.countDown();
        }

        @Override
        public void close() {
            executor.shutdownNow();
        }
    }
}

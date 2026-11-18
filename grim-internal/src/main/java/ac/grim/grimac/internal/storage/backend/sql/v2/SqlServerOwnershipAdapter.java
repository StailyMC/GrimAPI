package ac.grim.grimac.internal.storage.backend.sql.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.codec.EncodeShape;
import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipRenewResult;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.ServerOwnershipSnapshot;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.kind.Entity;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.backend.sql.v2.dialect.SqlDialect;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * SQL implementation of the authoritative server-ownership lease primitive.
 * Guarded mutations share a JDBC transaction with the lease check. All stores
 * must route to this adapter's database using the managed SQL schemas.
 */
@ApiStatus.Internal
public final class SqlServerOwnershipAdapter implements ServerOwnershipAdapter {

    private static final EncodeShape STARTUP = V2BuiltinKinds.serverStartups().codec().shape();
    private static final EncodeShape.FieldDef STARTUP_ID = startupField("startup_id");
    private static final EncodeShape.FieldDef INSTANCE_ID = startupField("instance_id");
    private static final Entity<?, ?, ?> SESSIONS = V2BuiltinKinds.sessions();
    private static final EncodeShape SESSION = SESSIONS.codec().shape();
    private static final EncodeShape.FieldDef SESSION_ID = sessionField(SESSION.idField());
    private static final EncodeShape.FieldDef SESSION_PLAYER = sessionField("player_uuid");
    private static final EncodeShape.FieldDef SESSION_STARTUP = sessionField("startup_id");

    private final @NotNull DataSource ds;
    private final @NotNull SqlDialect dialect;
    private final @NotNull Logger logger;

    public SqlServerOwnershipAdapter(
            @NotNull DataSource ds,
            @NotNull SqlDialect dialect,
            @NotNull Logger logger) {
        this.ds = ds;
        this.dialect = dialect;
        this.logger = logger;
    }

    @Override
    public void ensureStore(@NotNull StoreId id) throws BackendException {
        String table = table(id);
        try (Connection c = ds.getConnection(); Statement s = c.createStatement()) {
            s.executeUpdate("CREATE TABLE IF NOT EXISTS " + table + " ("
                    + q("persistent_id") + " " + textColumnType() + " PRIMARY KEY, "
                    + q("owner_startup_id") + " " + textColumnType() + " NOT NULL, "
                    + q("fence") + " " + textColumnType() + " NOT NULL, "
                    + q("lease_expires_at_ms") + " BIGINT NOT NULL, "
                    + q("last_renewed_at_ms") + " BIGINT NOT NULL, "
                    + q("closed_at_ms") + " BIGINT NOT NULL, "
                    + q("close_reason") + " " + textColumnType() + ", "
                    + q("server_name") + " " + textColumnType() + ", "
                    + q("hostname") + " " + textColumnType() + ", "
                    + q("grim_version") + " " + textColumnType() + ", "
                    + q("server_version") + " " + textColumnType()
                    + ")" + ("mysql".equals(dialect.name()) ? " ENGINE=InnoDB" : ""));
        } catch (SQLException e) {
            throw new BackendException("failed to ensure server ownership table " + id.name(), e);
        }
    }

    @Override
    public long dbNowEpochMs() throws BackendException {
        try (Connection c = ds.getConnection()) {
            return dbNowEpochMs(c);
        } catch (SQLException e) {
            throw new BackendException("failed to read DB time", e);
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
        SQLException lastDuplicate = null;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (Connection c = ds.getConnection()) {
                boolean originalAutoCommit = c.getAutoCommit();
                try {
                    c.setAutoCommit(false);
                    long now = dbNowEpochMs(c);
                    ServerOwnershipSnapshot previous = select(c, id, persistentId, true).orElse(null);
                    if (previous == null) {
                        insert(c, id, persistentId, startupId, fence, now, now + ttlMs, metadata);
                        c.commit();
                        return OwnershipClaimResult.claimed(
                                persistentId, startupId, fence, now, now + ttlMs, null);
                    }

                    boolean alreadyMine = startupId.equals(previous.ownerStartupId())
                            && fence.equals(previous.fence());
                    boolean claimable = alreadyMine
                            || previous.closedAtEpochMs() != ServerOwnershipSnapshot.OPEN
                            || previous.leaseExpiresAtEpochMs() <= now;
                    if (!claimable) {
                        c.commit();
                        return OwnershipClaimResult.denied(
                                persistentId, startupId, fence, now, previous);
                    }

                    updateOwner(c, id, persistentId, startupId, fence, now, now + ttlMs, metadata);
                    c.commit();
                    return OwnershipClaimResult.claimed(
                            persistentId, startupId, fence, now, now + ttlMs, previous);
                } catch (SQLException e) {
                    rollbackQuietly(c);
                    if (isDuplicate(e)) {
                        lastDuplicate = e;
                        continue;
                    }
                    throw e;
                } finally {
                    try { c.setAutoCommit(originalAutoCommit); }
                    catch (SQLException ignored) {}
                }
            } catch (SQLException e) {
                throw new BackendException("failed to claim server ownership", e);
            }
        }
        throw new BackendException("failed to claim server ownership after duplicate-key retries", lastDuplicate);
    }

    @Override
    public OwnershipClaimResult claimRecovery(StoreId id, OwnershipToken candidate, @Nullable OwnershipToken expected,
                                              long ttlMs, ServerOwnershipMetadata metadata) throws BackendException {
        return atomic("recover server ownership", true, id, "persistent_id", c -> {
            // PostgreSQL row locks don't protect a missing key. InnoDB's locking read
            // takes a gap lock at REPEATABLE READ. SQLite has the database writer lock.
            if (expected == null) lockPostgresTable(c, id);
            UUID instanceId = candidate.instanceId();
            UUID startupId = candidate.startupId();
            UUID fence = candidate.fence();
            ServerOwnershipSnapshot current = select(c, id, instanceId, true).orElse(null);
            long now = dbNowEpochMs(c);
            boolean claimable = expected == null ? current == null
                    : current != null && expected.equals(OwnershipToken.of(current)) && !current.activeAt(now);
            if (!claimable) return OwnershipClaimResult.denied(instanceId, startupId, fence, now, current);

            long expires = now + ttlMs;
            if (current == null) {
                insert(c, id, instanceId, startupId, fence, now, expires, metadata);
            } else {
                updateOwner(c, id, instanceId, startupId, fence, now, expires, metadata);
            }
            return OwnershipClaimResult.claimed(instanceId, startupId, fence, now, expires, current);
        });
    }

    @Override
    public void publishStartup(StartupStores stores, @Nullable OwnershipToken owner, ServerStartupRecord row)
            throws BackendException {
        requireDistinctTables(stores);
        requireIdentity(owner, row.instanceId(), row.startupId());
        if (row.isClosed()) throw new BackendException("SQL startup publication requires an open row");
        StoreId startups = stores.startups();
        startupMutation("publish startup", stores, owner, c -> {
            StartupState current = selectStartup(c, startups, row.startupId());
            if (current != null) {
                requireStartupInstance(current, row.instanceId());
                if (current.closedAt() != ServerStartupRecord.OPEN) {
                    throw new BackendException("SQL startup is closed");
                }
                if (current.heartbeat() > row.lastHeartbeatEpochMs()) return null;
            }
            // If two publishers both see an absent PostgreSQL row, the loser
            // retries the insert race and checks the now-existing identity.
            writeStartup(c, startups, row, current == null);
            return null;
        }, startups);
    }

    @Override
    public long reassignSession(StartupStores stores, @Nullable OwnershipToken owner, EntityOps.ReassignSessionOp op)
            throws BackendException {
        requireDistinctTables(stores);
        if (owner != null) requireIdentity(owner, owner.instanceId(), op.startupId());
        return startupMutation("reassign session", stores, owner, c -> {
            // The destination row lock orders this move with a close or repair of that startup.
            requireOpenStartup(c, stores.startups(), op.startupId(), owner, null);
            return SqlEntityAdapter.reassignSession(c, dialect, table(stores.sessions()), SESSIONS, op);
        }, stores.startups(), stores.sessions());
    }

    @Override
    public void publishSession(StartupStores stores, @Nullable OwnershipToken owner, SessionRecord row)
            throws BackendException {
        requireDistinctTables(stores);
        UUID startupId = row.startupId();
        if (startupId == null) throw new BackendException("SQL session publication requires a startup");
        if (owner != null) requireIdentity(owner, owner.instanceId(), startupId);
        startupMutation("publish session", stores, owner, c -> {
            // Close and repair lock the startup row before its sessions, so a first insert
            // either commits before the close or sees the closed startup and is refused.
            StartupState startup = requireOpenStartup(c, stores.startups(), startupId, owner, row.instanceId());
            StoredSession stored = selectSession(c, stores.sessions(), row.sessionId());
            if (stored != null) {
                if (stored.player() != null && !stored.player().equals(row.playerUuid())) {
                    throw new BackendException("SQL session belongs to another player");
                }
                UUID storedStartup = stored.startup();
                if (storedStartup != null && !storedStartup.equals(startupId)) {
                    if (!row.isClosed()) throw new BackendException("SQL session belongs to another startup");
                    // A locking read sees the latest committed row. A plain InnoDB read would use the
                    // snapshot taken at transaction start, before a startup created by a concurrent reload.
                    StartupState retained = selectStartup(c, stores.startups(), storedStartup, false, true);
                    if (retained == null || !Objects.equals(retained.instanceId(), startup.instanceId())) {
                        throw new BackendException("SQL session belongs to another instance");
                    }
                }
                if (stored.closedAt() != SessionRecord.OPEN) return null;
            }
            // The merge keeps a stored startup and a stored closure. An absent row is inserted without a
            // conflict clause: a concurrent first insert then fails, and the retry re-reads it under the checks.
            if (stored == null) SqlEntityAdapter.insertOn(c, dialect, stores.sessions().name(), SESSIONS, row);
            else SqlEntityAdapter.upsertOn(c, dialect, stores.sessions().name(), SESSIONS, row);
            return null;
        }, stores.startups(), stores.sessions());
    }

    @Override
    public long closeStartup(StartupStores stores, @Nullable OwnershipToken owner, UUID instanceId, UUID startupId)
            throws BackendException {
        requireIdentity(owner, instanceId, startupId);
        return closeStartup(stores, owner, instanceId, startupId, null);
    }

    @Override
    public long repairStaleStartup(StartupStores stores, @Nullable OwnershipToken sweeper, UUID targetInstanceId,
                                   UUID targetStartupId, long staleHeartbeatTtlMs) throws BackendException {
        return closeStartup(stores, sweeper, targetInstanceId, targetStartupId, staleHeartbeatTtlMs);
    }

    /**
     * Runs co-located JDBC DML with the exact ownership row locked, checking DB time before the
     * callback and again before commit. Supply the physical store IDs of every table the callback
     * mutates so MySQL can validate their engines. The callback must use only this connection and
     * this database's transactional tables. The adapter controls the transaction and its connection.
     */
    public <T> T withOwnedTransaction(StoreId ownership, OwnershipToken owner, OwnedSqlWork<T> work, StoreId... mutationStores)
            throws BackendException {
        Objects.requireNonNull(owner, "owner");
        return atomic("run owned JDBC mutation", false, ownership, "persistent_id", guarded(ownership, owner, work),
                mutationStores);
    }

    @FunctionalInterface
    public interface OwnedSqlWork<T> {
        T execute(Connection connection) throws SQLException, BackendException;
    }

    private long closeStartup(StartupStores stores, @Nullable OwnershipToken actor, UUID instanceId, UUID startupId,
                              @Nullable Long staleTtlMs) throws BackendException {
        requireDistinctTables(stores);
        boolean stale = staleTtlMs != null;
        StoreId ownership = stores.ownership();
        StoreId startups = stores.startups();
        StoreId sessions = stores.sessions();
        StoreId first = actor == null ? startups : ownership;
        String key = actor == null ? "startup_id" : "persistent_id";
        OwnedSqlWork<Long> close = c -> {
            ServerOwnershipSnapshot targetLease = stale && actor != null
                    ? select(c, ownership, instanceId, true).orElse(null) : null;

            StartupState target = selectStartup(c, startups, startupId);
            if (target == null) return 0L;
            requireStartupInstance(target, instanceId);
            if (target.closedAt() != ServerStartupRecord.OPEN) return 0L;

            long now = dbNowEpochMs(c);
            long closedAt;
            if (stale) {
                long lastRenewed = 0L;
                if (actor != null) {
                    if (actor.startupId().equals(startupId)) return 0L;
                    if (targetLease != null && targetLease.ownerStartupId().equals(startupId)) {
                        if (targetLease.activeAt(now)) return 0L;
                        lastRenewed = targetLease.lastRenewedAtEpochMs();
                    }
                } else if (now - target.heartbeat() <= staleTtlMs) {
                    return 0L;
                }
                closedAt = Math.max(lastRenewed, Math.max(target.startedAt(), target.heartbeat()));
            } else {
                closedAt = now;
            }

            // PostgreSQL needs a table lock to keep concurrent session inserts out of
            // the set being closed. InnoDB protects that range with next-key locks.
            lockPostgresTable(c, sessions);
            lockOpenSessions(c, sessions, startupId, stale);
            if (!stale) closedAt = dbNowEpochMs(c);
            if (closedAt <= 0L) throw new BackendException("Invalid SQL startup close time");
            long closed = closeSessions(c, sessions, startupId, stale ? null : closedAt);
            closeStartupRow(c, startups, startupId, closedAt, stale ? "stale" : "graceful");
            return closed;
        };
        return atomic("close startup and sessions", true, first, key, c -> {
            // Protect the target instance even if it has no ownership row yet.
            if (stale && actor != null) lockPostgresTable(c, ownership);
            return actor == null ? close.execute(c) : guarded(ownership, actor, close).execute(c);
        }, startups, sessions);
    }

    private <T> T startupMutation(String operation, StartupStores stores, @Nullable OwnershipToken owner,
                                  OwnedSqlWork<T> work, StoreId... targets) throws BackendException {
        if (owner == null) return atomic(operation, true, stores.startups(), "startup_id", work, targets);
        StoreId ownership = stores.ownership();
        return atomic(operation, true, ownership, "persistent_id", guarded(ownership, owner, work), targets);
    }

    private StartupState requireOpenStartup(Connection c, StoreId startups, UUID startupId,
                                            @Nullable OwnershipToken owner, @Nullable UUID rowInstance)
            throws SQLException, BackendException {
        StartupState startup = selectStartup(c, startups, startupId);
        if (startup == null) throw new BackendException("SQL startup is missing");
        if (startup.closedAt() != ServerStartupRecord.OPEN) throw new BackendException("SQL startup is closed");
        if (owner != null) requireStartupInstance(startup, owner.instanceId());
        if (rowInstance != null) requireStartupInstance(startup, rowInstance);
        return startup;
    }

    private @Nullable StoredSession selectSession(Connection c, StoreId sessions, UUID sessionId) throws SQLException {
        String sql = "SELECT " + q(SESSION_PLAYER.name()) + ", " + q(SESSION_STARTUP.name()) + ", " + q("closed_at")
                + " FROM " + table(sessions) + " WHERE " + q(SESSION_ID.name()) + " = ?" + forUpdate();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, SESSION_ID, sessionId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new StoredSession((UUID) SqlBindings.extract(rs, SESSION_PLAYER),
                        (UUID) SqlBindings.extract(rs, SESSION_STARTUP), rs.getLong("closed_at"));
            }
        }
    }

    private record StoredSession(@Nullable UUID player, @Nullable UUID startup, long closedAt) {}

    private static EncodeShape.FieldDef sessionField(String name) {
        return SESSION.fields().stream().filter(field -> field.name().equals(name)).findFirst().orElseThrow();
    }

    private <T> OwnedSqlWork<T> guarded(StoreId ownership, OwnershipToken owner, OwnedSqlWork<T> work) {
        return c -> {
            requireLiveOwner(c, ownership, owner);
            T result = work.execute(c);
            // A write may have waited for a startup or session lock past lease expiry.
            requireLiveOwner(c, ownership, owner);
            return result;
        };
    }

    private void requireLiveOwner(Connection c, StoreId ownership, OwnershipToken owner)
            throws SQLException, BackendException {
        ServerOwnershipSnapshot current = select(c, ownership, owner.instanceId(), true).orElse(null);
        if (current == null || !owner.equals(OwnershipToken.of(current)) || !current.activeAt(dbNowEpochMs(c))) {
            throw new BackendException("Invalid or expired SQL ownership fence");
        }
    }

    private static void requireIdentity(@Nullable OwnershipToken owner, UUID instanceId, UUID startupId)
            throws BackendException {
        if (owner != null && (!owner.instanceId().equals(instanceId) || !owner.startupId().equals(startupId))) {
            throw new BackendException("SQL ownership token does not identify this startup");
        }
    }

    private void requireDistinctTables(StartupStores stores) throws BackendException {
        // SQL adapters map StoreId.name() to a physical table independently of its namespace.
        String a = stores.ownership().name();
        String b = stores.startups().name();
        String c = stores.sessions().name();
        boolean collides = "postgres".equals(dialect.name())
                ? a.equals(b) || b.equals(c) || a.equals(c)
                : a.equalsIgnoreCase(b) || b.equalsIgnoreCase(c) || a.equalsIgnoreCase(c);
        if (collides) {
            throw new BackendException("Atomic startup operations require distinct SQL tables");
        }
    }

    private @Nullable StartupState selectStartup(Connection c, StoreId id, UUID startupId) throws SQLException {
        return selectStartup(c, id, startupId, true);
    }

    private @Nullable StartupState selectStartup(Connection c, StoreId id, UUID startupId, boolean lock)
            throws SQLException {
        return selectStartup(c, id, startupId, lock, false);
    }

    private @Nullable StartupState selectStartup(Connection c, StoreId id, UUID startupId, boolean lock, boolean current)
            throws SQLException {
        String read = lock ? forUpdate() : current && "mysql".equals(dialect.name()) ? " LOCK IN SHARE MODE" : "";
        String sql = "SELECT instance_id, started_at, last_heartbeat, closed_at FROM " + table(id)
                + " WHERE startup_id = ?" + read;
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, STARTUP_ID, startupId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return new StartupState((UUID) SqlBindings.extract(rs, INSTANCE_ID),
                        rs.getLong("started_at"), rs.getLong("last_heartbeat"), rs.getLong("closed_at"));
            }
        }
    }

    private record StartupState(@Nullable UUID instanceId, long startedAt, long heartbeat, long closedAt) {}

    private static void requireStartupInstance(StartupState current, UUID instanceId) throws BackendException {
        if (!instanceId.equals(current.instanceId())) throw new BackendException("SQL startup instance mismatch");
    }

    private void writeStartup(Connection c, StoreId id, ServerStartupRecord row, boolean insert) throws SQLException {
        String sql = insert
                ? "INSERT INTO " + table(id) + " (server_name, grim_version, server_version, hostname, started_at, "
                        + "last_heartbeat, verbose_manifest, instance_id, startup_id, closed_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)"
                : "UPDATE " + table(id) + " SET server_name = ?, grim_version = ?, server_version = ?, hostname = ?, "
                        + "started_at = ?, last_heartbeat = ?, verbose_manifest = ?, close_reason = NULL "
                        + "WHERE instance_id = ? AND startup_id = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, row.serverName());
            ps.setString(2, row.grimVersion());
            ps.setString(3, row.serverVersionString());
            ps.setString(4, row.hostname());
            ps.setLong(5, row.startedEpochMs());
            ps.setLong(6, row.lastHeartbeatEpochMs());
            ps.setBytes(7, row.verboseManifest());
            SqlBindings.bind(ps, 8, INSTANCE_ID, row.instanceId());
            SqlBindings.bind(ps, 9, STARTUP_ID, row.startupId());
            ps.executeUpdate();
        }
    }

    private void lockOpenSessions(Connection c, StoreId sessions, UUID startupId, boolean stale)
            throws SQLException, BackendException {
        if (!stale && !"mysql".equals(dialect.name())) return;
        String sql = "SELECT last_activity FROM " + table(sessions)
                + " WHERE startup_id = ? AND closed_at = 0" + forUpdate();
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            SqlBindings.bind(ps, 1, STARTUP_ID, startupId);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    if (stale && rs.getLong(1) <= 0L) throw new BackendException("Invalid SQL session last activity");
                }
            }
        }
    }

    private long closeSessions(Connection c, StoreId sessions, UUID startupId, @Nullable Long gracefulTime)
            throws SQLException {
        String sql = "UPDATE " + table(sessions) + " SET closed_at = " + (gracefulTime == null ? "last_activity" : "?")
                + " WHERE startup_id = ? AND closed_at = 0";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            int parameter = 1;
            if (gracefulTime != null) ps.setLong(parameter++, gracefulTime);
            SqlBindings.bind(ps, parameter, STARTUP_ID, startupId);
            return ps.executeUpdate();
        }
    }

    private void closeStartupRow(Connection c, StoreId startups, UUID startupId, long closedAt, String reason)
            throws SQLException {
        String sql = "UPDATE " + table(startups) + " SET closed_at = ?, close_reason = ? WHERE startup_id = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setLong(1, closedAt);
            ps.setString(2, reason);
            SqlBindings.bind(ps, 3, STARTUP_ID, startupId);
            if (ps.executeUpdate() != 1) throw new SQLException("SQL startup disappeared during close");
        }
    }

    private static EncodeShape.FieldDef startupField(String name) {
        return STARTUP.fields().stream().filter(field -> field.name().equals(name)).findFirst().orElseThrow();
    }

    private <T> T atomic(String operation, boolean retryConflicts, StoreId first, String key, OwnedSqlWork<T> work,
                         StoreId... otherTables) throws BackendException {
        for (int attempt = 0; ; attempt++) {
            try {
                return transaction(first, key, work, otherTables);
            } catch (SQLException e) {
                String state = e.getSQLState();
                boolean conflict = "40001".equals(state) || "40P01".equals(state) || "23505".equals(state)
                        || e.getErrorCode() == 1062;
                if (!retryConflicts || !conflict || attempt >= 2 || e.getSuppressed().length != 0) {
                    throw new BackendException("Failed to " + operation, e);
                }
            }
        }
    }

    private <T> T transaction(StoreId first, String key, OwnedSqlWork<T> work, StoreId... otherTables)
            throws SQLException, BackendException {
        try (Connection c = ds.getConnection()) {
            if (!c.getAutoCommit()) throw new BackendException("SQL ownership requires a fresh JDBC transaction");
            int isolation = c.getTransactionIsolation();
            int requiredIsolation = switch (dialect.name()) {
                case "postgres" -> Connection.TRANSACTION_READ_COMMITTED;
                case "mysql" -> Connection.TRANSACTION_REPEATABLE_READ;
                default -> isolation;
            };
            if (isolation != requiredIsolation) c.setTransactionIsolation(requiredIsolation);
            c.setAutoCommit(false);
            T result;
            try {
                if ("sqlite".equals(dialect.name())) {
                    // Reserve the SQLite writer before reading any snapshot, even when
                    // using a driver that starts a deferred JDBC transaction.
                    try (Statement s = c.createStatement()) {
                        s.executeUpdate("UPDATE " + table(first) + " SET " + q(key) + " = " + q(key) + " WHERE 1 = 0");
                    }
                } else if ("mysql".equals(dialect.name())) {
                    requireInnoDb(c, first);
                    for (StoreId id : otherTables) {
                        if (!first.equals(id)) requireInnoDb(c, id);
                    }
                }
                result = work.execute(c);
                c.commit();
            } catch (SQLException | BackendException | RuntimeException | Error failure) {
                try {
                    if (!c.getAutoCommit()) {
                        c.rollback();
                        restoreConnection(c, isolation, requiredIsolation);
                    }
                } catch (SQLException cleanup) {
                    failure.addSuppressed(cleanup);
                }
                throw failure;
            }
            try {
                restoreConnection(c, isolation, requiredIsolation);
            } catch (SQLException e) {
                throw new BackendException("SQL mutation committed but JDBC connection reset failed", e);
            }
            return result;
        }
    }

    private static void restoreConnection(Connection c, int isolation, int requiredIsolation) throws SQLException {
        c.setAutoCommit(true);
        if (isolation != requiredIsolation) c.setTransactionIsolation(isolation);
    }

    private void lockPostgresTable(Connection c, StoreId id) throws SQLException {
        if (!"postgres".equals(dialect.name())) return;
        try (Statement s = c.createStatement()) {
            s.execute("LOCK TABLE " + table(id) + " IN EXCLUSIVE MODE");
        }
    }

    private void requireInnoDb(Connection c, StoreId id) throws SQLException, BackendException {
        // Open the table so concurrent engine-changing DDL waits for this transaction.
        try (Statement s = c.createStatement(); ResultSet ignored = s.executeQuery("SELECT 1 FROM " + table(id) + " LIMIT 0")) {
            // The metadata lock is held until transaction end.
        }
        String sql = "SELECT ENGINE FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() "
                + "AND BINARY IF(@@lower_case_table_names = 0, TABLE_NAME, LOWER(TABLE_NAME)) "
                + "= BINARY IF(@@lower_case_table_names = 0, ?, LOWER(?))";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            ps.setString(1, id.name());
            ps.setString(2, id.name());
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next() || !"InnoDB".equalsIgnoreCase(rs.getString(1))) {
                    throw new BackendException("Atomic SQL mutation requires an InnoDB table: " + id.name());
                }
            }
        }
    }

    @Override
    public @NotNull OwnershipRenewResult renewOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long ttlMs) throws BackendException {
        return atomic("renew server ownership", true, id, "persistent_id", c -> {
            // A guarded mutation can hold this row past expiry. Renew against
            // the current backend time once the write can actually proceed.
            ServerOwnershipSnapshot current = select(c, id, persistentId, true).orElse(null);
            long now = dbNowEpochMs(c);
            if (current == null || !current.activeAt(now)) {
                return OwnershipRenewResult.lost(persistentId, startupId, fence, now);
            }
            String sql = "UPDATE " + table(id) + " SET "
                    + q("lease_expires_at_ms") + " = ?, "
                    + q("last_renewed_at_ms") + " = ?, "
                    + q("closed_at_ms") + " = 0, "
                    + q("close_reason") + " = NULL "
                    + "WHERE " + q("persistent_id") + " = ? "
                    + "AND " + q("owner_startup_id") + " = ? "
                    + "AND " + q("fence") + " = ? "
                    + "AND " + q("closed_at_ms") + " = 0 "
                    + "AND " + q("lease_expires_at_ms") + " > ?";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setLong(1, now + ttlMs);
                ps.setLong(2, now);
                bindUuid(ps, 3, persistentId);
                bindUuid(ps, 4, startupId);
                bindUuid(ps, 5, fence);
                ps.setLong(6, now);
                int rows = ps.executeUpdate();
                if (rows == 1) {
                    return OwnershipRenewResult.renewed(
                            persistentId, startupId, fence, now, now + ttlMs);
                }
                return OwnershipRenewResult.lost(persistentId, startupId, fence, now);
            }
        });
    }

    @Override
    public boolean closeOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            @NotNull String reason) throws BackendException {
        try (Connection c = ds.getConnection()) {
            long now = dbNowEpochMs(c);
            String sql = "UPDATE " + table(id) + " SET "
                    + q("closed_at_ms") + " = ?, "
                    + q("close_reason") + " = ? "
                    + "WHERE " + q("persistent_id") + " = ? "
                    + "AND " + q("owner_startup_id") + " = ? "
                    + "AND " + q("fence") + " = ? "
                    + "AND " + q("closed_at_ms") + " = 0";
            try (PreparedStatement ps = c.prepareStatement(sql)) {
                ps.setLong(1, now);
                ps.setString(2, reason);
                bindUuid(ps, 3, persistentId);
                bindUuid(ps, 4, startupId);
                bindUuid(ps, 5, fence);
                return ps.executeUpdate() == 1;
            }
        } catch (SQLException e) {
            throw new BackendException("failed to close server ownership", e);
        }
    }

    @Override
    public @NotNull Optional<ServerOwnershipSnapshot> readOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId) throws BackendException {
        try (Connection c = ds.getConnection()) {
            return select(c, id, persistentId, false);
        } catch (SQLException e) {
            throw new BackendException("failed to read server ownership", e);
        }
    }

    private long dbNowEpochMs(@NotNull Connection c) throws SQLException {
        String sql = switch (dialect.name()) {
            case "postgres" -> "SELECT CAST(FLOOR(EXTRACT(EPOCH FROM clock_timestamp()) * 1000) AS BIGINT)";
            case "mysql" -> "SELECT CAST(UNIX_TIMESTAMP(CURRENT_TIMESTAMP(3)) * 1000 AS UNSIGNED)";
            case "sqlite" -> "SELECT CAST((julianday('now') - 2440587.5) * 86400000 AS INTEGER)";
            default -> "SELECT CAST(EXTRACT(EPOCH FROM CURRENT_TIMESTAMP) * 1000 AS BIGINT)";
        };
        try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery(sql)) {
            if (!rs.next()) throw new SQLException("DB time query returned no rows");
            return rs.getLong(1);
        }
    }

    private Optional<ServerOwnershipSnapshot> select(
            @NotNull Connection c,
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            boolean forUpdate) throws SQLException {
        String sql = "SELECT * FROM " + table(id)
                + " WHERE " + q("persistent_id") + " = ?"
                + (forUpdate ? forUpdate() : "");
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bindUuid(ps, 1, persistentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Optional.empty();
                return Optional.of(readSnapshot(rs));
            }
        }
    }

    private void insert(
            @NotNull Connection c,
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long now,
            long leaseExpiresAt,
            @NotNull ServerOwnershipMetadata metadata) throws SQLException {
        String sql = "INSERT INTO " + table(id) + " ("
                + q("persistent_id") + ", "
                + q("owner_startup_id") + ", "
                + q("fence") + ", "
                + q("lease_expires_at_ms") + ", "
                + q("last_renewed_at_ms") + ", "
                + q("closed_at_ms") + ", "
                + q("close_reason") + ", "
                + q("server_name") + ", "
                + q("hostname") + ", "
                + q("grim_version") + ", "
                + q("server_version")
                + ") VALUES (?, ?, ?, ?, ?, 0, NULL, ?, ?, ?, ?)";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bindOwnerFields(ps, persistentId, startupId, fence, leaseExpiresAt, now, metadata);
            ps.executeUpdate();
        }
    }

    private void updateOwner(
            @NotNull Connection c,
            @NotNull StoreId id,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long now,
            long leaseExpiresAt,
            @NotNull ServerOwnershipMetadata metadata) throws SQLException {
        String sql = "UPDATE " + table(id) + " SET "
                + q("owner_startup_id") + " = ?, "
                + q("fence") + " = ?, "
                + q("lease_expires_at_ms") + " = ?, "
                + q("last_renewed_at_ms") + " = ?, "
                + q("closed_at_ms") + " = 0, "
                + q("close_reason") + " = NULL, "
                + q("server_name") + " = ?, "
                + q("hostname") + " = ?, "
                + q("grim_version") + " = ?, "
                + q("server_version") + " = ? "
                + "WHERE " + q("persistent_id") + " = ?";
        try (PreparedStatement ps = c.prepareStatement(sql)) {
            bindUuid(ps, 1, startupId);
            bindUuid(ps, 2, fence);
            ps.setLong(3, leaseExpiresAt);
            ps.setLong(4, now);
            ps.setString(5, metadata.serverName());
            ps.setString(6, metadata.hostname());
            ps.setString(7, metadata.grimVersion());
            ps.setString(8, metadata.serverVersionString());
            bindUuid(ps, 9, persistentId);
            ps.executeUpdate();
        }
    }

    private void bindOwnerFields(
            @NotNull PreparedStatement ps,
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long leaseExpiresAt,
            long now,
            @NotNull ServerOwnershipMetadata metadata) throws SQLException {
        bindUuid(ps, 1, persistentId);
        bindUuid(ps, 2, startupId);
        bindUuid(ps, 3, fence);
        ps.setLong(4, leaseExpiresAt);
        ps.setLong(5, now);
        ps.setString(6, metadata.serverName());
        ps.setString(7, metadata.hostname());
        ps.setString(8, metadata.grimVersion());
        ps.setString(9, metadata.serverVersionString());
    }

    private @NotNull ServerOwnershipSnapshot readSnapshot(@NotNull ResultSet rs) throws SQLException {
        return new ServerOwnershipSnapshot(
                readUuid(rs, "persistent_id"),
                readUuid(rs, "owner_startup_id"),
                readUuid(rs, "fence"),
                rs.getLong("lease_expires_at_ms"),
                rs.getLong("last_renewed_at_ms"),
                rs.getLong("closed_at_ms"),
                rs.getString("close_reason"),
                rs.getString("server_name"),
                rs.getString("hostname"),
                rs.getString("grim_version"),
                rs.getString("server_version"));
    }

    private void bindUuid(@NotNull PreparedStatement ps, int idx, @NotNull UUID uuid) throws SQLException {
        ps.setString(idx, uuid.toString());
    }

    private @NotNull UUID readUuid(@NotNull ResultSet rs, @NotNull String column) throws SQLException {
        Object value = rs.getObject(column);
        if (value instanceof UUID u) return u;
        if (value instanceof String s) return UUID.fromString(s);
        throw new SQLException("cannot read UUID column " + column + " from "
                + (value == null ? "null" : value.getClass().getName()));
    }

    private @NotNull String textColumnType() {
        return "mysql".equals(dialect.name()) ? "VARCHAR(512)" : "TEXT";
    }

    private @NotNull String q(@NotNull String identifier) {
        return dialect.quoteIdentifier(identifier);
    }

    private String table(StoreId id) {
        // An attached SQLite database cannot join the main file's WAL commit.
        return ("sqlite".equals(dialect.name()) ? "main." : "") + q(id.name());
    }

    private String forUpdate() {
        return "sqlite".equals(dialect.name()) ? "" : " FOR UPDATE";
    }

    private static void rollbackQuietly(@NotNull Connection c) {
        try { c.rollback(); }
        catch (SQLException ignored) {}
    }

    private boolean isDuplicate(@NotNull SQLException e) {
        String state = e.getSQLState();
        if (state != null && state.startsWith("23")) return true;
        String message = e.getMessage();
        boolean duplicate = message != null
                && message.toLowerCase(Locale.ROOT).contains("duplicate");
        if (duplicate) {
            logger.log(Level.FINE, "duplicate ownership insert race; retrying", e);
        }
        return duplicate;
    }
}

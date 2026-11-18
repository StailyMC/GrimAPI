package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.ServerOwnershipSnapshot;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.kind.ops.EntityOps;
import ac.grim.grimac.api.storage.model.ServerStartupRecord;
import ac.grim.grimac.api.storage.model.SessionRecord;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import com.mongodb.MongoException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.TransactionOptions;
import com.mongodb.WriteConcern;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;

import static ac.grim.grimac.internal.storage.backend.mongo.v2.MongoServerOwnershipAdapter.databaseMillis;
import static ac.grim.grimac.internal.storage.backend.mongo.v2.MongoServerOwnershipAdapter.liveToken;

/**
 * Startup and session operations for the native v2 collections. Fencing requires real writes to
 * the lease documents in the same transaction as the data writes: snapshot reads alone permit write skew.
 * The new transactional operations require one replica-set database. Ordinary entity operations, initial
 * leases, recovery claims and unfenced publication retain their single-document Mongo paths.
 */
final class MongoStartupTransactions {
    private static final ac.grim.grimac.api.storage.kind.Entity<?, ?, ?> SESSIONS =
            ac.grim.grimac.internal.storage.category.V2BuiltinKinds.sessions();
    private static final int DUPLICATE_KEY = 11000;
    private static final int PIPELINE_UPDATE_WIRE_VERSION = 8;
    private static final String GUARD = "_grim_lease_guard";
    private static final String GUARD_TIME = "_grim_lease_guard_ms";
    private static final TransactionOptions TRANSACTION = TransactionOptions.builder()
            .readPreference(ReadPreference.primary()).readConcern(ReadConcern.SNAPSHOT)
            .writeConcern(WriteConcern.MAJORITY).build();

    private final MongoClient client;
    private final MongoDatabase db;

    MongoStartupTransactions(MongoClient client, MongoDatabase db) {
        this.client = client;
        this.db = db;
    }

    void publish(StartupStores stores, @Nullable OwnershipToken owner, ServerStartupRecord row) throws BackendException {
        if (row.isClosed() || row.closeReason() != null) throw new BackendException("Mongo publication requires an OPEN startup");
        requireIdentity(owner, row.instanceId(), row.startupId());
        if (owner == null) {
            try {
                requireDistinctCollections(stores);
                publishRow(null, stores.startups(), row);
            } catch (RuntimeException e) {
                throw new BackendException("Mongo startup publication failed", e);
            }
            return;
        }
        transact(stores, owner, "startup publication", session -> {
            publishRow(session, stores.startups(), row);
            return null;
        });
    }

    long reassign(StartupStores stores, @Nullable OwnershipToken owner, EntityOps.ReassignSessionOp op) throws BackendException {
        if (owner != null && !owner.startupId().equals(op.startupId())) {
            throw new BackendException("Mongo session reassignment requires its destination startup's grant");
        }
        return transact(stores, owner, "session reassignment", session -> {
            guardOpenStartup(session, stores.startups(), op.startupId(), owner, null);
            Bson filter = Filters.and(id(op.sessionId()),
                    Filters.in("startup_id", op.previousStartups().stream().map(BsonBinaries::uuidBinary).toList()),
                    Filters.ne("startup_id", BsonBinaries.uuidBinary(op.startupId())));
            return collection(stores.sessions()).updateOne(session, filter, Updates.combine(
                    Updates.set("startup_id", BsonBinaries.uuidBinary(op.startupId())),
                    Updates.set("closed_at", SessionRecord.OPEN))).getModifiedCount();
        });
    }

    void publishSession(StartupStores stores, @Nullable OwnershipToken owner, SessionRecord row) throws BackendException {
        UUID startupId = row.startupId();
        if (startupId == null) throw new BackendException("Mongo session publication requires a startup");
        if (owner != null) requireIdentity(owner, owner.instanceId(), startupId);
        transact(stores, owner, "session publication", session -> {
            Document startup = guardOpenStartup(session, stores.startups(), startupId, owner, row.instanceId());
            var sessions = collection(stores.sessions());
            Document stored = sessions.find(session, id(row.sessionId())).first();
            if (stored != null) {
                UUID player = BsonBinaries.toUuid(stored.get("player_uuid"));
                if (player != null && !player.equals(row.playerUuid())) throw new Rejected("session belongs to another player");
                UUID storedStartup = BsonBinaries.toUuid(stored.get("startup_id"));
                if (storedStartup != null && !storedStartup.equals(startupId)) {
                    if (!row.isClosed()) throw new Rejected("session belongs to another startup");
                    Document retained = collection(stores.startups()).find(session, id(storedStartup)).first();
                    if (retained == null || !Objects.equals(BsonBinaries.toUuid(startup.get("instance_id")),
                            BsonBinaries.toUuid(retained.get("instance_id")))) {
                        throw new Rejected("session belongs to another instance");
                    }
                }
                if (stored.get("closed_at") instanceof Number closed && closed.longValue() != SessionRecord.OPEN) return null;
            }
            // The merge keeps a stored startup and a stored closure.
            MongoEntityAdapter.upsertMerged(sessions, session, SESSIONS, row);
            return null;
        });
    }

    /**
     * Writes a guard to the OPEN startup so the transaction conflicts with a concurrent close or
     * repair of it. A snapshot read alone would let a first session insert commit after the close.
     */
    private Document guardOpenStartup(ClientSession session, StoreId store, UUID startupId,
                                      @Nullable OwnershipToken owner, @Nullable UUID rowInstance) {
        Document startup = collection(store).findOneAndUpdate(session, Filters.and(id(startupId), open()),
                Updates.set(GUARD, new ObjectId()), new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        if (startup == null) {
            throw new Rejected(collection(store).find(session, id(startupId)).first() == null
                    ? "startup is missing" : "startup is already closed");
        }
        if (owner != null) requireInstance(startup, owner.instanceId());
        if (rowInstance != null) requireInstance(startup, rowInstance);
        return startup;
    }

    long close(StartupStores stores, @Nullable OwnershipToken owner, UUID instanceId, UUID startupId) throws BackendException {
        requireIdentity(owner, instanceId, startupId);
        return transact(stores, owner, "graceful startup close", session -> {
            if (!openStartup(session, stores.startups(), instanceId, startupId)) return 0L;
            Document startup = terminalStartup(session, stores.startups(), instanceId, startupId,
                    databaseMillis(), "graceful", open());
            if (startup == null) return 0L;
            return collection(stores.sessions()).updateMany(session, openSessions(startupId),
                    Updates.set("closed_at", timestamp(startup, "closed_at"))).getModifiedCount();
        });
    }

    long repair(StartupStores stores, @Nullable OwnershipToken sweeper, UUID instanceId,
                UUID startupId, long heartbeatTtlMs) throws BackendException {
        if (sweeper == null && heartbeatTtlMs <= 0) throw new BackendException("Mongo heartbeat repair requires a positive TTL");
        return transact(stores, sweeper, "stale startup repair", session -> {
            if (!openStartup(session, stores.startups(), instanceId, startupId)) return 0L;
            if (sweeper != null && sweeper.startupId().equals(startupId)) return 0L;
            var leases = collection(stores.ownership());
            Document lease = sweeper == null ? null : lockTargetLease(session, leases, instanceId);
            boolean matchingLease = lease != null && Objects.equals(startupId, BsonBinaries.toUuid(lease.get("owner_startup_id")));
            if (matchingLease && timestamp(lease, "closed_at_ms") == ServerOwnershipSnapshot.OPEN
                    && timestamp(lease, "lease_expires_at_ms") > timestamp(lease, GUARD_TIME)) return 0L;
            Bson stale = sweeper == null
                    ? Filters.expr(new Document("$lt", List.of("$last_heartbeat",
                            new Document("$subtract", List.of(databaseMillis(), heartbeatTtlMs)))))
                    : open();
            long renewal = matchingLease ? timestamp(lease, "last_renewed_at_ms") : 0L;
            Document closeAt = new Document("$max", List.of("$started_at", "$last_heartbeat", renewal));
            Document startup = terminalStartup(session, stores.startups(), instanceId, startupId, closeAt, "stale", stale);
            long closed = startup == null ? 0L : collection(stores.sessions()).updateMany(session,
                    openSessions(startupId), List.of(new Document("$set", new Document("closed_at", "$last_activity"))))
                    .getModifiedCount();
            if (sweeper != null && lease == null) leases.deleteOne(session, id(instanceId));
            return closed;
        });
    }

    private void publishRow(@Nullable ClientSession session, StoreId store, ServerStartupRecord row) {
        var startups = session == null
                ? collection(store).withWriteConcern(WriteConcern.MAJORITY).withReadPreference(ReadPreference.primary())
                : collection(store);
        Bson selector = startupSelector(row.instanceId(), row.startupId());
        Bson updateFilter = Filters.and(selector, Filters.lte("last_heartbeat", row.lastHeartbeatEpochMs()));
        Document fields = new Document("instance_id", BsonBinaries.uuidBinary(row.instanceId()))
                .append("server_name", row.serverName()).append("hostname", row.hostname())
                .append("grim_version", row.grimVersion()).append("server_version", row.serverVersionString())
                .append("started_at", row.startedEpochMs()).append("last_heartbeat", row.lastHeartbeatEpochMs())
                .append("closed_at", ServerStartupRecord.OPEN).append("verbose_manifest", row.verboseManifest());
        Bson update = new Document("$set", fields);
        if ((session == null ? startups.updateOne(updateFilter, update) : startups.updateOne(session, updateFilter, update))
                .getMatchedCount() != 0) return;

        Bson byId = id(row.startupId());
        Document existing = (session == null ? startups.find(byId) : startups.find(session, byId)).first();
        if (existing != null) {
            requireOpenIdentity(existing, row.instanceId());
            return;
        }

        Document inserted = new Document("_id", BsonBinaries.uuidBinary(row.startupId()));
        inserted.putAll(fields);
        if (session != null) {
            startups.insertOne(session, inserted);
            return;
        }
        try {
            startups.insertOne(inserted);
        } catch (MongoWriteException collision) {
            if (collision.getError().getCode() != DUPLICATE_KEY) throw collision;
            if (startups.updateOne(updateFilter, update).getMatchedCount() != 0) return;
            Document winner = startups.find(byId).first();
            if (winner == null) throw collision;
            requireOpenIdentity(winner, row.instanceId());
        }
    }

    private static void requireOpenIdentity(Document row, UUID instanceId) {
        requireInstance(row, instanceId);
        if (timestamp(row, "closed_at") != ServerStartupRecord.OPEN) throw new Rejected("startup is already closed");
    }

    private boolean openStartup(ClientSession session, StoreId store, UUID instanceId, UUID startupId) {
        Document row = collection(store).find(session, id(startupId)).first();
        if (row == null) return false;
        requireInstance(row, instanceId);
        return timestamp(row, "closed_at") == ServerStartupRecord.OPEN;
    }

    private static void requireInstance(Document row, UUID instanceId) {
        if (!Objects.equals(instanceId, BsonBinaries.toUuid(row.get("instance_id")))) {
            throw new Rejected("startup belongs to a different instance");
        }
    }

    private @Nullable Document terminalStartup(ClientSession session, StoreId store, UUID instanceId, UUID startupId,
                                                Document closeAt, String reason, Bson eligible) {
        Bson update = new Document("$set", new Document("closed_at", closeAt)
                .append("last_heartbeat", new Document("$max", List.of("$last_heartbeat", closeAt)))
                .append("close_reason", reason));
        return collection(store).findOneAndUpdate(session, Filters.and(startupSelector(instanceId, startupId), eligible),
                List.of(update), new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
    }

    private static @Nullable Document lockTargetLease(ClientSession session, MongoCollection<Document> leases, UUID instanceId) {
        Bson guard = new Document("$set", new Document(GUARD, new ObjectId()).append(GUARD_TIME, databaseMillis()));
        Document lease = leases.findOneAndUpdate(session, id(instanceId), List.of(guard),
                new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
        if (lease != null) return lease;

        // A read of an absent row locks nothing. The unique key excludes a concurrent first claim
        // until this transaction ends, and the temporary row is removed before committing.
        try {
            leases.insertOne(session, new Document("_id", BsonBinaries.uuidBinary(instanceId)));
        } catch (MongoWriteException collision) {
            if (collision.getError().getCode() == DUPLICATE_KEY) {
                collision.addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL);
            }
            throw collision;
        }
        return null;
    }

    private <T> T transact(StartupStores stores, @Nullable OwnershipToken owner, String operation,
                           Function<ClientSession, T> body) throws BackendException {
        requireStartupTransactions();
        try {
            requireDistinctCollections(stores);
            try (ClientSession session = client.startSession()) {
                return session.withTransaction(() -> {
                    if (owner != null) guardOwner(session, stores.ownership(), owner);
                    T result = body.apply(session);
                    if (owner != null) guardOwner(session, stores.ownership(), owner);
                    return result;
                }, TRANSACTION);
            }
        } catch (RuntimeException e) {
            throw new BackendException("Mongo " + operation + " failed: " + e.getMessage(), e);
        }
    }

    void requireStartupTransactions() throws BackendException {
        try {
            requireReplicaSet(db.runCommand(new Document("hello", 1), ReadPreference.primary()));
        } catch (RuntimeException e) {
            throw new BackendException("Mongo startup transactions unavailable: " + e.getMessage(), e);
        }
    }

    private static void requireDistinctCollections(StartupStores stores) {
        String ownership = stores.ownership().name();
        String startups = stores.startups().name();
        String sessions = stores.sessions().name();
        if (ownership.equals(startups) || ownership.equals(sessions) || startups.equals(sessions)) {
            throw new Rejected("ownership, startups and sessions require distinct collections");
        }
    }

    static void requireReplicaSet(Document hello) {
        if (!(hello.get("setName") instanceof String) || !Boolean.TRUE.equals(hello.get("isWritablePrimary"))
                || !(hello.get("logicalSessionTimeoutMinutes") instanceof Number)
                || !(hello.get("maxWireVersion") instanceof Number version) || version.intValue() < PIPELINE_UPDATE_WIRE_VERSION) {
            throw new Rejected("atomic startup operations require a Mongo replica set with sessions and pipeline updates");
        }
    }

    private void guardOwner(ClientSession session, StoreId ownership, OwnershipToken owner) {
        if (collection(ownership).updateOne(session, liveToken(owner), Updates.set(GUARD, new ObjectId())).getMatchedCount() != 1) {
            throw new Rejected("ownership grant is no longer current");
        }
    }

    private static void requireIdentity(@Nullable OwnershipToken owner, UUID instanceId, UUID startupId) throws BackendException {
        if (owner != null && (!owner.instanceId().equals(instanceId) || !owner.startupId().equals(startupId))) {
            throw new BackendException("Mongo ownership grant does not identify this startup");
        }
    }

    private MongoCollection<Document> collection(StoreId id) {
        return db.getCollection(id.name());
    }

    private static Bson startupSelector(UUID instanceId, UUID startupId) {
        return Filters.and(id(startupId), Filters.eq("instance_id", BsonBinaries.uuidBinary(instanceId)), open());
    }

    private static Bson open() {
        return Filters.eq("closed_at", ServerStartupRecord.OPEN);
    }

    private static Bson openSessions(UUID startupId) {
        return Filters.and(Filters.eq("startup_id", BsonBinaries.uuidBinary(startupId)),
                Filters.or(Filters.eq("closed_at", SessionRecord.OPEN), Filters.eq("closed_at", null)));
    }

    private static Bson id(UUID uuid) {
        return Filters.eq("_id", BsonBinaries.uuidBinary(uuid));
    }

    private static long timestamp(Document row, String field) {
        Object value = row.get(field);
        if (value instanceof Number number) return number.longValue();
        throw new Rejected("missing Mongo timestamp " + field);
    }

    private static final class Rejected extends RuntimeException {
        Rejected(String message) { super(message); }
    }
}

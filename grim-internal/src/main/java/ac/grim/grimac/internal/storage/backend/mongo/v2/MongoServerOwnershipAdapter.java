package ac.grim.grimac.internal.storage.backend.mongo.v2;

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
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoWriteException;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Mongo implementation of the server ownership lease primitive.
 */
@ApiStatus.Internal
public final class MongoServerOwnershipAdapter implements ServerOwnershipAdapter {

    private static final int NAMESPACE_EXISTS = 48;
    private static final int DUPLICATE_KEY = 11000;

    private final @NotNull MongoDatabase db;
    private final @NotNull Logger logger;
    private final MongoStartupTransactions startupTransactions;

    public MongoServerOwnershipAdapter(@NotNull MongoClient client, @NotNull MongoDatabase db, @NotNull Logger logger) {
        this.db = db;
        this.logger = logger;
        this.startupTransactions = new MongoStartupTransactions(client, db);
    }

    @Override
    public void ensureStore(@NotNull StoreId id) throws BackendException {
        try {
            try {
                db.createCollection(id.name());
            } catch (MongoCommandException e) {
                if (e.getErrorCode() != NAMESPACE_EXISTS) throw e;
            }
            collection(id).createIndex(new Document("lease_expires_at_ms", 1),
                    new IndexOptions().name("by_lease_expires"));
        } catch (RuntimeException e) {
            throw new BackendException("failed to ensure server ownership collection " + id.name(), e);
        }
    }

    @Override
    public long dbNowEpochMs() throws BackendException {
        try {
            Document hello = db.runCommand(new Document("hello", 1));
            Date localTime = hello.getDate("localTime");
            if (localTime != null) return localTime.getTime();
            logger.fine("[grim-datastore] Mongo hello.localTime missing; falling back to JVM time");
            return System.currentTimeMillis();
        } catch (RuntimeException e) {
            throw new BackendException("failed to read Mongo DB time", e);
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
        for (int attempt = 0; attempt < 3; attempt++) {
            try {
                long now = dbNowEpochMs();
                ServerOwnershipSnapshot previous = readOwnership(id, persistentId).orElse(null);
                Bson claimableFilter = Filters.and(
                        Filters.eq("_id", BsonBinaries.uuidBinary(persistentId)),
                        Filters.or(
                                Filters.ne("closed_at_ms", ServerOwnershipSnapshot.OPEN),
                                Filters.lte("lease_expires_at_ms", now),
                                Filters.and(
                                        Filters.eq("owner_startup_id", BsonBinaries.uuidBinary(startupId)),
                                        Filters.eq("fence", BsonBinaries.uuidBinary(fence)))));
                Bson update = Updates.combine(
                        Updates.set("owner_startup_id", BsonBinaries.uuidBinary(startupId)),
                        Updates.set("fence", BsonBinaries.uuidBinary(fence)),
                        Updates.set("lease_expires_at_ms", now + ttlMs),
                        Updates.set("last_renewed_at_ms", now),
                        Updates.set("closed_at_ms", ServerOwnershipSnapshot.OPEN),
                        Updates.unset("close_reason"),
                        Updates.set("server_name", metadata.serverName()),
                        Updates.set("hostname", metadata.hostname()),
                        Updates.set("grim_version", metadata.grimVersion()),
                        Updates.set("server_version", metadata.serverVersionString()));
                Document before = collection(id).findOneAndUpdate(
                        claimableFilter,
                        update,
                        new FindOneAndUpdateOptions()
                                .returnDocument(ReturnDocument.BEFORE));
                if (before != null) {
                    // Return the row replaced by this atomic claim, not the
                    // earlier observation used only to decide whether to insert.
                    return OwnershipClaimResult.claimed(
                            persistentId, startupId, fence, now, now + ttlMs, snapshot(before));
                }
                if (previous == null) {
                    try {
                        collection(id).insertOne(ownershipDocument(
                                persistentId, startupId, fence, now, now + ttlMs, metadata));
                        return OwnershipClaimResult.claimed(
                                persistentId, startupId, fence, now, now + ttlMs, null);
                    } catch (MongoWriteException e) {
                        if (e.getError() == null || e.getError().getCode() != DUPLICATE_KEY) throw e;
                    }
                }
                ServerOwnershipSnapshot current = readOwnership(id, persistentId).orElse(null);
                if (current != null) {
                    return OwnershipClaimResult.denied(
                            persistentId, startupId, fence, now, current);
                }
            } catch (RuntimeException e) {
                if (attempt == 2) throw new BackendException("failed to claim Mongo server ownership", e);
            }
        }
        throw new BackendException("failed to claim Mongo server ownership after retries", null);
    }

    @Override
    public OwnershipClaimResult claimRecovery(StoreId id, OwnershipToken candidate, @Nullable OwnershipToken expected,
                                              long ttlMs, ServerOwnershipMetadata metadata) throws BackendException {
        try {
            if (ttlMs <= 0) throw new BackendException("Mongo recovery claim requires a positive TTL");
            // Timestamp the grant conservatively; the predecessor filter uses the server clock at the write.
            Date dbTime = db.runCommand(new Document("hello", 1), ReadPreference.primary()).getDate("localTime");
            if (dbTime == null) throw new BackendException("Mongo recovery claim requires hello.localTime");
            long now = dbTime.getTime();
            long expires = Math.addExact(now, ttlMs);
            UUID instance = candidate.instanceId();
            UUID startup = candidate.startupId();
            UUID fence = candidate.fence();
            var rows = collection(id).withWriteConcern(WriteConcern.MAJORITY)
                    .withReadPreference(ReadPreference.primary());
            if (expected == null) {
                try {
                    rows.insertOne(ownershipDocument(instance, startup, fence, now, expires, metadata));
                    return OwnershipClaimResult.claimed(instance, startup, fence, now, expires, null);
                } catch (MongoWriteException e) {
                    if (e.getError().getCode() != DUPLICATE_KEY) throw e;
                }
            } else if (instance.equals(expected.instanceId())) {
                Bson inactive = Filters.or(
                        Filters.and(Filters.type("closed_at_ms", "number"),
                                Filters.ne("closed_at_ms", ServerOwnershipSnapshot.OPEN)),
                        Filters.and(Filters.type("lease_expires_at_ms", "number"),
                                Filters.expr(new Document("$lte", List.of("$lease_expires_at_ms", databaseMillis())))));
                Bson update = Updates.combine(
                        Updates.set("owner_startup_id", BsonBinaries.uuidBinary(startup)),
                        Updates.set("fence", BsonBinaries.uuidBinary(fence)),
                        Updates.set("lease_expires_at_ms", expires),
                        Updates.set("last_renewed_at_ms", now),
                        Updates.set("closed_at_ms", ServerOwnershipSnapshot.OPEN),
                        Updates.unset("close_reason"),
                        Updates.set("server_name", metadata.serverName()),
                        Updates.set("hostname", metadata.hostname()),
                        Updates.set("grim_version", metadata.grimVersion()),
                        Updates.set("server_version", metadata.serverVersionString()));
                Document before = rows.findOneAndUpdate(Filters.and(exactToken(expected), inactive), update,
                        new FindOneAndUpdateOptions().returnDocument(ReturnDocument.BEFORE));
                if (before != null) {
                    return OwnershipClaimResult.claimed(instance, startup, fence, now, expires, snapshot(before));
                }
            }
            Document current = rows.find(Filters.eq("_id", BsonBinaries.uuidBinary(instance))).first();
            return OwnershipClaimResult.denied(instance, startup, fence, now, current == null ? null : snapshot(current));
        } catch (RuntimeException e) {
            throw new BackendException("failed to claim Mongo recovery ownership", e);
        }
    }

    @Override
    public @NotNull OwnershipRenewResult renewOwnership(
            @NotNull StoreId id, @NotNull UUID persistentId, @NotNull UUID startupId,
            @NotNull UUID fence, long ttlMs) throws BackendException {
        try {
            Bson filter = liveToken(new OwnershipToken(persistentId, startupId, fence));
            Document renewed = collection(id).findOneAndUpdate(filter,
                    List.of(new Document("$set", new Document("last_renewed_at_ms", databaseMillis())
                            .append("lease_expires_at_ms", new Document("$add", List.of(databaseMillis(), ttlMs)))),
                            new Document("$unset", "close_reason")),
                    new FindOneAndUpdateOptions().returnDocument(ReturnDocument.AFTER));
            if (renewed != null) {
                return OwnershipRenewResult.renewed(persistentId, startupId, fence,
                        longValue(renewed, "last_renewed_at_ms"), longValue(renewed, "lease_expires_at_ms"));
            }
            return OwnershipRenewResult.lost(persistentId, startupId, fence, dbNowEpochMs());
        } catch (RuntimeException e) {
            throw new BackendException("failed to renew Mongo server ownership", e);
        }
    }

    @Override
    public boolean closeOwnership(
            @NotNull StoreId id, @NotNull UUID persistentId, @NotNull UUID startupId,
            @NotNull UUID fence, @NotNull String reason) throws BackendException {
        try {
            Bson filter = Filters.and(exactToken(new OwnershipToken(persistentId, startupId, fence)),
                    Filters.eq("closed_at_ms", ServerOwnershipSnapshot.OPEN));
            Bson update = new Document("$set", new Document("closed_at_ms", databaseMillis())
                    .append("close_reason", new Document("$literal", reason)));
            return collection(id).updateOne(filter, List.of(update)).getMatchedCount() == 1;
        } catch (RuntimeException e) {
            throw new BackendException("failed to close Mongo server ownership", e);
        }
    }

    /** Preflight for callers routing the three stores to this Mongo v2 backend. */
    public void requireStartupTransactions() throws BackendException {
        startupTransactions.requireStartupTransactions();
    }

    @Override
    public void requireAtomicStartupOperations() throws BackendException {
        requireStartupTransactions();
    }

    @Override
    public void publishStartup(StartupStores stores, @Nullable OwnershipToken owner, ServerStartupRecord row)
            throws BackendException {
        startupTransactions.publish(stores, owner, row);
    }

    @Override
    public long reassignSession(StartupStores stores, @Nullable OwnershipToken owner, EntityOps.ReassignSessionOp op)
            throws BackendException {
        return startupTransactions.reassign(stores, owner, op);
    }

    @Override
    public void publishSession(StartupStores stores, @Nullable OwnershipToken owner, SessionRecord row)
            throws BackendException {
        startupTransactions.publishSession(stores, owner, row);
    }

    @Override
    public long closeStartup(StartupStores stores, @Nullable OwnershipToken owner, UUID instanceId, UUID startupId)
            throws BackendException {
        return startupTransactions.close(stores, owner, instanceId, startupId);
    }

    @Override
    public long repairStaleStartup(StartupStores stores, @Nullable OwnershipToken sweeper, UUID targetInstanceId,
                                   UUID targetStartupId, long staleHeartbeatTtlMs) throws BackendException {
        return startupTransactions.repair(stores, sweeper, targetInstanceId, targetStartupId, staleHeartbeatTtlMs);
    }

    private static Bson exactToken(OwnershipToken token) {
        return Filters.and(
                Filters.eq("_id", BsonBinaries.uuidBinary(token.instanceId())),
                Filters.eq("owner_startup_id", BsonBinaries.uuidBinary(token.startupId())),
                Filters.eq("fence", BsonBinaries.uuidBinary(token.fence())));
    }

    static Bson liveToken(OwnershipToken token) {
        return Filters.and(exactToken(token),
                Filters.eq("closed_at_ms", ServerOwnershipSnapshot.OPEN),
                Filters.type("lease_expires_at_ms", "number"),
                Filters.expr(new Document("$gt", List.of("$lease_expires_at_ms", databaseMillis()))));
    }

    static Document databaseMillis() {
        return new Document("$toLong", "$$NOW");
    }

    @Override
    public @NotNull Optional<ServerOwnershipSnapshot> readOwnership(
            @NotNull StoreId id,
            @NotNull UUID persistentId) throws BackendException {
        try {
            Document doc = collection(id)
                    .find(Filters.eq("_id", BsonBinaries.uuidBinary(persistentId)))
                    .first();
            return Optional.ofNullable(doc == null ? null : snapshot(doc));
        } catch (RuntimeException e) {
            throw new BackendException("failed to read Mongo server ownership", e);
        }
    }

    private @NotNull MongoCollection<Document> collection(@NotNull StoreId id) {
        return db.getCollection(id.name());
    }

    private static @NotNull Document ownershipDocument(
            @NotNull UUID persistentId,
            @NotNull UUID startupId,
            @NotNull UUID fence,
            long now,
            long leaseExpiresAt,
            @NotNull ServerOwnershipMetadata metadata) {
        return new Document("_id", BsonBinaries.uuidBinary(persistentId))
                .append("owner_startup_id", BsonBinaries.uuidBinary(startupId))
                .append("fence", BsonBinaries.uuidBinary(fence))
                .append("lease_expires_at_ms", leaseExpiresAt)
                .append("last_renewed_at_ms", now)
                .append("closed_at_ms", ServerOwnershipSnapshot.OPEN)
                .append("server_name", metadata.serverName())
                .append("hostname", metadata.hostname())
                .append("grim_version", metadata.grimVersion())
                .append("server_version", metadata.serverVersionString());
    }

    private static @NotNull ServerOwnershipSnapshot snapshot(@NotNull Document doc) {
        UUID persistentId = BsonBinaries.toUuid(doc.get("_id"));
        UUID ownerStartupId = BsonBinaries.toUuid(doc.get("owner_startup_id"));
        UUID fence = BsonBinaries.toUuid(doc.get("fence"));
        if (persistentId == null || ownerStartupId == null || fence == null) {
            throw new IllegalStateException("server ownership document has missing UUID fields");
        }
        return new ServerOwnershipSnapshot(
                persistentId,
                ownerStartupId,
                fence,
                longValue(doc, "lease_expires_at_ms"),
                longValue(doc, "last_renewed_at_ms"),
                longValue(doc, "closed_at_ms"),
                doc.getString("close_reason"),
                doc.getString("server_name"),
                doc.getString("hostname"),
                doc.getString("grim_version"),
                doc.getString("server_version"));
    }

    private static long longValue(@NotNull Document doc, @NotNull String field) {
        Number n = doc.get(field, Number.class);
        return n == null ? 0L : n.longValue();
    }
}

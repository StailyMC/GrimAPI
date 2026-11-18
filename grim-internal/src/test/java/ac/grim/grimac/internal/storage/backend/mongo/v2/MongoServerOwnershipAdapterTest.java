package ac.grim.grimac.internal.storage.backend.mongo.v2;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.instance.OwnershipClaimResult;
import ac.grim.grimac.api.storage.instance.OwnershipToken;
import ac.grim.grimac.api.storage.instance.ServerOwnershipMetadata;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.codec.bson.BsonBinaries;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Date;
import java.util.UUID;
import java.util.logging.Logger;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class MongoServerOwnershipAdapterTest {
    private static final StoreId STORE = StoreId.grim("ownership");
    private static final long NOW = 100_000L;
    private final UUID persistent = UUID.randomUUID();
    private final UUID startup = UUID.randomUUID();
    private final UUID fence = UUID.randomUUID();
    private final MongoClient client = mock(MongoClient.class);
    private final MongoDatabase db = mock(MongoDatabase.class);
    @SuppressWarnings("unchecked")
    private final MongoCollection<Document> collection = mock(MongoCollection.class);
    @SuppressWarnings("unchecked")
    private final FindIterable<Document> read = mock(FindIterable.class);

    private MongoServerOwnershipAdapter adapter() {
        when(db.getCollection(STORE.name())).thenReturn(collection);
        when(db.runCommand(any(Bson.class))).thenReturn(new Document("localTime", new Date(NOW)));
        when(collection.find(any(Bson.class))).thenReturn(read);
        return new MongoServerOwnershipAdapter(client, db, Logger.getLogger(getClass().getName()));
    }

    @Test
    void claimReportsTheOwnerReplacedByTheAtomicUpdate() throws Exception {
        MongoServerOwnershipAdapter adapter = adapter();
        Document observed = row(UUID.randomUUID(), UUID.randomUUID(), NOW - 1);
        UUID interveningOwner = UUID.randomUUID();
        UUID interveningFence = UUID.randomUUID();
        Document replaced = row(interveningOwner, interveningFence, NOW - 1)
                .append("closed_at_ms", NOW - 2).append("close_reason", "shutdown");
        when(read.first()).thenReturn(observed);
        when(collection.findOneAndUpdate(any(Bson.class), any(Bson.class), any(FindOneAndUpdateOptions.class)))
                .thenAnswer(call -> {
                    // The DB changes between the pre-read and the atomic update.
                    FindOneAndUpdateOptions options = call.getArgument(2);
                    return options.getReturnDocument() == ReturnDocument.BEFORE
                            ? replaced : row(startup, fence, NOW + 20_000);
                });

        OwnershipClaimResult result = adapter.claimOwnership(STORE, persistent, startup, fence, 20_000, metadata());

        assertTrue(result.claimed());
        assertEquals(interveningOwner, result.previousOwner().ownerStartupId());
        assertEquals(interveningFence, result.previousOwner().fence());
        assertEquals("shutdown", result.previousOwner().closeReason());
        assertEquals(NOW - 2, result.previousOwner().closedAtEpochMs());
    }

    @Test
    void newRowHasNoPreviousOwner() throws Exception {
        OwnershipClaimResult result = adapter().claimOwnership(STORE, persistent, startup, fence, 20_000, metadata());
        assertTrue(result.claimed());
        assertNull(result.previousOwner());
        verify(collection).insertOne(any(Document.class));
    }

    @Test
    void rejectedAtomicUpdateReportsTheCurrentOwner() throws Exception {
        MongoServerOwnershipAdapter adapter = adapter();
        UUID other = UUID.randomUUID();
        when(read.first()).thenReturn(row(other, UUID.randomUUID(), NOW + 20_000));
        OwnershipClaimResult result = adapter.claimOwnership(STORE, persistent, startup, fence, 20_000, metadata());
        assertFalse(result.claimed());
        assertEquals(other, result.currentOwner().ownerStartupId());
        verify(collection, never()).insertOne(any(Document.class));
    }

    @Test
    void recoveryComparesTheExpectedTokenAndServerClockAtTheWrite() throws Exception {
        MongoServerOwnershipAdapter adapter = adapter();
        OwnershipToken expected = new OwnershipToken(persistent, UUID.randomUUID(), UUID.randomUUID());
        OwnershipToken candidate = new OwnershipToken(persistent, startup, fence);
        when(db.runCommand(any(Bson.class), eq(ReadPreference.primary())))
                .thenReturn(new Document("localTime", new Date(NOW)));
        when(collection.withWriteConcern(WriteConcern.MAJORITY)).thenReturn(collection);
        when(collection.withReadPreference(ReadPreference.primary())).thenReturn(collection);
        UUID foreign = UUID.randomUUID();
        when(read.first()).thenReturn(row(foreign, UUID.randomUUID(), NOW - 1));

        var result = adapter.claimRecovery(STORE, candidate, expected, 20_000, metadata());

        assertFalse(result.claimed());
        assertEquals(foreign, result.currentOwner().ownerStartupId());
        ArgumentCaptor<Bson> predicate = ArgumentCaptor.forClass(Bson.class);
        verify(collection).findOneAndUpdate(predicate.capture(), any(Bson.class), any(FindOneAndUpdateOptions.class));
        String filter = predicate.getValue().toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry()).toJson();
        assertTrue(filter.contains("owner_startup_id"));
        assertTrue(filter.contains("fence"));
        assertTrue(filter.contains("$$NOW"));
        assertTrue(filter.contains(java.util.Base64.getEncoder()
                .encodeToString(BsonBinaries.uuidBinary(expected.startupId()).getData())));
        verify(collection, never()).insertOne(any(Document.class));
    }

    @Test
    void standaloneRejectsMultiCollectionOperationsBeforeOpeningAClientSession() throws Exception {
        MongoServerOwnershipAdapter adapter = adapter();
        when(db.runCommand(any(Bson.class), eq(ReadPreference.primary())))
                .thenReturn(new Document("maxWireVersion", 21).append("logicalSessionTimeoutMinutes", 30));
        StartupStores stores = new StartupStores(STORE, StoreId.grim("startups"), StoreId.grim("sessions"));
        var failure = assertThrows(BackendException.class, () -> adapter.closeStartup(stores, null, persistent, startup));
        assertTrue(failure.getMessage().contains("replica set"));
        verify(client, never()).startSession();
        verifyNoInteractions(collection);
    }

    private Document row(UUID owner, UUID token, long expires) {
        return new Document("_id", BsonBinaries.uuidBinary(persistent))
                .append("owner_startup_id", BsonBinaries.uuidBinary(owner))
                .append("fence", BsonBinaries.uuidBinary(token))
                .append("lease_expires_at_ms", expires).append("last_renewed_at_ms", NOW - 20_000)
                .append("closed_at_ms", 0L).append("server_name", "test");
    }

    private static ServerOwnershipMetadata metadata() {
        return new ServerOwnershipMetadata("test", null, null, null);
    }
}

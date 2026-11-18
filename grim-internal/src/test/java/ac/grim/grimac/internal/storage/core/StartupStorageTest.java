package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.backend.BackendV2;
import ac.grim.grimac.api.storage.backend.KindAdapter;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter;
import ac.grim.grimac.api.storage.registry.StoreId;
import ac.grim.grimac.internal.storage.category.V2BuiltinKinds;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class StartupStorageTest {
    @Test
    void aBackendThatCannotFenceSessionWritesIsRefusedWhenStorageIsBound() throws Exception {
        BackendV2 backend = mock(BackendV2.class);
        ServerOwnershipAdapter adapter = mock(ServerOwnershipAdapter.class);
        when(backend.id()).thenReturn("standalone-mongo");
        when(backend.ownershipAdapter()).thenReturn(Optional.of(adapter));
        doReturn(Optional.of(mock(KindAdapter.class))).when(backend).adapterFor(any());
        doThrow(new BackendException("atomic startup operations require a Mongo replica set"))
                .when(adapter).requireAtomicStartupOperations();
        V2Routes routes = V2Routes.builder()
                .register(Categories.SESSION, StoreId.grim("sessions"), V2BuiltinKinds.sessions(), backend)
                .register(Categories.SERVER_STARTUP, StoreId.grim("startups"), V2BuiltinKinds.serverStartups(), backend).build();

        IllegalStateException refused = assertThrows(IllegalStateException.class,
                () -> StartupStorage.resolve(routes, StoreId.grim("ownership")));
        assertTrue(refused.getMessage().contains("replica set"), refused.getMessage());

        doNothing().when(adapter).requireAtomicStartupOperations();
        assertSame(adapter, StartupStorage.resolve(routes, StoreId.grim("ownership")).adapter());
    }
}

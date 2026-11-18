package ac.grim.grimac.internal.storage.core;

import ac.grim.grimac.api.storage.backend.BackendException;
import ac.grim.grimac.api.storage.category.Categories;
import ac.grim.grimac.api.storage.instance.ServerOwnershipAdapter;
import ac.grim.grimac.api.storage.instance.StartupStores;
import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.Nullable;

/** Binds physical startup and session stores to the ownership adapter of the same backend instance. */
@ApiStatus.Internal
public record StartupStorage(ServerOwnershipAdapter adapter, StartupStores stores) {
    public static @Nullable StartupStorage resolve(V2Routes routes, StoreId ownershipStore) {
        var startups = routes.routeFor(Categories.SERVER_STARTUP);
        var sessions = routes.routeFor(Categories.SESSION);
        if (startups == null || sessions == null) return null;
        if (startups.backend() != sessions.backend()) {
            throw new IllegalStateException("Atomic startup and session operations require the same backend instance");
        }
        ServerOwnershipAdapter adapter = sessions.backend().ownershipAdapter().orElseThrow(() ->
                new IllegalStateException("Session backend " + sessions.backend().id() + " has no atomic startup adapter"));
        try {
            adapter.requireAtomicStartupOperations();
        } catch (BackendException e) {
            throw new IllegalStateException("Session backend " + sessions.backend().id()
                    + " cannot fence startup and session writes: " + e.getMessage(), e);
        }
        return new StartupStorage(adapter, new StartupStores(ownershipStore, startups.storeId(), sessions.storeId()));
    }
}

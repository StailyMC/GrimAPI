package ac.grim.grimac.api.storage.instance;

import ac.grim.grimac.api.storage.registry.StoreId;
import org.jetbrains.annotations.ApiStatus;

import java.util.Objects;

/** Physical stores on the same backend instance. A backend must refuse targets it cannot mutate atomically. */
@ApiStatus.Experimental
public record StartupStores(StoreId ownership, StoreId startups, StoreId sessions) {
    public StartupStores {
        Objects.requireNonNull(ownership, "ownership");
        Objects.requireNonNull(startups, "startups");
        Objects.requireNonNull(sessions, "sessions");
        if (ownership.name().equals(startups.name()) || ownership.name().equals(sessions.name()) || startups.name().equals(sessions.name())) {
            throw new IllegalArgumentException("ownership, startups and sessions require distinct stores");
        }
    }
}

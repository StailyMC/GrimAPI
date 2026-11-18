package ac.grim.grimac.api.storage.instance;

import org.jetbrains.annotations.ApiStatus;

import java.util.Objects;
import java.util.UUID;

/** The specific ownership row and grant a mutation is authorized to use. */
@ApiStatus.Experimental
public record OwnershipToken(UUID instanceId, UUID startupId, UUID fence) {
    public OwnershipToken {
        Objects.requireNonNull(instanceId, "instanceId");
        Objects.requireNonNull(startupId, "startupId");
        Objects.requireNonNull(fence, "fence");
    }

    public static OwnershipToken of(ServerOwnershipSnapshot row) {
        return new OwnershipToken(row.persistentId(), row.ownerStartupId(), row.fence());
    }
}

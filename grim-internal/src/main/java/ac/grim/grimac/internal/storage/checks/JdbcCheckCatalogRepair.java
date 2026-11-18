package ac.grim.grimac.internal.storage.checks;

import ac.grim.grimac.api.storage.check.CheckCatalogRepairResult;
import ac.grim.grimac.api.storage.check.CheckCatalogRow;
import org.jetbrains.annotations.ApiStatus;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

@ApiStatus.Internal
public final class JdbcCheckCatalogRepair {

    public static final String STUB_VERSION = "0.0.0-stub";

    private JdbcCheckCatalogRepair() {}

    public static @NotNull CheckCatalogRepairResult run(
            @NotNull JdbcCheckCatalogPersistence.ConnectionFactory connections,
            @NotNull String checksTable,
            @NotNull String violationsTable,
            @NotNull Map<Integer, Integer> legacyToCatalogCheckIds,
            @Nullable String introducedVersionReplacement) throws SQLException {
        try (Connection c = connections.open()) {
            boolean autoCommit = c.getAutoCommit();
            c.setAutoCommit(false);
            try {
                CheckCatalogRepairResult result = run(c, checksTable, violationsTable, legacyToCatalogCheckIds,
                        introducedVersionReplacement);
                c.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                try { c.rollback(); } catch (SQLException ignored) {}
                throw e;
            } finally {
                try { c.setAutoCommit(autoCommit); } catch (SQLException ignored) {}
            }
        }
    }

    /** All statements use the caller's transaction, including its ownership check and rollback. */
    public static CheckCatalogRepairResult run(Connection c, String checksTable, String violationsTable,
            Map<Integer, Integer> mappings, @Nullable String replacement) throws SQLException {
        return new CheckCatalogRepairResult(mappings.size(), rewriteViolationIds(c, violationsTable, mappings),
                repairStubVersions(c, checksTable, replacement));
    }

    /** Apply prewarm inserts and display renames to the same snapshot used to plan the repair. */
    public static void applyPrewarm(Connection c, String checksTable, Map<String, CheckCatalogRow> before,
                                    Iterable<CheckCatalogRow> after) throws SQLException {
        try (PreparedStatement insert = c.prepareStatement("INSERT INTO " + checksTable
                    + " (check_id, stable_key, display, description, introduced_version, introduced_at) VALUES (?, ?, ?, ?, ?, ?)");
             PreparedStatement rename = c.prepareStatement("UPDATE " + checksTable
                    + " SET display = ?, description = ? WHERE stable_key = ? AND check_id = ?")) {
            for (CheckCatalogRow row : after) {
                CheckCatalogRow old = before.get(row.stableKey());
                if (row.equals(old)) continue;
                if (old == null) {
                    insert.setInt(1, row.checkId());
                    insert.setString(2, row.stableKey());
                    insert.setString(3, row.display());
                    insert.setString(4, row.description());
                    insert.setString(5, row.introducedVersion());
                    insert.setLong(6, row.introducedAt());
                    insert.executeUpdate();
                } else {
                    rename.setString(1, row.display());
                    rename.setString(2, row.description());
                    rename.setString(3, old.stableKey());
                    rename.setInt(4, old.checkId());
                    if (rename.executeUpdate() != 1) throw new SQLException("Catalog changed during prewarm: " + old.stableKey());
                }
            }
        }
    }

    private static long rewriteViolationIds(
            Connection c,
            String violationsTable,
            Map<Integer, Integer> legacyToCatalogCheckIds) throws SQLException {
        if (legacyToCatalogCheckIds.isEmpty()) return 0L;
        long updated = 0L;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE " + violationsTable + " SET check_id = ? WHERE check_id = ?")) {
            for (Map.Entry<Integer, Integer> e : legacyToCatalogCheckIds.entrySet()) {
                int legacyId = e.getKey();
                int catalogId = e.getValue();
                if (legacyId == catalogId) continue;
                ps.setInt(1, catalogId);
                ps.setInt(2, legacyId);
                updated += ps.executeUpdate();
            }
        }
        return updated;
    }

    private static long repairStubVersions(
            Connection c,
            String checksTable,
            @Nullable String introducedVersionReplacement) throws SQLException {
        if (introducedVersionReplacement == null || introducedVersionReplacement.isBlank()) return 0L;
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE " + checksTable
                        + " SET introduced_version = ? WHERE introduced_version = ?")) {
            ps.setString(1, introducedVersionReplacement);
            ps.setString(2, STUB_VERSION);
            return ps.executeUpdate();
        }
    }
}

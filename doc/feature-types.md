# Feature Types in GBD - Design Proposal

Status: **Draft / proposal** · Scope: `gbd_core` schema + API, `gbd_server`, `gbd` CLI

## 1. Motivation

GBD has no persisted notion of a feature's type. Values are stored as text and their
type is *guessed per value at display time*. That heuristic is ambiguous:

- `isohash2 = 9026252821384e97` is a hex hash, but Python's `float()` parses it as
  `9026…×10⁹⁷`, so the web table rendered it as a giant `%0.2f` number.
- The same ambiguity affects **sorting** (numeric vs lexical), **comparison** in
  queries, **storage/indexing**, and **validation**.

We patched the display symptom (a stricter `DISPLAY_NUMBER` regex in
[gbd_server/server.py](../gbd_server/server.py)), but the root cause is the missing
type. This proposes explicit, persisted **semantic types**, with the current value
heuristic kept only as a **fallback** for untyped (legacy) features.

## 2. Goals / Non-goals

**Goals**
- One persisted semantic type per feature, used for rendering, sorting, comparison.
- Deterministic behaviour instead of per-value guessing.
- Full backward/forward compatibility with existing published `.db` files.
- Duck typing remains the fallback when a type is absent.
- Incremental adoption - no big-bang migration, no re-import required.

**Non-goals**
- A full relational type/constraint system.
- Changing query-grammar semantics (initially).
- Forcing existing databases to be rewritten.

## 3. Current state (as-is)

- [`FeatureInfo`](../gbd_core/schema.py) is a `@dataclass`: `name`, `database`,
  `table`, `column`, `default`. **No type field.**
- Cardinality is encoded by `default`:
  - `default is None` -> **1:n** feature in a dedicated table
    `{name}(hash TEXT, value TEXT, UNIQUE(hash, value))` - value is `TEXT`.
  - `default is not None` -> **1:1** feature as a column in the central `features`
    table (created untyped).
- Feature enumeration in `features_from_database` **skips tables whose name starts
  with `_`** - a natural, compat-safe place to keep metadata.
- Extractors already know the type: [gbd_init/feature_extractors.py](../gbd_init/feature_extractors.py)
  declares `return_dtype=pl.Boolean` etc. - but it is **not persisted**.
- Display type is inferred per value: `gbd_core.util.is_number` (`float()`),
  `str.isnumeric()`, and the server's `link_field` / `int_field` / `num_field`
  Jinja tests.

## 4. Proposed design

### 4.1 Semantic type taxonomy

A small, closed vocabulary, stored as a string label (forward-compatible):

| type   | rendering            | sort/compare | notes |
| ------ | -------------------- | ------------ | ----- |
| `text` | as-is (default)      | lexical      | opaque string |
| `int`  | as-is, right-aligned | numeric      | |
| `real` | `%0.2f`, right-align | numeric      | |
| `bool` | yes/no               | boolean      | maps `pl.Boolean` |
| `hash` | as-is                | lexical      | never numeric/link |
| `url`  | link                 | lexical      | replaces the `startswith("http")` heuristic |

`hash` is the type that structurally fixes `isohash2`.

### 4.2 Two layers

1. **SQLite affinity** (`INTEGER` / `REAL` / `TEXT`) on the storage column - helps
   sort/compare/index. Backward compatible (affinity is only a hint; existing `TEXT`
   keeps working). *Optional optimization - see §7.*
2. **Semantic-type metadata** (source of truth for display/semantics), stored in a
   reserved, underscore-prefixed table so older GBD ignores it:
   ```sql
   CREATE TABLE _feature_types (
     database TEXT NOT NULL,
     feature  TEXT NOT NULL,
     type     TEXT NOT NULL,
     PRIMARY KEY (database, feature)
   );
   ```

`FeatureInfo` gains an optional `type: str | None`, populated from `_feature_types`
when present, else `None` (-> fallback).

### 4.3 Where types come from

- **Extractors**: persist their declared `return_dtype` on feature creation
  (`pl.Boolean -> bool`, etc.). New features get typed for free.
- **`gbd create --type <t>`**: explicit declaration.
- **CSV import**: optional per-column inference, or a sidecar/header convention.
- **Legacy features**: unset -> duck-typing fallback until re-typed.

### 4.4 Consumers

- **API**: `get_feature_info` / `get_features` return the type.
- **Web** ([gbd_server](../gbd_server/server.py)): the template renders by type;
  when `type is None`, fall back to the current `link_field`/`int_field`/`num_field`
  (`DISPLAY_NUMBER`) heuristics.
- **CLI**: `gbd get` / `gbd info` format and sort by type.
- **Query grammar** (later): numeric comparison for `int`/`real`, lexical for
  `text`/`hash`.

### 4.5 Fallback (duck typing)

Keep `is_number` / `isnumeric` / `DISPLAY_NUMBER` as the behaviour for features with
no declared type. This preserves current behaviour for every existing DB and lets
typing roll out feature-by-feature.

## 5. Compatibility

- **Additive**: new `_`-prefixed metadata table; existing feature tables untouched.
- **Forward** (old GBD, new DB): the metadata table is skipped by enumeration; values
  still read as `TEXT`.
- **Backward** (new GBD, old DB): no `_feature_types` table -> every feature falls back
  to duck typing.
- Published `.db` artifacts stay readable by both.

## 6. Migration plan (phased)

1. **Read path + fallback**: `_feature_types` reader, `FeatureInfo.type`, API surface,
   web/CLI render-by-type-with-fallback. No change to existing DBs.
2. **Write path**: extractors persist `return_dtype` on creation; `gbd create --type`.
3. **Backfill**: `gbd features --infer-types` (opt-in, reversible) - samples values,
   proposes types, lets the user confirm/override, writes metadata.
4. **Optional, later**: enforce affinity, comparison-by-type in the grammar, insert
   validation.

## 7. Risks / open questions

- **Affinity enforcement** for 1:1 columns requires rebuilding the `features` table
  (SQLite can't `ALTER COLUMN` type). Recommendation: **metadata-only first**;
  treat affinity as a later optimization.
- **Multi-valued (1:n) features**: the type describes each element, not the
  `GROUP_CONCAT` string - the display layer must split before formatting.
- **Type vocabulary governance**: closed enum vs open strings (proposed: closed enum,
  stored as string for forward-compat).
- **Conflicts**: declared vs inferred type; how to resolve/warn.
- **CSV in-memory schemas** and metadata-table versioning.

## 8. Recommended first slice

The smallest change that proves the model and structurally fixes `isohash2`:

1. Add the `_feature_types` read + `FeatureInfo.type` + expose via `get_feature_info`.
2. Web template prefers the declared type, falls back to the current heuristics.
3. Persist extractor `return_dtype` for newly created features.

This lands behind the fallback, so it is safe to ship without touching any existing
database.

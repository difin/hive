# VARIANT data type in Hive: status one-pager

*As of Nov 25, 2026. Umbrella: [HIVE-29183](https://issues.apache.org/jira/browse/HIVE-29183). Upstream = apache/hive `master`; downstream = CDH `cdh_main`.*

## What the feature is

`VARIANT` stores semi-structured, JSON-like data in an efficient binary form. It's like JSON, plus date, timestamp, decimal and binary values. The encoding comes from the Parquet spec, and Iceberg v3 tables support it. In Hive a variant is a `(metadata, value)` byte pair, stored as `struct<metadata:binary,value:binary>`. The encoder and decoder (`serde2.variant.Variant`, `VariantBuilder` and `VariantUtil`) are ported from Spark.

**Why not just a JSON string?** With a `STRING` column, every query re-parses the text (for example with `get_json_object`), and all values come back as strings. A variant is parsed once, at write time, and keeps each value's type. With shredding (below), a query can also read only the fields it needs instead of the whole document.

**How you use it:**

- `parse_json(string)` turns JSON text into a variant.
- `to_json(variant)` turns a variant back into JSON text.
- `variant_get(variant, path [, type])` extracts a value. Without `type` it returns the value as a string, with objects and arrays as JSON.
- `try_variant_get(...)` works the same way, but returns NULL instead of failing.

Paths are simple: `$` for the root, `.field` for an object field and `[n]` for an array element, combined as in `$.dealer.ship` or `$.items[0].price`. Wildcards (`[*]`) and recursive descent (`..`) aren't supported. `type` can be `boolean`, `int`, `long`, `double` or `string`.

**Shredding:** when it's enabled, frequently used fields are also written as separate, typed Parquet columns next to the binary variant. Readers can then read just those columns and skip row groups using their statistics. Shredding exists only upstream (see below).

**Example of VARIANT usage:**

```sql
CREATE TABLE car_sales (id INT, record VARIANT) STORED BY ICEBERG TBLPROPERTIES ('format-version'='3');
INSERT INTO car_sales VALUES (1, parse_json('{"dealer":{"ship":"Seattle"},"price":12.50}'));
SELECT variant_get(record, '$.dealer.ship', 'string') FROM car_sales;  -- Seattle
```

**Rules, upstream and downstream:** `VARIANT` is allowed only in Iceberg tables with format version 3; anything else fails with "not supported in non-Iceberg tables". You can't partition by a variant column. Parquet and Avro work; writes to ORC fail ([HIVE-30198](https://issues.apache.org/jira/browse/HIVE-30198)).

**Upstream only:** shredding and pushdown work only with Parquet.

## What works upstream

| JIRA | PR and title | Fix version | What it adds |
|---|---|---|---|
| [HIVE-29184](https://issues.apache.org/jira/browse/HIVE-29184) | [#6069](https://github.com/apache/hive/pull/6069) Iceberg: Basic Variant type support in Hive | 4.2.0 | Basic type: `VARIANT` keyword, Iceberg v3 read and write (Parquet and Avro), and the UDFs `parse_json`, `to_json`, `variant_get(v, path [, type])` and `try_variant_get`. |
| [HIVE-29222](https://issues.apache.org/jira/browse/HIVE-29222) | [#6096](https://github.com/apache/hive/pull/6096) Fix Compilation failure when running with thrift profile by adding VARIANT_TYPE_NAME in serde.thrift | 4.2.0 | Adds `VARIANT_TYPE_NAME` to `serde.thrift`, fixing the build with the thrift profile. |
| [HIVE-29310](https://issues.apache.org/jira/browse/HIVE-29310) | [#6176](https://github.com/apache/hive/pull/6176) Type casting problem in the variant_get UDF | 4.2.0 | `variant_get(v, path, type)` now returns the requested type. Before, filters comparing it with a non-string constant failed. |
| [HIVE-29287](https://issues.apache.org/jira/browse/HIVE-29287) | [#6152](https://github.com/apache/hive/pull/6152) Iceberg: [V3] Variant Shredding support, and [#6234](https://github.com/apache/hive/pull/6234) Addendum: Iceberg: [V3] Variant Shredding support | 4.3.0 | **Shredding on write** (`'variant.shredding.enabled'='true'`). Each writer task infers the shredded schema from the first non-null variant value it sees, so different files can be shredded differently. The addendum adds variants inside structs ([HIVE-29373](https://issues.apache.org/jira/browse/HIVE-29373)), but not inside arrays or maps. Until Iceberg 1.11, the code uses reflection to reach Iceberg's internal shredding hook. |
| [HIVE-29372](https://issues.apache.org/jira/browse/HIVE-29372) | [#6245](https://github.com/apache/hive/pull/6245) Iceberg: [V3] Fix inconsistencies when vectorizartion is enabled in case of variant shredding | 4.3.0 | Interim fix: tables with shredded variants fall back to non-vectorized reads, because vectorized reads gave wrong results or `MALFORMED_VARIANT`. |
| [HIVE-29354](https://issues.apache.org/jira/browse/HIVE-29354) | [#6224](https://github.com/apache/hive/pull/6224) Iceberg: [V3] Projection and Filter Pushdown for Shredded VARIANT Columns | 4.3.0 | **Projection and filter pushdown** for shredded variants: reads only the shredded columns a query needs, and uses `variant_get(...) <op> literal` predicates to skip Parquet row groups. Also adds **vectorized reads of shredded variants** and removes HIVE-29372's fallback. |
| [HIVE-29797](https://issues.apache.org/jira/browse/HIVE-29797) | [#6669](https://github.com/apache/hive/pull/6669) Use Iceberg 1.11.0 public variantShreddingFunc instead of reflection | 4.3.0 | Internal cleanup with no user-facing change: uses Iceberg 1.11's public shredding API instead of reflection. It isn't a sub-task of HIVE-29183. |
| [HIVE-29770](https://issues.apache.org/jira/browse/HIVE-29770) | [#6644](https://github.com/apache/hive/pull/6644) Iceberg: Return VARIANT values as JSON over the HS2 Thrift protocol | 4.3.0 | `SELECT v` returns JSON to JDBC, ODBC, beeline and CLI clients instead of the raw struct bytes. JDBC `DatabaseMetaData.getColumns` reports VARIANT columns as `Types.OTHER` instead of failing with "Unrecognized type name: variant". Query result metadata still reports the column as a struct ([HIVE-30166](https://issues.apache.org/jira/browse/HIVE-30166)). |
| [HIVE-29773](https://issues.apache.org/jira/browse/HIVE-29773) | [#6654](https://github.com/apache/hive/pull/6654) Iceberg: Support CAST(x AS VARIANT) | 4.3.0 | `CAST(x AS VARIANT)`, also written `variant(x)`, keeps the input's type (date, timestamp, binary, decimal, arrays). Timestamps are truncated to microseconds, and structs and maps are rejected. A string input becomes a variant string; use `parse_json` to parse JSON text. |

Upstream uses Iceberg **1.12.0**.

## What works downstream

`cdh_main` has only the **4.2.0 baseline**:

| Downstream commit | JIRA | Upstream PR and title |
|---|---|---|
| `344df13b0b16` CDPD-99287 | [HIVE-29184](https://issues.apache.org/jira/browse/HIVE-29184) | [#6069](https://github.com/apache/hive/pull/6069) Iceberg: Basic Variant type support in Hive |
| `324c1cf86d72` CDPD-99287 | [HIVE-29222](https://issues.apache.org/jira/browse/HIVE-29222) | [#6096](https://github.com/apache/hive/pull/6096) Fix Compilation failure when running with thrift profile by adding VARIANT_TYPE_NAME in serde.thrift |
| `5ada5c6fbe16` CDPD-109030 | [HIVE-29310](https://issues.apache.org/jira/browse/HIVE-29310) | [#6176](https://github.com/apache/hive/pull/6176) Type casting problem in the variant_get UDF |

Downstream uses Iceberg **1.10.1**. On `cdh_main` you can:

- create Iceberg v3 tables with variant columns;
- insert data with `parse_json`;
- read it with `to_json`, `variant_get` and `try_variant_get`;
- filter on `variant_get(...)`. Hive evaluates these filters row by row, with no pushdown or pruning.

**Missing downstream compared with upstream 4.3.0:**

| Missing | Impact on users | Upstream JIRAs |
|---|---|---|
| Shredding | Variants are always stored as one opaque binary blob. | HIVE-29287, its addendum, HIVE-29372 |
| Pushdown, pruning and vectorized shredded reads | Every variant query reads the whole column. | HIVE-29354 |
| JSON results over HS2 | `SELECT v` returns `struct<metadata,value>` bytes; use `to_json(v)` instead. JDBC tools that list columns with `getColumns` fail on tables with a VARIANT column. | HIVE-29770 |
| `CAST(x AS VARIANT)` | Variants can only be built with `parse_json`, which turns dates and timestamps into strings. | HIVE-29773 |

**How the nine upstream JIRAs add up:** 3 are backported, and 5 are missing (HIVE-29287, HIVE-29372, HIVE-29354, HIVE-29770 and HIVE-29773). The ninth, HIVE-29797, isn't listed as missing because it's deliberately skipped: it needs Iceberg 1.11 and has no user-facing change. Counting commits instead gives 10 upstream, because HIVE-29287 has two (the original and its addendum).

**Backport order:** HIVE-29287 and its addendum, then HIVE-29372, then HIVE-29354, then HIVE-29770, then HIVE-29773.

- Skip HIVE-29797: it needs Iceberg 1.11 or later. HIVE-29287's reflection-based code works on 1.10.1: the downstream jar has the private `appenderBuilder` field and the `WriteBuilder.variantShreddingFunc(...)` method it uses.
- HIVE-29354's PR depends on [HIVE-29400](https://issues.apache.org/jira/browse/HIVE-29400), which runs the Hive Iceberg test suite against format v3 and isn't on `cdh_main`. Backport it first, or expect test conflicts.
- HIVE-29773 depends on HIVE-29770.
- HIVE-29770 creates `serde/.../TestSerDeUtils.java`. Until it's backported, later backports that touch that file conflict, as the HIVE-30219 backport did.

## Open upstream issues

22 of the 30 sub-tasks are open and one more has a patch available. Only three have open PRs: HIVE-30170, HIVE-30176 and HIVE-30197. HIVE-29857 and HIVE-29317 each had a PR that was closed without merging. Rows are grouped by type, starting with the issues that have PRs.

| JIRA | PR and title | Status | Type | What it's about |
|---|---|---|---|---|
| [HIVE-30170](https://issues.apache.org/jira/browse/HIVE-30170) | [#6881](https://github.com/apache/hive/pull/6881) VARIANT as a first-class type (open) | Open | Feature | Makes VARIANT a first-class type. Today object inspectors treat it as a struct, so DESCRIBE, CTAS, views and HS2 report `struct<metadata:binary,value:binary>`. Several of the issues below build on it. |
| [HIVE-30176](https://issues.apache.org/jira/browse/HIVE-30176) | [#6879](https://github.com/apache/hive/pull/6879) Iceberg: file-level pruning of shredded VARIANT values (open) | Open | Performance | Skips whole data files using variant bounds. Works around Iceberg 1.12 bugs that are fixed in the unreleased apache/iceberg#18518. |
| [HIVE-30197](https://issues.apache.org/jira/browse/HIVE-30197) | [#6890](https://github.com/apache/hive/pull/6890) Variant: read primitive types TIME, TIMESTAMP_NANOS and TIMESTAMP_NTZ_NANOS (open) | Open | Query failure | Reading variant types 17–19, which Iceberg can write, fails with `UNKNOWN_PRIMITIVE_TYPE_IN_VARIANT`. |
| [HIVE-29857](https://issues.apache.org/jira/browse/HIVE-29857) | [#6743](https://github.com/apache/hive/pull/6743) Iceberg: Fix ROW__POSITION on vectorized VARIANT reads (closed without merging, Nov 8, 2026) | Patch Available | Wrong results | `ROW__POSITION` is wrong when variants are read with vectorization. |
| [HIVE-29317](https://issues.apache.org/jira/browse/HIVE-29317) | [#6185](https://github.com/apache/hive/pull/6185) Iceberg: get_variant() wrongly return null for invalid entries (closed without merging, Nov 17, 2025) | Open | Wrong results | `variant_get` returns NULL instead of an error for invalid path syntax and for values that can't be converted to the requested type. See the next section. |
| [HIVE-30171](https://issues.apache.org/jira/browse/HIVE-30171) | None. JIRA: Semantic equality, hashing and grouping for VARIANT | Open | Wrong results | `=`, `IN`, `GROUP BY`, `DISTINCT` and joins compare raw bytes, so `v = parse_json('{"a":1}')` can match no rows even when the value is the same. |
| [HIVE-30186](https://issues.apache.org/jira/browse/HIVE-30186) | None. JIRA: variant_get(v, path, 'int') wraps around for values outside the int range | Open | Wrong results | `variant_get(v, p, 'int')` wraps out-of-range values; for example 4294967311 becomes 15. |
| [HIVE-30173](https://issues.apache.org/jira/browse/HIVE-30173) | None. JIRA: Iceberg: variant row-group pruning ignores the logical type of shredded values | Open | Wrong results | Shredded tables only. Pruning can give wrong results for decimals, because 12.50 is stored unscaled as 1250. |
| [HIVE-30185](https://issues.apache.org/jira/browse/HIVE-30185) | None. JIRA: Iceberg: vectorized time travel to a snapshot from before a VARIANT column was added fails with MALFORMED_VARIANT | Open | Query failure | Time travel to a snapshot from before a variant column was added fails with vectorization, and returns empty results without it. |
| [HIVE-30198](https://issues.apache.org/jira/browse/HIVE-30198) | None. JIRA: Iceberg: writing a VARIANT column to an ORC table fails (OrcMetrics) | Open | Query failure | Every write to an ORC variant table fails; the gap is in Iceberg's `OrcMetrics`. Workaround: use Parquet or Avro. |
| [HIVE-30187](https://issues.apache.org/jira/browse/HIVE-30187) | None. JIRA: UNION ALL / multi-row VALUES of a variant with a bare NULL fails at compile time | Open | Query failure | `UNION ALL` or multi-row `VALUES` that mix a variant with a bare `NULL` fail at compile time. Workaround: `CAST(NULL AS VARIANT)`, which needs HIVE-29773. |
| [HIVE-30188](https://issues.apache.org/jira/browse/HIVE-30188) | None. JIRA: Vectorized IF/CASE over a struct or VARIANT column fails at run time when the condition is repeating or all true | Open | Query failure | Vectorized `IF` or `CASE` that returns a variant or struct fails. Workaround: turn vectorization off. |
| [HIVE-30189](https://issues.apache.org/jira/browse/HIVE-30189) | None. JIRA: Vectorized ORDER BY on a struct or VARIANT column with LIMIT fails at operator initialization | Open | Query failure | Vectorized `ORDER BY v LIMIT n` fails. Workaround: turn vectorization off. |
| [HIVE-30190](https://issues.apache.org/jira/browse/HIVE-30190) | None. JIRA: Iceberg: join on a struct or VARIANT key fails when dynamic partition pruning leaves IN (RS[n]) in the filter | Open | Query failure | Joins on a variant or struct key fail. Workaround: `hive.tez.dynamic.partition.pruning=false`. |
| [HIVE-30184](https://issues.apache.org/jira/browse/HIVE-30184) | None. JIRA: Iceberg: variant_get(..., 'int'\|'long'\|'double'\|'boolean') IS [NOT] NULL fails on shredded tables | Open | Query failure | `variant_get(..., 'int'\|'long'\|'double'\|'boolean') IS [NOT] NULL` fails on shredded tables. |
| [HIVE-30174](https://issues.apache.org/jira/browse/HIVE-30174) | None. JIRA: Iceberg: a DECIMAL variant predicate leaves an unbindable typed_value reference in the reader filter | Open | Query failure | A DECIMAL `variant_get` predicate may fail on a shredded, partitioned table without vectorization. Not yet reproduced. |
| [HIVE-30166](https://issues.apache.org/jira/browse/HIVE-30166) | None. JIRA: Iceberg: report VARIANT columns as VARIANT to clients that declare VARIANT support | Open | Feature | Report VARIANT as its own column type to HS2 clients that support it; result metadata reports a struct today. |
| [HIVE-30191](https://issues.apache.org/jira/browse/HIVE-30191) | None. JIRA: LLAP external client: VARIANT capability or string fallback (next to HIVE-30166) | Open | Feature | Send VARIANT to older LLAP external clients as JSON strings; older clients fail to read VARIANT columns today. |
| [HIVE-30172](https://issues.apache.org/jira/browse/HIVE-30172) | None. JIRA: Support CAST from VARIANT to primitive types | Open | Feature | Add `CAST(v AS INT/DATE/...)`, with one set of conversion rules shared by CAST and `variant_get`. Even `CAST(v AS STRING)` returns JSON text only once HIVE-30170 is merged, and a reviewer on the HIVE-29773 PR reported unexpected output from it on master. For now, use `to_json(v)` or `variant_get`. |
| [HIVE-30178](https://issues.apache.org/jira/browse/HIVE-30178) | None. JIRA: Constant-fold VARIANT literals | Open | Performance | Compute `parse_json('...')` on constants once instead of per row. |
| [HIVE-30175](https://issues.apache.org/jira/browse/HIVE-30175) | None. JIRA: Iceberg: use Iceberg's native variant shredding (write.parquet.shred-variants) | Open | Performance | Use Iceberg's native shredding instead of Hive's sampler for top-level columns. |
| [HIVE-30177](https://issues.apache.org/jira/browse/HIVE-30177) | None. JIRA: Iceberg: variant row-group pruning never applies to nested paths or struct-nested variants | Open | Performance | Pruning never applies to nested paths like `$.a.b` or to variants inside structs. This loses pruning but doesn't give wrong results. |
| [HIVE-30179](https://issues.apache.org/jira/browse/HIVE-30179) | None. JIRA: JsonSerDe and json_read: read and write VARIANT as JSON | Open | Feature | `JsonSerDe` and `json_read` should read and write variants as JSON instead of base64 metadata and value. |

## Impact of HIVE-29317 on existing functionality

**Short answer:** it doesn't break queries that use valid paths and convertible values. But it does affect how existing `variant_get` calls handle errors, in the same way on upstream and downstream: two kinds of user errors silently return NULL instead of failing the query. `GenericUDFVariantGet` and `GenericUDFTryVariantGet` are the same on `cdh_main` and upstream master apart from the license header; their last functional change, HIVE-29310, is on both.

**Status:** the JIRA is open and assigned to Ayush Saxena. Its PR, [#6185](https://github.com/apache/hive/pull/6185), was closed without merging on Nov 17, 2025, and there's no newer PR. The JIRA description ("for invalid keys, `variant_get()` is supposed to throw") is out of date. In the PR discussion, the maintainers agreed that a missing key should keep returning NULL. What's left of the issue is the second and third rows below.

| Case | Example | Today, on both branches | Intended (PR discussion; same as Spark) |
|---|---|---|---|
| Valid path that matches nothing: missing field, index out of range, or a field on a non-object | `variant_get(parse_json('{"a":1}'), '$.b')` | NULL | NULL, which is already correct. PR #6185 would have made these throw, which is why it was closed. |
| Invalid path syntax | `'$.a['`, `'a.b'`, `'$[abc]'`, `'$..a'` | NULL for every row. A warning goes to the HS2 log. | Error |
| Value that can't be converted to the requested type | `variant_get(parse_json('{"city":"Budapest"}'), '$.city', 'int')` | NULL | Error. `try_variant_get` returns NULL. |
| Malformed variant bytes | Corrupt data | Error | Error, which is already correct. |

**What this means in practice:**

- **Silent empty results:** a typo in a path makes `variant_get` return NULL for every row. A filter such as `WHERE variant_get(v, '$.a[', 'int') = 5` then returns no rows, and aggregates come out NULL, with no error. A string that isn't a number, like `"Budapest"` read as `'int'`, is silently treated as NULL too.
- **`try_variant_get` adds little:** it's meant to be the version that returns NULL on errors, but `variant_get` already returns NULL for both of these cases. The two differ only for malformed data and unsupported target types.
- **Fixing it changes behavior:** queries that get NULL today would start failing. `variant_type.q` on both branches already expects NULL for `variant_get` of the string `'hello world'` as `'int'`, so the fix would also change that expected output. A downstream backport would need to be called out to customers, and `try_variant_get` is the way to keep the NULL behavior.

**A related quirk HIVE-29317 doesn't cover:** `variant_get(v, path, 'boolean')` on a string uses `Boolean.parseBoolean`, so any string other than `"true"`, such as `"yes"` or `"abc"`, becomes `false` rather than NULL or an error.

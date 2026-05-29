# MobilityFlink parity status — MEOS surface audit

Generated 2026-05-29 by `tools/parity/parity_audit.py`.

The MobilityFlink MEOS facade (`org.mobilitydb.flink.meos.MeosOps*`) exposes MEOS C functions to Flink through JMEOS. This audit measures, per type family, the share of the **MEOS public C API** that the facade exposes and that JMEOS binds.

**Headline.** The facade exposes **2296 of 2296 public MEOS functions (100.0%)**. The MEOS public surface (`meos/include/meos*.h`, excluding internal headers) is 2297 functions; JMEOS binds 2296 of them. 0 bindable functions are not exposed (listed in §3).

Coverage is **static**: a function counts as covered when the facade declares a method of the same name and arity that delegates to a JMEOS export.

Per-family runtime behaviour is asserted by `src/test/java/org/mobilitydb/flink/meos/MeosFacadeSmokeTest.java`, which constructs and reads back a value in the core, geo, cbuffer, npoint and pose families through the facade against libmeos. The cbuffer, npoint and pose families require a libmeos built with the extended modules (`-DCBUFFER=ON -DNPOINT=ON -DPOSE=ON -DRGEO=ON`); the stock library carries the core and geo surfaces only.

## 1. Reference surface and method

- **Denominator**: distinct function names declared `extern` in the MEOS public headers `meos.h`, `meos_geo.h`, `meos_cbuffer.h`, `meos_npoint.h`, `meos_pose.h`, `meos_rgeo.h`. Internal headers (`meos_internal*.h`) are excluded.

- **Numerator**: `public static` methods on the generated `MeosOps*` facade whose name is also a `functions.GeneratedFunctions` export in the bundled JMEOS jar.

- **JMEOS jar**: jar/JMEOS.jar exports 2916 static methods.

## 2. Per-family coverage of the public MEOS surface

| Family (header) | Public ∩ JMEOS | Exposed | Missing | Coverage |
|---|---:|---:|---:|---:|
| core temporal / set / span / spanset / tbox (`meos.h`) | 1343 | 1343 | 0 | 100.0% |
| geo (tgeo / tpoint / stbox) (`meos_geo.h`) | 421 | 421 | 0 | 100.0% |
| cbuffer (`meos_cbuffer.h`) | 175 | 175 | 0 | 100.0% |
| npoint (`meos_npoint.h`) | 119 | 119 | 0 | 100.0% |
| pose (`meos_pose.h`) | 101 | 101 | 0 | 100.0% |
| rgeo (`meos_rgeo.h`) | 68 | 68 | 0 | 100.0% |
| h3 / th3index (`meos_h3.h`) | 69 | 69 | 0 | 100.0% |
| **total** | **2296** | **2296** | **0** | **100.0%** |

## 3. Bindable MEOS functions not exposed by the facade

0 functions are present in the public MEOS headers and bound by JMEOS but not generated into the facade:


## 4. MobilityDB SQL-surface cross-check

The facade is also matched against the underlying MEOS C symbol of each addressable `CREATE FUNCTION` in `mobilitydb/sql/**/*.in.sql` (PG-only sections and helper symbols bucketed out; 876 out-of-scope, 113 SQL/plpgsql-composed functions with no single C symbol). Functions the SQL layer implements through the internal MEOS headers (`meos_internal*.h`) are exposed via `MeosOpsSqlSurface`.

- Addressable distinct C symbols: **1336**; bound by JMEOS: **1068**; exposed by the facade: **1068** (100.0% of the JMEOS-bindable SQL surface).

- The remaining **268** addressable C symbols are not exported by JMEOS under the name the SQL layer's extension wrapper uses; the wrapper names differ from the MEOS function names they call.


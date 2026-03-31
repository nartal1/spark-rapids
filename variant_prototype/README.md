# Variant Prototype — Skeleton Code for Resuming Tomorrow

This directory contains **prototype skeleton code** for three integration layers
so you can resume the Variant work tomorrow. None of the files are drop-in
compilable — each targets a separate repository and references APIs you need
to adapt to the actual branch/repo versions. Treat them as well-commented
starting points, not finished implementations.

## Contents

```
variant_prototype/
├── README.md                                      (this file)
├── cudf_tests/
│   └── variant_cast_semantics_test.cpp            ← adds to libcudf branch
├── spark_rapids_jni/
│   ├── java/
│   │   └── VariantUtils.java                      ← adds to spark-rapids-jni
│   └── cpp/
│       └── VariantUtilsJni.cpp                    ← adds to spark-rapids-jni
├── spark_rapids/
│   └── GpuVariantGet.scala                        ← adds to this repo
└── cpu_benchmark/
    ├── variant_cpu_benchmark.py                   ← PySpark CPU baseline harness
    └── README.md                                  ← how to run / interpret
```

## Where each file goes when you resume

| File | Target repo | Path in that repo |
|---|---|---|
| `cudf_tests/variant_cast_semantics_test.cpp` | [rapidsai/cudf (vuule branch)](https://github.com/vuule/cudf/tree/variant-extraction-gpu) | `cpp/tests/io/variant_cast_semantics_test.cpp` (add to `cpp/tests/CMakeLists.txt` under `ConfigureTest(PARQUET_TEST ...)`) |
| `spark_rapids_jni/java/VariantUtils.java` | [NVIDIA/spark-rapids-jni](https://github.com/NVIDIA/spark-rapids-jni) | `src/main/java/com/nvidia/spark/rapids/jni/VariantUtils.java` |
| `spark_rapids_jni/cpp/VariantUtilsJni.cpp` | [NVIDIA/spark-rapids-jni](https://github.com/NVIDIA/spark-rapids-jni) | `src/main/cpp/src/VariantUtilsJni.cpp` (also add to `src/main/cpp/CMakeLists.txt`) |
| `spark_rapids/GpuVariantGet.scala` | this repo | `sql-plugin/src/main/spark400/scala/com/nvidia/spark/rapids/GpuVariantGet.scala` |
| `cpu_benchmark/variant_cpu_benchmark.py` | stays here | runnable standalone; produces markdown table for `libcudf_variant_feedback.md §6.3` |

## Recommended order of work

1. **Start with the cuDF tests.** They compile against the existing branch and
   surface the cast-semantics mismatch concretely. Run them, confirm the
   failures match what we documented in
   [`libcudf_variant_feedback.md §3.6`](../libcudf_variant_feedback.md), and
   use them as the conversation starter with the libcudf team.
2. **Draft the JNI layer** (Java + C++). Even without spark-rapids-jni repo
   access today, you can iterate on the signatures and build the C++ side
   against a local cuDF checkout.
3. **Draft the Scala expression** last. It's the thinnest layer — most of its
   shape mirrors [`GpuGetJsonObject`](../sql-plugin/src/main/scala/com/nvidia/spark/rapids/GpuGetJsonObject.scala) and depends on the Java signatures above.

## Known limitations in the skeletons

- The cuDF test file uses byte arrays constructed by hand; when integrating
  into the branch, substitute the branch's `build_metadata` / `build_object_value`
  helpers if they exist and match the intent.
- The JNI C++ assumes helpers like `cudf::jni::native_column_view` from existing
  spark-rapids-jni code. Exact helper names may differ — check `JSONUtilsJni.cpp`
  in spark-rapids-jni for the current idiom and adapt.
- The Scala expression handles single-segment `$.field` paths only. Chained
  `$.a.b.c` extraction via repeated `getVariantField` calls is stubbed with a
  TODO. Array indexing and wildcards fall back to CPU, per the plan.
- `CudfUnsafeRow.getVariant()` is **not** touched in this prototype — kept as a
  follow-up once target-type coverage expands in cuDF.

## Reference docs in the repo

- [variant_type_primer.md](../variant_type_primer.md) — learning-oriented intro
- [variant_data_type_deep_dive.md](../variant_data_type_deep_dive.md) — full technical reference
- [libcudf_variant_feedback.md](../libcudf_variant_feedback.md) — assessment and open questions

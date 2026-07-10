# Release dependency pins

Reviewed on 2026-07-10 against the official Go download index and the official
GitHub repositories' Release tag objects.

| Dependency | Reviewed release | Immutable value |
| --- | --- | --- |
| Go toolchain | go1.24.13 | `1.24.13`; Linux amd64 archive SHA-256 `1fc94b57134d51669c72173ad5d49fd62afb0f1db9bf3f798fd98ee423f8d730` |
| actions/checkout | v7.0.0 | `9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0` |
| actions/setup-go | v6.5.0 | `924ae3a1cded613372ab5595356fb5720e22ba16` |
| actions/upload-artifact | v7.0.1 | `043fb46d1a93c77aae656e7c1c64a875d1fc6a0a` |
| actions/download-artifact | v8.0.1 | `3e5f45b2cfb9172054b4087a40e8e0b5a5461e7c` |

Do not replace an immutable action SHA with a mutable major tag. Update this
table and the workflows together after reviewing the new official tag object.

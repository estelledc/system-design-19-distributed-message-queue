# Repository instructions

- This is an independent clean-room learning repository for one bounded distributed-message-queue invariant.
- Keep producer acceptance, quorum commit, consumer fetch, handler completion, offset commit, downstream side effect, and human or
  external outcome as separate facts.
- Never add real credentials, identities, message bodies, production traces, proprietary broker data, copied third-party prose,
  diagrams, images, data, or code.
- Preserve unrelated work. Stage exact paths, keep commits single-purpose, and do not add co-author trailers unless requested.
- Infrastructure-dependent conclusions require the matching real service or real multi-process boundary in public CI; mocks cannot
  prove cross-process durability, fencing, or recovery.
- A benchmark is a bounded synthetic observation for its exact fixture and runtime, never a production-capacity or SLA claim.

# Contributing

Contributions should preserve at-least-once semantics, Redis Cluster slot safety, per-queue ordering, and backward compatibility of the Kafka command contract.

## Development

1. Use Java 17 and the checked-in Gradle Wrapper.
2. Start local dependencies with `podman-compose up -d`.
3. Run `./gradlew clean check` before opening a pull request.
4. Update the command contract, architecture docs, and diagrams when behavior or storage layout changes.

## Change requirements

- Add or update focused tests for behavior changes.
- Keep secrets, local environment files, and production data out of commits.
- Document retries, idempotency, Redis key TTL/lifecycle, and rolling-upgrade impacts.
- Keep metric labels bounded and logs free of task payloads or credentials.
- Use conventional commit subjects where practical (for example, `feat: add queue pause command`).

## Pull request checklist

- [ ] Tests and build pass.
- [ ] Kafka contract and Redis key compatibility are documented.
- [ ] Operational behavior and failure modes are documented.
- [ ] No secrets or generated artifacts are included.

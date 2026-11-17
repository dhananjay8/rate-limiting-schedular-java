# Security Policy

## Supported versions

Security fixes are applied to the latest supported release. Upgrade to the newest release before reporting a vulnerability against an older version.

## Reporting a vulnerability

Do not disclose security issues in public issue trackers. Use the repository host's private vulnerability reporting mechanism and include affected versions, impact, and reproduction details without sharing live credentials or customer data.

## Security practices

- Inject Kafka and Redis credentials from a secret manager; never commit secrets.
- Use TLS, least-privilege Kafka ACLs, and Redis ACL users in deployed environments.
- Restrict command-topic producers and allowed dispatch topics at the platform boundary.
- Limit payload sizes and avoid logging payload content.
- Keep Redis persistence protected because it can contain queued task data.

# Contributing

## Conventional Commits

`crdbpgx` heavily relies on [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) which are used to generate changelog. All PR titles must follow the conventional commits standard. Ideally, use _Conventional Commits_ for commits themselves too.

## Tests

Run the unit tests with `make test`.

Run database integration tests separately with `make test-integration`. Set `CRDB_TEST_DATABASE_URL` to a disposable CockroachDB database. For example, start an in-memory local database:

```sh
docker run --rm --name crdbpgx-test --entrypoint /cockroach/cockroach \
  -p 127.0.0.1:26257:26257 \
  cockroachdb/cockroach:v25.1.4 start-single-node --insecure \
  --store=type=mem,size=1GiB --cache=64MiB --max-sql-memory=64MiB \
  --listen-addr=0.0.0.0:26257
```

In another terminal:

```sh
CRDB_TEST_DATABASE_URL='postgres://root@127.0.0.1:26257/defaultdb?sslmode=disable' make test-integration
```

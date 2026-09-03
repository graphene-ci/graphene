# graphene

Репозиторий содержит control plane Graphene и CLI `graphenectl`. Сервер хранит
записи ресурсов, запускает и восстанавливает их долговечные процессы в Temporal,
управляет прогонами, исходниками, ревизиями, секретами, RBAC и подключёнными
агентами.

Инсталляция имеет одну внешнюю точку входа. На одном listener работают
Management и worker API, соединения агентов, прокси к Temporal, приём OTLP,
health probes и прокси container registry. Пользовательский Go SDK находится в
репозитории [`pipeline`](https://github.com/graphene-ci/pipeline), а полная
модель продукта и руководства — в
[`docs`](https://graphene-ci.github.io/docs/).

## Устройство

| Путь | Назначение |
|---|---|
| `cmd/graphene-server` | сборка и запуск сервера |
| `cmd/graphenectl` | универсальный операторский CLI |
| `proto/management` | публичный Management API |
| `internal/services` | реализации Management, worker и agent API |
| `internal/worker` | Temporal worker и регистрация системных процессов |
| `internal/*flow`, `internal/ops` | жизненные циклы записей и внешние эффекты |
| `internal/auth`, `internal/authz` | аутентификация, токены и RBAC |
| `internal/infrastructure` | хранилища blobs, секретов и интеграции |
| `deployments/` | контейнерная сборка dev-инсталляции |

## Локальная разработка

```bash
make configure
make lint
make test
make build
```

Полный dev-контур поднимается командой `make compose-up` и останавливается
`make compose-down`. Это окружение для разработки, не production deployment;
его ограничения и настройка описаны в документации.

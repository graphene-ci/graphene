# AGENTS.md — graphene

Control plane и `graphenectl` Graphene. Текущая модель продукта находится в
`../GRAPHENE.MD`. Публичного пользовательского Go SDK в этом репозитории нет.

## Перед изменением

1. Прочитайте `../GRAPHENE.MD`. Противоречащее ему изменение сначала правит
   продуктовый документ.
2. Перед push обязательны `make lint` и `make test`.

## Правила кода

- Go; код, имена и комментарии — на английском. Коммиты — Conventional Commits
  без `Co-Authored-By`.
- Pipeline-side типы, идентификаторы, wire conventions и определения системных
  ресурсов приходят из репозитория `pipeline`. Публичный Management API
  принадлежит этому репозиторию и живёт в `proto/management`.
- Внешние эффекты flows скрыты за `Ops` interfaces; реализации находятся в
  `internal/`, каждый метод идемпотентен.
- Секреты и большие данные не входят в specs, логи или Temporal history:
  используются только ссылки.

## Границы

- `cmd/` — сборка `graphene-server` и `graphenectl`;
- `proto/management` — публичный Management API, generated code коммитится;
- `internal/` — реализации flows, server worker, API, auth, storage,
  materialization исходников и managed execution; внешних импортируемых пакетов
  здесь нет.

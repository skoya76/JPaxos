# JPaxos Dynatune E2E (Docker Compose)

このディレクトリは、Dynatune 実装の E2E 確認用です。

構成は以下です。

- `replica0`, `replica1`, `replica2`: JPaxos replica (EchoService)
- `client`: GenericClient 実行用

各コンテナには `tc netem` で遅延を注入できます（デフォルト `50ms`）。
送受信の往復で、おおむね `RTT ~= 100ms` を狙う設定です。

## 実行方法

リポジトリルートで次を実行します。

```bash
./scripts/e2e-docker-compose.sh
```

### 主な環境変数

- `NETEM_DELAY_MS` (default: `50`)
- `REQUEST_COUNT` (default: `50`)
- `REQUEST_SIZE` (default: `64`)
- `WAIT_TIMEOUT_SEC` (default: `120`)
- `KEEP_UP=1` を指定すると、成功/失敗後もコンテナを残します

例:

```bash
NETEM_DELAY_MS=50 REQUEST_COUNT=100 KEEP_UP=1 ./scripts/e2e-docker-compose.sh
```

## 検証内容

1. replica の client port 到達確認（クラスタ起動）
2. client からリクエスト送信し、3 replica 全てで実行ログを確認
3. Dynatune の更新ログを確認
   - `Dynatune updated E_t`
   - `Dynatune updated suggested heartbeat interval`
   - `Dynatune applied per-follower heartbeat interval`

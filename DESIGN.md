# Design Notes — wdi-pipeline

このドキュメントは設計上の判断とその理由を記録する。
"なぜそう作ったか" を残すことで、後からの変更・拡張の指針にする。

---

## 1. 1フォルダ = 1CSV = 1job

```
pipelines/
├── gdp_jpn/           → outputs/gdp_jpn.parquet
└── population_latam/  → outputs/population_latam.csv
```

**判断:** pipeline の単位をフォルダで表す。1フォルダは必ず1jobを持ち、1ファイルを出力する。

**理由:**
- フォルダ名を見れば何が出力されるか一目でわかる
- job 同士が独立しているため、1つ追加・削除しても他に影響しない
- `run-all` がフォルダをループするだけで済む（メタ設定ファイル不要）

**却下した代替案:** 1つの manifest に複数 job を並べる（旧 `pipelines/worldbank/` スタイル）。
job が増えるにつれ manifest が肥大化し、依存関係が見えにくくなる。

---

## 2. Manifest の自己管理（output_root はmanifest が持つ）

各 manifest は `defaults.output_root` で自分の出力先を宣言する。

```yaml
defaults:
  output_root: outputs/
```

**判断:** 出力先は manifest の責任。グローバルな環境変数（`WDI_OUTPUT_ROOT`）で上書きしない。

**理由:**
- manifest をコピーすればそのまま動く（ポータブル）
- 「どこに出力されるか」を manifest 1ファイルを読めば完結する
- 環境変数による暗黙の上書きは、manifest だけ見ても動作がわからなくなる

**`--output-root` CLI フラグは残す:** 一時的な override（デバッグ、ステージング環境への出力）は正当なユースケース。
ただし env var ではなく明示的なフラグにとどめる。

---

## 3. フラット出力（subdir なし）

出力ファイルは `output_root` 直下にフラットに置く。サブディレクトリは作らない。

```
outputs/
├── gdp_jpn.parquet
├── gdp_jpn_summary.json
├── population_latam.csv
└── population_latam_summary.json
```

**判断:** `export.subdir` フィールドを削除し、`output_root / filename.ext` に固定する。

**理由:**
- 1フォルダ = 1ファイルであればサブディレクトリに意味がない
- 出力パスの計算ロジックが単純になる
- `run-all` の collision check が `output_root / filename` だけ見ればよい

---

## 4. run-all は CLI の責務、runner は単一 manifest に集中

```
cli.py      ← pipelines/ をループ（run-all の場合）
  └── runner.py  ← 1つの manifest の job を実行する
```

**判断:** 複数 pipeline のオーケストレーション（フォルダ glob、preflight check）は `cli.py` に置く。
`runner.py` は `ManifestConfig` を1つ受け取って実行するだけ。

**理由:**
- `runner.py` のインターフェース（`run_pipeline(manifest)`）が変わらない
- `run` と `run-all` の両方から同じ `run_pipeline()` を呼べる
- `runner.py` に「フォルダ構造の知識」を持ち込まない

---

## 5. run-all の preflight collision check

`run-all` は実行前に全 manifest をロードし、出力パスの衝突を検出してエラーで停止する。

**対象パス:**
- export: `output_root / {filename}.{ext}`
- summary: `output_root / {job_name}_summary.json`

**判断:** 衝突があれば **実行前に** エラーにする（fail fast）。`--allow-overwrite` で無効化できる。

**理由:**
- フラット出力を採用した結果、異なる pipeline が同じファイル名を使う可能性が生まれた
- 後から上書きされたファイルを見ても問題に気づけない（サイレントな不正）
- `--dry-run` / `--probe` 時も check を走らせることで、設定ミスを早期発見できる

---

## 6. Connector-agnostic コア

```python
class ConnectorProtocol(Protocol):
    def discover(self, job) -> DiscoveryResult: ...
    def materialize(self, job, conn: duckdb.DuckDBPyConnection) -> None: ...
```

**判断:** データ取得の実装を connector に隔離し、`runner.py` はプロトコルだけを知る。

**理由:**
- 新しいデータソース（Salesforce、BigQuery 等）を追加するとき、`runner.py` を変更しない
- `_REGISTRY` に1行追加するだけでコアに接続できる
- テストでは `FakeSession` を注入して HTTP を完全に排除できる

**`discover()` と `materialize()` の分離:**
- `discover()` は常に呼ばれ、ネットワーク不要（`job.schema` から列名を返す）
- `materialize()` は full run のみ。probe モードでスキップできる
- この分離により `--probe`（schema 確認だけ）が成立する

---

## 7. Schema の外部化

列定義（name / DuckDB type）はコードではなく YAML ファイルに書く。

```yaml
# schemas/timeseries.yaml
columns:
  - name: year
    type: INTEGER
  - name: value
    type: DOUBLE
```

**判断:** `connector.py` に `_COLUMNS` 定数を持たず、`job.schema.columns` から読む。

**理由:**
- 列の追加・変更がコード変更を伴わない
- DDL 生成（`CREATE TABLE dataset`）と discover の両方が同じ定義を参照する
- manifest と同じフォルダに置くことで pipeline が自己完結する

---

## 8. SQL テンプレート（バインドパラメータではなくリテラル展開）

```sql
WHERE year >= {{min_year}}   -- {{}} → SQL リテラルに展開
```

**判断:** `?` プレースホルダーではなく `{{key}}` をリテラル値に置換する。

**理由:**
- DuckDB は `CREATE OR REPLACE TEMP VIEW _export AS <sql>` 内でバインドパラメータを受け付けない
- `export.py` が VIEW 経由で COPY するアーキテクチャ上、リテラル展開が必要
- パラメータは manifest の `sql.params`（オペレーター制御の設定値）であり、ユーザー入力ではないため SQL injection リスクはない
- `render()` が int/float はそのまま、文字列はシングルクォート + `'` エスケープで展開する

---

## 9. per-job DuckDB 接続の分離

各 job は `duckdb.connect()`（インメモリ）を独立して持ち、export 後に `close()` する。

**判断:** 接続を job 間で共有しない。

**理由:**
- job A の `TABLE dataset` が job B に漏れない
- 1 job が失敗しても次の job に影響しない（`try/finally` で必ず close）
- メモリは job ごとに解放される

---

## 10. Summary は常に書く

`summary.write(output_root)` は dry-run・probe・success・failed いずれの場合も呼ばれる。

**判断:** `runner.py` のループ外（`run_pipeline` 内）で無条件に呼ぶ。

**理由:**
- dry-run でも「どの job がスキップされたか」の記録が残る
- probe でも「発見された列名」が JSON に記録される
- 失敗した job のエラー内容を後から確認できる
- 監査ログとして機能する

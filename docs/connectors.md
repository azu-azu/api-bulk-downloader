# コネクター リファレンス

各コネクターの `source` フィールド設計と利用例をまとめます。

---

## worldbank_indicator

World Bank Indicators API からデータを取得する既存コネクター。

### source.params

| パラメーター | 必須 | 説明 |
|---|---|---|
| `indicator_code` | Yes | World Bank インジケーターコード（例: `NY.GDP.MKTP.CD`） |
| `country_code` | Yes | ISO 3166-1 alpha-3 国コード（例: `JPN`、`BRA`） |

### マニフェスト例

```yaml
# pipelines/gdp_jpn/manifest.yaml
defaults:
  output_root: outputs/
  export_format: csv

jobs:
  - name: gdp_jpn
    connector_params:
      indicator_code: NY.GDP.MKTP.CD
      country_code: JPN
    sql:
      file: queries/timeseries.sql
      params:
        min_year: "2000"
    export:
      filename: gdp_jpn
    schema:
      file: schemas/timeseries.yaml
```

### スキーマファイル例

```yaml
# schemas/timeseries.yaml
columns:
  - name: country_code
    type: VARCHAR
  - name: indicator_code
    type: VARCHAR
  - name: year
    type: INTEGER
  - name: value
    type: DOUBLE
```

### SQL テンプレート例

```sql
-- queries/timeseries.sql
SELECT *
FROM dataset
WHERE year >= {{min_year}}
ORDER BY year;
```

---

## salesforce

> **現時点では未実装（stub）です。** `salesforce.py` は `NotImplementedError` を送出します。

Salesforce REST API / Bulk API から SObject データを取得するコネクター。

### source.params

| パラメーター | 必須 | 説明 |
|---|---|---|
| `object_name` | Yes | 取得対象の SObject 名（例: `Account`、`Contact`） |

### マニフェスト例

```yaml
# pipelines/sf_accounts/manifest.yaml
defaults:
  output_root: outputs/
  export_format: csv

jobs:
  - name: sf_accounts
    connector_params:
      object_name: Account          # 取得対象の SObject 名
    sql:
      file: queries/filter.sql
      params:
        min_created: "2024-01-01"   # DuckDB 側のフィルター用パラメーター
    export:
      filename: sf_accounts
    schema:
      file: schemas/accounts.yaml
```

### スキーマファイル例

```yaml
# schemas/accounts.yaml
columns:
  - name: id
    type: VARCHAR
  - name: name
    type: VARCHAR
  - name: industry
    type: VARCHAR
  - name: annual_revenue
    type: DOUBLE
  - name: created_date
    type: TIMESTAMP
```

### SQL テンプレート例

```sql
-- queries/filter.sql
SELECT *
FROM dataset
WHERE created_date >= TIMESTAMP '{{min_created}}'
ORDER BY name;
```

### 認証情報の扱い

Salesforce の資格情報はマニフェストに書かず、**環境変数** で渡します。
`source.params` には認証情報を含めません。

```dotenv
# .env
SF_USERNAME=user@example.com
SF_PASSWORD=mypassword
SF_SECURITY_TOKEN=xxxxxx
SF_INSTANCE_URL=https://myorg.my.salesforce.com
```

コネクター内で `os.environ` から読み込みます。

### コネクター実装方針（実装時の参考）

| ステップ | 内容 |
|---|---|
| 依存ライブラリ | `simple-salesforce` を `requirements.txt` / `pyproject.toml` に追加 |
| `discover()` | `job.schema.columns` から列名を返す（worldbank_indicator と同じ） |
| `materialize()` | スキーマ列名から SOQL を自動生成 → 全レコード取得 → DuckDB `dataset` に INSERT |
| SOQL 生成例 | `SELECT Id, Name, Industry FROM Account` |
| ページング | `simple-salesforce` の `query_all()` または Bulk API |


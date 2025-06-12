# Databricks料金計算ツール

このアプリケーションは、Databricksワークロードの料金を計算するためのStreamlitアプリです。東京リージョン（ap-northeast-1）に特化し、メモリ最適化インスタンスを中心とした料金計算を提供します。

## 主要機能

### 料金計算機能
- **複数ワークロード対応**: All-Purpose Cluster, Jobs, DLT Advanced, SQL Warehouse Serverless等
- **詳細料金内訳**: Databricks DBU料金とAWS EC2料金を個別計算
- **リアルタイム計算**: 動的テーブルで即座に料金更新
- **Enterprise Advanced**: Databricks最高エディションの料金体系

### インスタンス情報
- **メモリ最適化重視**: R5, R6i, X1e, Z1dシリーズ中心（37種類）
- **詳細スペック表示**: vCPU、メモリ、ネットワーク性能、ストレージ情報
- **ファミリ別分類**: メモリ最適化と汎用のタブ表示
- **料金情報**: 各インスタンスの時間単価表示

### 料金データ管理
- **API自動更新**: Databricks Pricing APIとAWS Pricing API連携
- **手動アップロード**: JSONファイルでのカスタム料金設定
- **更新履歴管理**: 最終更新日時の追跡
- **リアルタイム反映**: 料金更新後の即座な再計算

## 技術スタック

- **フロントエンド**: Streamlit + pandas
- **バックエンド**: Python + Databricks SDK + boto3
- **デプロイ**: Databricks Apps
- **データ管理**: JSON設定ファイル

## ファイル構造

```
databricks-cost-calculator/
├── src/
│   ├── databricks_cost_calculator.py  # メインアプリケーション
│   └── pricing_updater.py             # 料金データ更新機能
├── data/
│   ├── databricks_pricing.json        # Databricks料金データ
│   ├── ec2_pricing.json               # EC2料金データ（東京リージョン）
│   └── ec2_specs.json                 # EC2インスタンススペック情報
├── app.py                             # Databricks Apps エントリーポイント
├── app.yaml                           # Databricks Apps 設定
├── requirements.txt                   # 依存関係
├── DEPLOY.md                          # デプロイ手順書
└── .env.example                       # 環境変数サンプル
```

## 料金体系

### Databricks Enterprise Advanced料金（東京リージョン）
- **All-Purpose Cluster**: $0.65/DBU
- **Jobs Cluster**: $0.18/DBU  
- **DLT Advanced**: $0.48/DBU
- **SQL Warehouse Serverless**: $0.85/DBU
- **Model Serving**: $0.12/DBU
- **Vector Search**: $0.48/DBU
- **AutoML**: $0.65/DBU
- **Unity Catalog**: $0.05/DBU

### EC2インスタンス（東京リージョン）
- **R5シリーズ**: $0.148/時間（r5.large）～ $7.104/時間（r5.24xlarge）
- **R6iシリーズ**: $0.1512/時間（r6i.large）～ $9.6768/時間（r6i.32xlarge）
- **X1eシリーズ**: $1.002/時間（x1e.xlarge）～ $32.064/時間（x1e.32xlarge）
- **Z1dシリーズ**: $0.222/時間（z1d.large）～ $5.328/時間（z1d.12xlarge）

## セットアップ

### Databricks Apps での実行

詳細な手順は [DEPLOY.md](DEPLOY.md) を参照してください。

1. **環境変数設定**:
   ```bash
   export DATABRICKS_USER_EMAIL="your.email@example.com"
   ```

2. **ファイル同期**:
   ```bash
   databricks sync --watch . /Workspace/Users/YOUR_EMAIL/databricks-cost-calculator
   ```

3. **アプリデプロイ**:
   ```bash
   databricks apps deploy databricks-cost-calculator --source-code-path /Workspace/Users/YOUR_EMAIL/databricks-cost-calculator
   ```

### ローカル実行（開発用）

```bash
# 依存関係インストール
pip install -r requirements.txt

# アプリ実行  
streamlit run src/databricks_cost_calculator.py
```

## 使用方法

1. **ワークロード設定**: ワークロードタイプとインスタンスタイプを選択
2. **リソース指定**: ノード数と実行時間を入力
3. **料金計算**: 「料金計算実行」ボタンをクリック
4. **結果確認**: Databricks料金、EC2料金、合計コストを表示

## セキュリティ注意事項

- **個人情報保護**: デプロイ時は`YOUR_EMAIL`を実際のメールアドレスに置き換えてください
- **認証情報管理**: AWS/Databricks認証情報はDatabricks Secretsを使用してください
- **環境変数**: `DATABRICKS_USER_EMAIL`環境変数を設定してください

## 注意事項

- 料金データは参考値です。実際の請求額とは異なる場合があります
- 定期的に公式料金表を確認し、データを更新してください
- 本番環境では適切なアクセス制御を設定してください
- メモリ最適化インスタンス（Rシリーズ）は機械学習・分析ワークロードに最適化されています

## 料金データの更新

アプリ内の「料金データ管理」セクションから：
- **自動更新**: Databricks/AWS APIから最新料金を取得
- **手動アップロード**: カスタムJSONファイルをアップロード
- **更新確認**: 最終更新日時を表示

## ライセンス

このプロジェクトはオープンソースです。
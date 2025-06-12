# Databricks料金計算ツール

このアプリケーションは、Databricksワークロードの料金を計算するためのStreamlitアプリです。

## 機能

- 複数のワークロード（All-Purpose Cluster, Jobs Cluster, DLT, SQL Warehouse）の料金計算
- AWS EC2とDatabricks DBUの料金を個別に計算
- リアルタイムでの料金更新
- 直感的なWebインターフェース

## セットアップ

### Databricks Apps での実行

1. Databricksワークスペースにファイルをアップロード
2. `app.py` をDatabricks Appsとして設定
3. 必要な依存関係をインストール:
   ```bash
   pip install -r requirements.txt
   ```

### ローカル実行（開発用）

```bash
# 依存関係インストール
pip install -r requirements.txt

# アプリ実行
streamlit run app.py
```

## ファイル構造

```
databricks-cost-calculator/
├── src/
│   └── databricks_cost_calculator.py  # メインアプリケーション
├── data/
│   ├── databricks_pricing.json        # Databricks料金データ
│   └── ec2_pricing.json               # EC2料金データ
├── app.py                             # エントリーポイント
├── requirements.txt                   # 依存関係
└── README.md                          # このファイル
```

## 使用方法

1. ワークロードの種類を選択
2. インスタンスタイプとノード数を設定
3. 実行時間とリージョンを指定
4. 「料金計算実行」ボタンをクリック
5. 結果を確認

## 料金データの更新

`data/` フォルダ内のJSONファイルを編集して最新の料金情報に更新してください。

## 注意事項

- 料金データは参考値です。実際の請求額とは異なる場合があります
- 定期的に公式料金表を確認し、データを更新してください
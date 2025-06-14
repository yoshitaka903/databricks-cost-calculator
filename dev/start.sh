#!/bin/bash
# 開発用Streamlitアプリ起動スクリプト

echo "Starting Databricks Cost Calculator (Development Mode)"
echo "======================================================="
echo ""
echo "Browser will open at: http://localhost:8501"
echo "Press Ctrl+C to stop the server"
echo ""

# 依存関係チェック
if ! command -v streamlit &> /dev/null; then
    echo "Error: streamlit is not installed"
    echo "Please install with: pip install -r requirements-dev.txt"
    exit 1
fi

# Streamlitアプリ起動
cd "$(dirname "$0")"
streamlit run streamlit_app.py --server.port=8501
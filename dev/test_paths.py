#!/usr/bin/env python3
"""パス解決のテスト用スクリプト"""

import json
from pathlib import Path

def test_path_resolution():
    print("=== Path Resolution Test ===")
    
    # 現在のファイル位置
    current_file = Path(__file__).absolute()
    print(f"Current file: {current_file}")
    
    # プロジェクトルート
    project_root = current_file.parent.parent
    print(f"Project root: {project_root}")
    
    # データパス
    data_path = project_root / "data"
    print(f"Data path: {data_path}")
    print(f"Data path exists: {data_path.exists()}")
    
    if data_path.exists():
        print(f"Data folder contents: {list(data_path.glob('*'))}")
        
        # Databricks料金ファイル
        databricks_file = data_path / "databricks_pricing.json"
        print(f"Databricks file: {databricks_file}")
        print(f"Databricks file exists: {databricks_file.exists()}")
        
        if databricks_file.exists():
            try:
                with open(databricks_file, 'r') as f:
                    data = json.load(f)
                print(f"Successfully loaded databricks pricing data")
                print(f"Available workload types: {list(data.keys())}")
                
                # all-purposeの確認
                if 'all-purpose' in data:
                    print(f"All-purpose data: {data['all-purpose']}")
                else:
                    print("ERROR: 'all-purpose' not found in data")
                    
            except Exception as e:
                print(f"ERROR loading databricks pricing: {e}")
    else:
        print("ERROR: Data path does not exist")

if __name__ == "__main__":
    test_path_resolution()
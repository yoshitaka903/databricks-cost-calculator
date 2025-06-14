#!/usr/bin/env python3
"""
All Purpose Computeの料金データを処理してJSONファイルを生成する
"""

import json
from pathlib import Path

def create_all_purpose_pricing():
    """All Purpose Computeの料金データを作成"""
    
    # 手動入力データ（提供されたAll Purposeデータから）
    all_purpose_data = {
        # General Purpose Instances - M
        "m4.large": {"dbu_per_hour": 0.400, "rate_per_hour": 0.2600},
        "m4.xlarge": {"dbu_per_hour": 0.750, "rate_per_hour": 0.4875},
        "m4.2xlarge": {"dbu_per_hour": 1.500, "rate_per_hour": 0.9750},
        "m4.4xlarge": {"dbu_per_hour": 3.000, "rate_per_hour": 1.9500},
        "m4.10xlarge": {"dbu_per_hour": 8.000, "rate_per_hour": 5.2000},
        "m4.16xlarge": {"dbu_per_hour": 12.000, "rate_per_hour": 7.8000},
        "m5.large": {"dbu_per_hour": 0.340, "rate_per_hour": 0.2210},
        "m5.xlarge": {"dbu_per_hour": 0.690, "rate_per_hour": 0.4485},
        "m5.2xlarge": {"dbu_per_hour": 1.370, "rate_per_hour": 0.8905},
        "m5.4xlarge": {"dbu_per_hour": 2.740, "rate_per_hour": 1.7810},
        "m5.8xlarge": {"dbu_per_hour": 5.480, "rate_per_hour": 3.5620},
        "m5.12xlarge": {"dbu_per_hour": 8.230, "rate_per_hour": 5.3495},
        "m5.16xlarge": {"dbu_per_hour": 10.960, "rate_per_hour": 7.1240},
        "m5.24xlarge": {"dbu_per_hour": 16.460, "rate_per_hour": 10.6990},
        
        # Memory Optimized - R
        "r5.large": {"dbu_per_hour": 0.450, "rate_per_hour": 0.2925},
        "r5.xlarge": {"dbu_per_hour": 0.900, "rate_per_hour": 0.5850},
        "r5.2xlarge": {"dbu_per_hour": 1.800, "rate_per_hour": 1.1700},
        "r5.4xlarge": {"dbu_per_hour": 3.600, "rate_per_hour": 2.3400},
        "r5.8xlarge": {"dbu_per_hour": 7.200, "rate_per_hour": 4.6800},
        "r5.12xlarge": {"dbu_per_hour": 10.800, "rate_per_hour": 7.0200},
        "r5.16xlarge": {"dbu_per_hour": 14.400, "rate_per_hour": 9.3600},
        "r5.24xlarge": {"dbu_per_hour": 21.600, "rate_per_hour": 14.0400},
        
        # 主要なインスタンスのみ先行実装（後で全部追加）
        "r5d.large": {"dbu_per_hour": 0.450, "rate_per_hour": 0.2925},
        "r5d.xlarge": {"dbu_per_hour": 0.900, "rate_per_hour": 0.5850},
        
        # Compute Optimized - C
        "c5.xlarge": {"dbu_per_hour": 0.610, "rate_per_hour": 0.3965},
        "c5.2xlarge": {"dbu_per_hour": 1.210, "rate_per_hour": 0.7865},
        "c5.4xlarge": {"dbu_per_hour": 2.430, "rate_per_hour": 1.5795},
        
        # Storage Optimized - I
        "i3.large": {"dbu_per_hour": 0.750, "rate_per_hour": 0.4875},
        "i3.xlarge": {"dbu_per_hour": 1.000, "rate_per_hour": 0.6500},
        "i3.2xlarge": {"dbu_per_hour": 2.000, "rate_per_hour": 1.3000},
    }
    
    # 新データ構造
    pricing_structure = {
        "enterprise": {
            "aws": {
                "ap-northeast-1": {
                    "all-purpose": all_purpose_data,
                    "all-purpose-photon": {},  # 後で追加
                    "jobs": {},  # 後で追加
                    "jobs-photon": {},  # 後で追加
                    "dlt-advanced": {},  # 後で追加
                    "dlt-advanced-photon": {}  # 後で追加
                }
            }
        }
    }
    
    return pricing_structure

def main():
    # データ作成
    pricing_data = create_all_purpose_pricing()
    
    # ファイル保存
    output_path = Path(__file__).parent.parent / "data" / "databricks_compute_pricing.json"
    
    with open(output_path, 'w') as f:
        json.dump(pricing_data, f, indent=2)
    
    print(f"✅ All Purpose料金データを保存しました: {output_path}")
    print(f"📊 インスタンス数: {len(pricing_data['enterprise']['aws']['ap-northeast-1']['all-purpose'])}")
    
    # DBU単価の検証（All Purposeは$0.65/DBU）
    print("\n🔍 DBU単価検証:")
    all_purpose = pricing_data['enterprise']['aws']['ap-northeast-1']['all-purpose']
    
    for instance, data in list(all_purpose.items())[:5]:  # 最初の5個をサンプル
        dbu_rate = data['dbu_per_hour']
        actual_rate = data['rate_per_hour']
        expected_rate = dbu_rate * 0.65  # All Purpose = $0.65/DBU
        
        print(f"  {instance}: {dbu_rate} DBU/h × $0.65 = ${expected_rate:.4f} (実際: ${actual_rate:.4f})")

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
EC2料金取得スクリプトのテスト版
少数のインスタンスタイプで動作確認
"""
import sys
import os
from pathlib import Path
sys.path.append(os.path.dirname(__file__))

from fetch_ec2_pricing import EC2PricingFetcher

def main():
    print("🧪 EC2料金取得テスト開始")
    print("=" * 40)
    
    # テスト用の少数インスタンスタイプ
    test_instances = {"m5.large", "m5.xlarge", "r5.large", "c5.large", "t3.micro"}
    
    fetcher = EC2PricingFetcher(region="ap-northeast-1")
    
    print(f"📊 テスト対象: {sorted(test_instances)}")
    
    # 料金取得
    pricing_data = fetcher.get_ec2_pricing(test_instances)
    
    # 結果表示
    print("\n📋 取得結果:")
    print("-" * 60)
    for instance_type, data in sorted(pricing_data.items()):
        price = data["price_per_hour"]
        vcpu = data["vcpu"]
        memory = data["memory"]
        status = "✅" if price > 0 else "❌"
        print(f"{status} {instance_type:12} | ${price:8.4f}/h | {vcpu:2} vCPU | {memory:10}")
    
    # 簡易保存
    script_dir = Path(__file__).parent.parent
    output_file = script_dir / "src" / "data" / "ec2_pricing_test.json"
    fetcher.save_pricing_data(pricing_data, output_file)
    
    print(f"\n💾 テスト結果保存: {output_file}")

if __name__ == "__main__":
    main()
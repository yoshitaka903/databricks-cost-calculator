#!/usr/bin/env python3
"""
AWS API を使用してEC2料金を取得するスクリプト
東京リージョン（ap-northeast-1）のEC2インスタンス料金を取得
"""
import boto3
import json
import time
from typing import Dict, List, Set
from pathlib import Path
import re

class EC2PricingFetcher:
    def __init__(self, region: str = "ap-northeast-1"):
        """
        EC2料金取得クラス
        
        Args:
            region: 対象リージョン（デフォルト: ap-northeast-1 東京）
        """
        self.region = region
        # AWS Pricing APIは常にus-east-1リージョンを使用
        self.pricing_client = boto3.client('pricing', region_name='us-east-1')
        
    def extract_instance_types_from_databricks_data(self, databricks_file: str) -> Set[str]:
        """
        Databricks料金データからインスタンスタイプを抽出
        
        Args:
            databricks_file: Databricks料金データファイルパス
            
        Returns:
            インスタンスタイプのセット
        """
        print("🔍 Databricks料金データからインスタンスタイプを抽出中...")
        
        try:
            with open(databricks_file, 'r') as f:
                data = json.load(f)
            
            instance_types = set()
            
            # 全ワークロードタイプから重複なしでインスタンスタイプを抽出
            workload_data = data["enterprise"]["aws"][self.region]
            
            for workload_type, instances in workload_data.items():
                for instance_type in instances.keys():
                    # インスタンスタイプの形式をチェック（例: m5.large, c5.xlarge）
                    if re.match(r'^[a-z0-9]+\.[a-z0-9-]+$', instance_type):
                        instance_types.add(instance_type)
            
            print(f"✅ {len(instance_types)}個のユニークなインスタンスタイプを発見")
            return instance_types
            
        except Exception as e:
            print(f"❌ Databricksデータ読み込みエラー: {e}")
            return set()
    
    def get_ec2_pricing(self, instance_types: Set[str]) -> Dict[str, Dict[str, float]]:
        """
        指定されたインスタンスタイプのEC2料金を取得
        
        Args:
            instance_types: 取得対象のインスタンスタイプセット
            
        Returns:
            インスタンスタイプ別料金辞書
        """
        print(f"💰 {len(instance_types)}個のインスタンスタイプのEC2料金を取得中...")
        
        pricing_data = {}
        
        # リージョンマッピング（AWS Pricing APIで使用される名前）
        region_mapping = {
            "ap-northeast-1": "Asia Pacific (Tokyo)",
            "us-east-1": "US East (N. Virginia)",
            "us-west-2": "US West (Oregon)",
            "eu-west-1": "Europe (Ireland)"
        }
        
        location = region_mapping.get(self.region, "Asia Pacific (Tokyo)")
        
        for i, instance_type in enumerate(sorted(instance_types), 1):
            print(f"📊 ({i}/{len(instance_types)}) {instance_type} の料金を取得中...")
            
            try:
                # AWS Pricing APIクエリ（適切なフィルターを使用）
                response = self.pricing_client.get_products(
                    ServiceCode='AmazonEC2',
                    Filters=[
                        {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': instance_type},
                        {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': location},
                        {'Type': 'TERM_MATCH', 'Field': 'tenancy', 'Value': 'Shared'},
                        {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'},
                        {'Type': 'TERM_MATCH', 'Field': 'preInstalledSw', 'Value': 'NA'},
                        {'Type': 'TERM_MATCH', 'Field': 'capacitystatus', 'Value': 'Used'}
                    ],
                    MaxResults=10
                )
                
                if response['PriceList']:
                    price_data = json.loads(response['PriceList'][0])
                    
                    # On-Demand料金を抽出
                    terms = price_data.get('terms', {})
                    on_demand = terms.get('OnDemand', {})
                    
                    if on_demand:
                        # 最初のOn-Demand料金を取得
                        first_term = list(on_demand.values())[0]
                        price_dimensions = first_term.get('priceDimensions', {})
                        
                        if price_dimensions:
                            first_dimension = list(price_dimensions.values())[0]
                            price_per_unit = first_dimension.get('pricePerUnit', {})
                            usd_price = float(price_per_unit.get('USD', '0'))
                            
                            # インスタンス情報も取得
                            attributes = price_data.get('product', {}).get('attributes', {})
                            vcpu = attributes.get('vcpu', 'N/A')
                            memory = attributes.get('memory', 'N/A')
                            
                            pricing_data[instance_type] = {
                                "price_per_hour": usd_price,
                                "vcpu": vcpu,
                                "memory": memory,
                                "region": self.region
                            }
                            
                            print(f"   ✅ ${usd_price:.4f}/hour (vCPU: {vcpu}, Memory: {memory})")
                        else:
                            print(f"   ⚠️  料金情報なし")
                            pricing_data[instance_type] = {
                                "price_per_hour": 0.0,
                                "vcpu": "N/A",
                                "memory": "N/A",
                                "region": self.region,
                                "error": "No price dimensions found"
                            }
                else:
                    print(f"   ⚠️  該当する料金プランなし")
                    pricing_data[instance_type] = {
                        "price_per_hour": 0.0,
                        "vcpu": "N/A", 
                        "memory": "N/A",
                        "region": self.region,
                        "error": "No matching price found"
                    }
                
                # API制限を避けるため少し待機
                time.sleep(0.1)
                
            except Exception as e:
                print(f"   ❌ エラー: {e}")
                pricing_data[instance_type] = {
                    "price_per_hour": 0.0,
                    "vcpu": "N/A",
                    "memory": "N/A", 
                    "region": self.region,
                    "error": str(e)
                }
        
        return pricing_data
    
    def save_pricing_data(self, pricing_data: Dict, output_file: str):
        """
        取得した料金データをJSONファイルに保存
        
        Args:
            pricing_data: 料金データ辞書
            output_file: 出力ファイルパス
        """
        print(f"💾 料金データを {output_file} に保存中...")
        
        # 統計情報を計算
        total_instances = len(pricing_data)
        successful_instances = len([v for v in pricing_data.values() if v["price_per_hour"] > 0])
        failed_instances = total_instances - successful_instances
        
        # メタデータ付きで保存
        output_data = {
            "metadata": {
                "region": self.region,
                "total_instances": total_instances,
                "successful_retrievals": successful_instances,
                "failed_retrievals": failed_instances,
                "success_rate": f"{(successful_instances/total_instances*100):.1f}%",
                "generated_at": time.strftime("%Y-%m-%d %H:%M:%S UTC"),
                "currency": "USD"
            },
            "pricing": pricing_data
        }
        
        try:
            # 出力ディレクトリを作成
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            
            print(f"✅ 保存完了!")
            print(f"📊 統計: {successful_instances}/{total_instances} 成功 ({(successful_instances/total_instances*100):.1f}%)")
            
        except Exception as e:
            print(f"❌ 保存エラー: {e}")

def main():
    """メイン実行関数"""
    print("🚀 AWS EC2料金取得スクリプト開始")
    print("=" * 50)
    
    # 設定
    region = "ap-northeast-1"  # 東京リージョン
    script_dir = Path(__file__).parent.parent
    databricks_file = script_dir / "src" / "data" / "databricks_compute_pricing_updated.json"
    output_file = script_dir / "src" / "data" / "ec2_pricing_tokyo.json"
    
    # 料金取得実行
    fetcher = EC2PricingFetcher(region=region)
    
    # 1. Databricksデータからインスタンスタイプを抽出
    instance_types = fetcher.extract_instance_types_from_databricks_data(databricks_file)
    
    if not instance_types:
        print("❌ インスタンスタイプが見つかりません。終了します。")
        return
    
    print(f"📋 対象インスタンスタイプ例: {sorted(list(instance_types))[:10]}")
    
    # ユーザー確認
    user_input = input(f"\n🤔 {len(instance_types)}個のインスタンスタイプの料金を取得しますか？ (y/N): ")
    if user_input.lower() != 'y':
        print("✋ キャンセルされました。")
        return
    
    # 2. EC2料金を取得
    pricing_data = fetcher.get_ec2_pricing(instance_types)
    
    # 3. 結果を保存
    fetcher.save_pricing_data(pricing_data, output_file)
    
    print("\n🎉 EC2料金取得完了!")
    print(f"📁 結果ファイル: {output_file}")

if __name__ == "__main__":
    main()
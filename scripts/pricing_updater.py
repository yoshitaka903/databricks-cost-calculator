import json
import requests
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional
import streamlit as st
from databricks.sdk import WorkspaceClient
import boto3

class PricingDataUpdater:
    """料金データの取得・更新を管理するクラス"""
    
    def __init__(self):
        self.data_path = Path(__file__).parent.parent / "data"
        self.last_update_file = self.data_path / "last_update.json"
        
    def get_last_update_info(self) -> Dict[str, Any]:
        """最終更新情報を取得"""
        try:
            if self.last_update_file.exists():
                with open(self.last_update_file, 'r') as f:
                    return json.load(f)
        except:
            pass
        return {"databricks": None, "ec2": None}
    
    def save_update_info(self, source: str):
        """更新情報を保存"""
        update_info = self.get_last_update_info()
        update_info[source] = datetime.now().isoformat()
        
        with open(self.last_update_file, 'w') as f:
            json.dump(update_info, f, indent=2)
    
    def fetch_databricks_pricing(self) -> Optional[Dict[str, Any]]:
        """Databricks Pricing APIから料金データを取得"""
        try:
            # Databricks SDK を使用して料金データを取得
            w = WorkspaceClient()
            
            # Account APIを使用してList Pricesを取得
            # 注意: 実際のAPIエンドポイントは環境により異なる場合があります
            pricing_data = {
                "all-purpose": {},
                "jobs": {},
                "dlt": {},
                "sql-warehouse": {}
            }
            
            # リージョンとワークロードタイプごとに料金を設定
            regions = ["us-east-1", "us-west-2", "ap-northeast-1", "eu-west-1"]
            
            # 実際のAPI呼び出しの代わりに、サンプルデータを使用
            # 本番環境では、実際のDatabricks Pricing APIを呼び出してください
            base_prices = {
                "all-purpose": 0.55,
                "jobs": 0.15,
                "dlt": 0.30,
                "sql-warehouse": 0.22
            }
            
            region_multipliers = {
                "us-east-1": 1.0,
                "us-west-2": 1.0,
                "ap-northeast-1": 1.1,
                "eu-west-1": 1.1
            }
            
            for workload_type, base_price in base_prices.items():
                pricing_data[workload_type] = {}
                for region in regions:
                    multiplier = region_multipliers.get(region, 1.0)
                    pricing_data[workload_type][region] = {
                        "dbu_per_hour": 1.0,
                        "price_per_dbu": base_price * multiplier
                    }
            
            return pricing_data
            
        except Exception as e:
            st.error(f"Databricks料金データの取得に失敗: {str(e)}")
            return None
    
    def fetch_ec2_pricing(self) -> Optional[Dict[str, Any]]:
        """AWS Pricing APIからEC2料金データを取得"""
        try:
            # AWS Pricing APIクライアントを作成
            # 注意: us-east-1リージョンでのみPricing APIが利用可能
            pricing_client = boto3.client('pricing', region_name='us-east-1')
            
            instance_types = ["m5.large", "m5.xlarge", "m5.2xlarge", "r5.large", "r5.xlarge", "c5.large", "c5.xlarge"]
            regions = ["us-east-1", "us-west-2", "ap-northeast-1", "eu-west-1"]
            
            pricing_data = {}
            
            for instance_type in instance_types:
                pricing_data[instance_type] = {}
                
                for region in regions:
                    try:
                        # AWS Pricing APIでEC2料金を取得
                        response = pricing_client.get_products(
                            ServiceCode='AmazonEC2',
                            Filters=[
                                {
                                    'Type': 'TERM_MATCH',
                                    'Field': 'instanceType',
                                    'Value': instance_type
                                },
                                {
                                    'Type': 'TERM_MATCH',
                                    'Field': 'location',
                                    'Value': self._get_aws_location_name(region)
                                },
                                {
                                    'Type': 'TERM_MATCH',
                                    'Field': 'tenancy',
                                    'Value': 'Shared'
                                },
                                {
                                    'Type': 'TERM_MATCH',
                                    'Field': 'operatingSystem',
                                    'Value': 'Linux'
                                }
                            ]
                        )
                        
                        if response['PriceList']:
                            price_item = json.loads(response['PriceList'][0])
                            terms = price_item['terms']['OnDemand']
                            
                            for term_key in terms:
                                price_dimensions = terms[term_key]['priceDimensions']
                                for pd_key in price_dimensions:
                                    price_per_hour = float(price_dimensions[pd_key]['pricePerUnit']['USD'])
                                    pricing_data[instance_type][region] = {
                                        "price_per_hour": price_per_hour
                                    }
                                    break
                                break
                        else:
                            # APIで取得できない場合は、デフォルト値を使用
                            pricing_data[instance_type][region] = {
                                "price_per_hour": self._get_default_ec2_price(instance_type, region)
                            }
                            
                    except Exception as e:
                        st.warning(f"EC2料金取得エラー ({instance_type}, {region}): {str(e)}")
                        pricing_data[instance_type][region] = {
                            "price_per_hour": self._get_default_ec2_price(instance_type, region)
                        }
            
            return pricing_data
            
        except Exception as e:
            st.error(f"AWS EC2料金データの取得に失敗: {str(e)}")
            return None
    
    def _get_aws_location_name(self, region: str) -> str:
        """AWSリージョンコードをLocation名に変換"""
        location_mapping = {
            "us-east-1": "US East (N. Virginia)",
            "us-west-2": "US West (Oregon)",
            "ap-northeast-1": "Asia Pacific (Tokyo)",
            "eu-west-1": "Europe (Ireland)"
        }
        return location_mapping.get(region, region)
    
    def _get_default_ec2_price(self, instance_type: str, region: str) -> float:
        """デフォルトのEC2料金を取得（API取得失敗時のフォールバック）"""
        # 現在の料金データから取得
        try:
            with open(self.data_path / "ec2_pricing.json", 'r') as f:
                current_data = json.load(f)
                return current_data.get(instance_type, {}).get(region, {}).get("price_per_hour", 0.1)
        except:
            return 0.1  # デフォルト値
    
    def update_databricks_pricing(self) -> bool:
        """Databricks料金データを更新"""
        new_data = self.fetch_databricks_pricing()
        if new_data:
            with open(self.data_path / "databricks_pricing.json", 'w') as f:
                json.dump(new_data, f, indent=2)
            self.save_update_info("databricks")
            return True
        return False
    
    def update_ec2_pricing(self) -> bool:
        """EC2料金データを更新"""
        new_data = self.fetch_ec2_pricing()
        if new_data:
            with open(self.data_path / "ec2_pricing.json", 'w') as f:
                json.dump(new_data, f, indent=2)
            self.save_update_info("ec2")
            return True
        return False
    
    def update_all_pricing(self) -> Dict[str, bool]:
        """全ての料金データを更新"""
        results = {
            "databricks": self.update_databricks_pricing(),
            "ec2": self.update_ec2_pricing()
        }
        return results
    
    def upload_custom_pricing(self, uploaded_file, data_type: str) -> bool:
        """カスタム料金データをアップロード"""
        try:
            if data_type == "databricks":
                file_path = self.data_path / "databricks_pricing.json"
            elif data_type == "ec2":
                file_path = self.data_path / "ec2_pricing.json"
            else:
                return False
            
            # ファイル内容を検証
            content = json.loads(uploaded_file.getvalue())
            
            # JSONファイルとして保存
            with open(file_path, 'w') as f:
                json.dump(content, f, indent=2)
            
            self.save_update_info(data_type)
            return True
            
        except Exception as e:
            st.error(f"ファイルアップロードエラー: {str(e)}")
            return False
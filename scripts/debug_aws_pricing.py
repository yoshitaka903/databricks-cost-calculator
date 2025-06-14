#!/usr/bin/env python3
"""
AWS Pricing APIのデバッグ用スクリプト
利用可能なフィルターや属性を確認
"""
import boto3
import json

def debug_pricing_api():
    print("🔧 AWS Pricing API デバッグ開始")
    print("=" * 50)
    
    # us-east-1リージョンでPricing APIクライアントを作成
    pricing_client = boto3.client('pricing', region_name='us-east-1')
    
    try:
        # 1. AmazonEC2サービスの属性を確認
        print("1️⃣ AmazonEC2サービスの利用可能な属性を確認...")
        
        response = pricing_client.describe_services(
            ServiceCode='AmazonEC2',
            MaxResults=1
        )
        
        if response['Services']:
            service = response['Services'][0]
            print(f"📋 サービス名: {service['ServiceCode']}")
            
            attribute_names = service.get('AttributeNames', [])
            print(f"📝 利用可能な属性 ({len(attribute_names)}個):")
            for attr in sorted(attribute_names):
                print(f"   - {attr}")
        
        print("\n" + "="*50)
        
        # 2. 特定のインスタンスタイプでサンプル検索
        print("2️⃣ サンプルインスタンス (m5.large) の情報を取得...")
        
        sample_response = pricing_client.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': 'm5.large'}
            ],
            MaxResults=3
        )
        
        print(f"📊 検索結果: {len(sample_response['PriceList'])}件")
        
        for i, price_item in enumerate(sample_response['PriceList'][:2]):
            price_data = json.loads(price_item)
            attributes = price_data.get('product', {}).get('attributes', {})
            
            print(f"\n🔍 結果 {i+1}:")
            print(f"   Location: {attributes.get('location', 'N/A')}")
            print(f"   Instance Type: {attributes.get('instanceType', 'N/A')}")
            print(f"   Operating System: {attributes.get('operatingSystem', 'N/A')}")
            print(f"   Tenancy: {attributes.get('tenancy', 'N/A')}")
            print(f"   Pre Installed SW: {attributes.get('preInstalledSw', 'N/A')}")
            print(f"   Capacity Status: {attributes.get('capacitystatus', 'N/A')}")
            print(f"   License Model: {attributes.get('licenseModel', 'N/A')}")
            
            # 料金情報も確認
            terms = price_data.get('terms', {})
            on_demand = terms.get('OnDemand', {})
            if on_demand:
                first_term = list(on_demand.values())[0]
                price_dimensions = first_term.get('priceDimensions', {})
                if price_dimensions:
                    first_dimension = list(price_dimensions.values())[0]
                    price_per_unit = first_dimension.get('pricePerUnit', {})
                    usd_price = price_per_unit.get('USD', 'N/A')
                    print(f"   💰 On-Demand Price: ${usd_price}/hour")
        
        print("\n" + "="*50)
        
        # 3. 東京リージョンでの検索
        print("3️⃣ 東京リージョン向けフィルターテスト...")
        
        tokyo_response = pricing_client.get_products(
            ServiceCode='AmazonEC2',
            Filters=[
                {'Type': 'TERM_MATCH', 'Field': 'instanceType', 'Value': 'm5.large'},
                {'Type': 'TERM_MATCH', 'Field': 'location', 'Value': 'Asia Pacific (Tokyo)'},
                {'Type': 'TERM_MATCH', 'Field': 'operatingSystem', 'Value': 'Linux'}
            ],
            MaxResults=3
        )
        
        print(f"📊 東京リージョン検索結果: {len(tokyo_response['PriceList'])}件")
        
        if tokyo_response['PriceList']:
            price_data = json.loads(tokyo_response['PriceList'][0])
            attributes = price_data.get('product', {}).get('attributes', {})
            
            print("🗾 東京リージョンの結果:")
            for key, value in sorted(attributes.items()):
                print(f"   {key}: {value}")
            
            # 料金も表示
            terms = price_data.get('terms', {})
            on_demand = terms.get('OnDemand', {})
            if on_demand:
                first_term = list(on_demand.values())[0]
                price_dimensions = first_term.get('priceDimensions', {})
                if price_dimensions:
                    first_dimension = list(price_dimensions.values())[0]
                    price_per_unit = first_dimension.get('pricePerUnit', {})
                    usd_price = price_per_unit.get('USD', 'N/A')
                    print(f"   💰 Price: ${usd_price}/hour")
        
    except Exception as e:
        print(f"❌ エラー: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_pricing_api()
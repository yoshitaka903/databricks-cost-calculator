#!/usr/bin/env python3
"""
Test script to verify the new pricing data integration
"""
import sys
import json
from pathlib import Path

# Add src directory to path
sys.path.append(str(Path(__file__).parent / "src"))

from app import DatabricksCostCalculator, WorkloadConfig

def test_pricing_data_loading():
    """Test that the new pricing data loads correctly"""
    print("🔍 Testing pricing data loading...")
    
    calculator = DatabricksCostCalculator()
    
    # Check if the new data structure is loaded
    try:
        region_data = calculator.pricing_data["databricks"]["enterprise"]["aws"]["ap-northeast-1"]
        print("✅ New pricing data structure loaded successfully")
        
        # Check available workload types
        workload_types = list(region_data.keys())
        print(f"📋 Available workload types: {workload_types}")
        
        # Check sample instance data for each workload type
        for workload_type in workload_types:
            workload_data = region_data[workload_type]
            sample_instances = list(workload_data.keys())[:3]  # First 3 instances
            print(f"   {workload_type}: {len(workload_data)} instances (sample: {sample_instances})")
            
            # Check data structure for first instance
            if sample_instances:
                instance_data = workload_data[sample_instances[0]]
                required_fields = ["dbu_per_hour", "rate_per_hour"]
                has_all_fields = all(field in instance_data for field in required_fields)
                print(f"   {sample_instances[0]}: {instance_data} (valid: {has_all_fields})")
        
        return True
    except KeyError as e:
        print(f"❌ Failed to load new pricing data structure: {e}")
        return False

def test_workload_calculations():
    """Test calculations with different workload types"""
    print("\n🧮 Testing workload calculations...")
    
    calculator = DatabricksCostCalculator()
    
    test_configs = [
        {
            "name": "All-Purpose Standard",
            "config": WorkloadConfig(
                workload_type="all-purpose",
                workload_purpose="データ分析",
                driver_instance_type="m5.large",
                executor_instance_type="m5.xlarge",
                executor_node_count=2,
                daily_hours=8,
                monthly_hours=160,
                photon_enabled=False,
                region="ap-northeast-1"
            )
        },
        {
            "name": "All-Purpose Photon",
            "config": WorkloadConfig(
                workload_type="all-purpose",
                workload_purpose="データ分析（Photon）",
                driver_instance_type="m5.large",
                executor_instance_type="m5.xlarge",
                executor_node_count=2,
                daily_hours=8,
                monthly_hours=160,
                photon_enabled=True,
                region="ap-northeast-1"
            )
        },
        {
            "name": "Jobs Compute",
            "config": WorkloadConfig(
                workload_type="jobs",
                workload_purpose="バッチジョブ",
                driver_instance_type="m5.large",
                executor_instance_type="m5.xlarge",
                executor_node_count=2,
                daily_hours=8,
                monthly_hours=160,
                photon_enabled=False,
                region="ap-northeast-1"
            )
        },
        {
            "name": "DLT Advanced",
            "config": WorkloadConfig(
                workload_type="dlt-advanced",
                workload_purpose="データパイプライン",
                driver_instance_type="m5.large",
                executor_instance_type="m5.xlarge",
                executor_node_count=2,
                daily_hours=8,
                monthly_hours=160,
                photon_enabled=False,
                region="ap-northeast-1"
            )
        },
        {
            "name": "Jobs Photon",
            "config": WorkloadConfig(
                workload_type="jobs",
                workload_purpose="バッチジョブ（Photon）",
                driver_instance_type="m5d.large",
                executor_instance_type="m5d.xlarge",
                executor_node_count=2,
                daily_hours=8,
                monthly_hours=160,
                photon_enabled=True,
                region="ap-northeast-1"
            )
        },
        {
            "name": "DLT Advanced Photon",
            "config": WorkloadConfig(
                workload_type="dlt-advanced",
                workload_purpose="データパイプライン（Photon）",
                driver_instance_type="m5d.large",
                executor_instance_type="m5d.xlarge",
                executor_node_count=2,
                daily_hours=8,
                monthly_hours=160,
                photon_enabled=True,
                region="ap-northeast-1"
            )
        }
    ]
    
    for test_case in test_configs:
        print(f"\n📊 {test_case['name']}:")
        try:
            result = calculator.calculate_databricks_cost(test_case['config'])
            
            print(f"   💰 DBU Price: ${result['dbu_price']:.3f}")
            print(f"   🚗 Driver: {result['driver_dbu_rate']} DBU/h")
            print(f"   ⚡ Executor: {result['executor_dbu_rate']} DBU/h")
            print(f"   📈 Total Monthly DBU: {result['total_dbu_monthly']:.1f}")
            print(f"   💵 Monthly Cost: ${result['databricks_cost_monthly']:.2f}")
            
            # Validate result
            if result['databricks_cost_monthly'] > 0:
                print("   ✅ Calculation successful")
            else:
                print("   ⚠️  Zero cost - check pricing data")
                
        except Exception as e:
            print(f"   ❌ Calculation failed: {e}")

def main():
    """Main test function"""
    print("🚀 Testing Databricks Cost Calculator Pricing Integration\n")
    
    # Test 1: Data loading
    data_loaded = test_pricing_data_loading()
    
    if data_loaded:
        # Test 2: Calculations
        test_workload_calculations()
    else:
        print("❌ Skipping calculation tests due to data loading issues")
    
    print("\n✨ Testing completed!")

if __name__ == "__main__":
    main()
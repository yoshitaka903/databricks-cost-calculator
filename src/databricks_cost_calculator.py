import streamlit as st
import pandas as pd
import json
from typing import Dict, List, Any
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from .pricing_updater import PricingDataUpdater

@dataclass
class WorkloadConfig:
    workload_type: str
    instance_type: str
    node_count: int
    hours: float
    region: str

class DatabricksCostCalculator:
    def __init__(self):
        self.pricing_data = self.load_pricing_data()
        
    def load_pricing_data(self) -> Dict[str, Any]:
        """料金データを読み込み"""
        try:
            data_path = Path(__file__).parent.parent / "data"
            
            with open(data_path / "databricks_pricing.json", "r") as f:
                databricks_pricing = json.load(f)
            
            with open(data_path / "ec2_pricing.json", "r") as f:
                ec2_pricing = json.load(f)
                
            with open(data_path / "ec2_specs.json", "r") as f:
                ec2_specs = json.load(f)
                
            return {
                "databricks": databricks_pricing,
                "ec2": ec2_pricing,
                "ec2_specs": ec2_specs
            }
        except FileNotFoundError:
            st.error("料金データファイルが見つかりません。データファイルを配置してください。")
            return {"databricks": {}, "ec2": {}, "ec2_specs": {}}
    
    def calculate_databricks_cost(self, config: WorkloadConfig) -> float:
        """Databricks料金計算"""
        try:
            workload_pricing = self.pricing_data["databricks"][config.workload_type]
            region_pricing = workload_pricing[config.region]
            dbu_rate = region_pricing["dbu_per_hour"]
            dbu_price = region_pricing["price_per_dbu"]
            
            total_dbu = dbu_rate * config.node_count * config.hours
            return total_dbu * dbu_price
        except KeyError:
            st.warning(f"料金データが見つかりません: {config.workload_type}, {config.region}")
            return 0.0
    
    def calculate_ec2_cost(self, config: WorkloadConfig) -> float:
        """EC2料金計算"""
        try:
            instance_pricing = self.pricing_data["ec2"][config.instance_type]
            region_pricing = instance_pricing[config.region]
            hourly_rate = region_pricing["price_per_hour"]
            
            return hourly_rate * config.node_count * config.hours
        except KeyError:
            st.warning(f"EC2料金データが見つかりません: {config.instance_type}, {config.region}")
            return 0.0
    
    def calculate_total_cost(self, config: WorkloadConfig) -> Dict[str, float]:
        """総料金計算"""
        databricks_cost = self.calculate_databricks_cost(config)
        ec2_cost = self.calculate_ec2_cost(config)
        total_cost = databricks_cost + ec2_cost
        
        return {
            "databricks_cost": databricks_cost,
            "ec2_cost": ec2_cost,
            "total_cost": total_cost
        }

def main():
    st.set_page_config(
        page_title="Databricks料金計算ツール",
        page_icon="💰",
        layout="wide"
    )
    
    st.title("💰 Databricks料金計算ツール")
    st.markdown("複数のDatabricksワークロード料金を計算し、コスト分析を行います。")
    
    calculator = DatabricksCostCalculator()
    updater = PricingDataUpdater()
    
    # サイドバー設定
    with st.sidebar:
        st.header("⚙️ アプリ設定")
        default_region = st.selectbox(
            "リージョン",
            ["ap-northeast-1"],
            index=0
        )
        
        currency = st.selectbox("通貨", ["USD", "JPY"], index=0)
        
        st.markdown("---")
        st.markdown("### 📊 料金データ管理")
        
        # 最終更新情報表示
        last_update = updater.get_last_update_info()
        if last_update["databricks"]:
            st.success(f"Databricks料金: {last_update['databricks'][:19]}更新")
        else:
            st.warning("Databricks料金: 未更新")
            
        if last_update["ec2"]:
            st.success(f"EC2料金: {last_update['ec2'][:19]}更新")
        else:
            st.warning("EC2料金: 未更新")
        
        st.markdown("---")
        
        # 料金データ更新機能
        st.subheader("🔄 料金データ更新")
        
        col1_sidebar, col2_sidebar = st.columns(2)
        
        with col1_sidebar:
            if st.button("Databricks更新", use_container_width=True):
                with st.spinner("Databricks料金を更新中..."):
                    if updater.update_databricks_pricing():
                        st.success("更新完了!")
                        st.rerun()
                    else:
                        st.error("更新失敗")
        
        with col2_sidebar:
            if st.button("EC2更新", use_container_width=True):
                with st.spinner("EC2料金を更新中..."):
                    if updater.update_ec2_pricing():
                        st.success("更新完了!")
                        st.rerun()
                    else:
                        st.error("更新失敗")
        
        if st.button("🔄 全て更新", type="primary", use_container_width=True):
            with st.spinner("全料金データを更新中..."):
                results = updater.update_all_pricing()
                if all(results.values()):
                    st.success("全データ更新完了!")
                    st.rerun()
                else:
                    st.warning(f"一部更新失敗: {results}")
        
        st.markdown("---")
        
        # 手動ファイルアップロード
        st.subheader("📁 手動アップロード")
        
        data_type = st.selectbox(
            "データタイプ",
            ["databricks", "ec2"],
            key="upload_type"
        )
        
        uploaded_file = st.file_uploader(
            "JSONファイルを選択",
            type=["json"],
            key="pricing_upload"
        )
        
        if uploaded_file and st.button("アップロード", use_container_width=True):
            if updater.upload_custom_pricing(uploaded_file, data_type):
                st.success("ファイルアップロード完了!")
                st.rerun()
            else:
                st.error("アップロード失敗")
    
    # メインコンテンツ
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📝 ワークロード設定")
        
        # インスタンススペック情報表示
        st.subheader("📊 EC2インスタンススペック情報")
        
        # メモリ最適化インスタンス（Rシリーズ）を優先表示
        if calculator.pricing_data.get("ec2_specs"):
            specs_data = calculator.pricing_data["ec2_specs"]
            
            # インスタンスファミリ別に整理
            memory_optimized = {k: v for k, v in specs_data.items() if k.startswith(('r5', 'r6i', 'x1e', 'z1d'))}
            general_purpose = {k: v for k, v in specs_data.items() if k.startswith('m5')}
            
            tab1, tab2 = st.tabs(["🧠 メモリ最適化 (推奨)", "⚖️ 汎用"])
            
            with tab1:
                st.markdown("**メモリ最適化インスタンス - 機械学習・分析ワークロードに最適**")
                for instance_type, specs in memory_optimized.items():
                    with st.expander(f"**{instance_type}** - {specs['family']} - vCPU: {specs['vcpu']}, メモリ: {specs['memory_gb']}GB"):
                        col_spec1, col_spec2 = st.columns(2)
                        with col_spec1:
                            st.write(f"**vCPU:** {specs['vcpu']}")
                            st.write(f"**メモリ:** {specs['memory_gb']} GB")
                        with col_spec2:
                            st.write(f"**ネットワーク:** {specs['network_performance']}")
                            st.write(f"**ストレージ:** {specs['storage']}")
                        
                        # 料金表示
                        if instance_type in calculator.pricing_data["ec2"]:
                            price = calculator.pricing_data["ec2"][instance_type]["ap-northeast-1"]["price_per_hour"]
                            st.write(f"**料金:** ${price}/時間")
            
            with tab2:
                st.markdown("**汎用インスタンス - バランス型ワークロード向け**")
                for instance_type, specs in general_purpose.items():
                    with st.expander(f"**{instance_type}** - {specs['family']} - vCPU: {specs['vcpu']}, メモリ: {specs['memory_gb']}GB"):
                        col_spec1, col_spec2 = st.columns(2)
                        with col_spec1:
                            st.write(f"**vCPU:** {specs['vcpu']}")
                            st.write(f"**メモリ:** {specs['memory_gb']} GB")
                        with col_spec2:
                            st.write(f"**ネットワーク:** {specs['network_performance']}")
                            st.write(f"**ストレージ:** {specs['storage']}")
                        
                        # 料金表示
                        if instance_type in calculator.pricing_data["ec2"]:
                            price = calculator.pricing_data["ec2"][instance_type]["ap-northeast-1"]["price_per_hour"]
                            st.write(f"**料金:** ${price}/時間")
        
        st.markdown("---")

        # 初期データフレーム
        if "workloads_df" not in st.session_state:
            st.session_state.workloads_df = pd.DataFrame({
                "ワークロードタイプ": ["all-purpose"],
                "インスタンスタイプ": ["r5.large"],
                "ノード数": [2],
                "実行時間(時間)": [8.0],
                "リージョン": [default_region]
            })
        
        # データエディター
        workloads_df = st.data_editor(
            st.session_state.workloads_df,
            column_config={
                "ワークロードタイプ": st.column_config.SelectboxColumn(
                    "ワークロードタイプ",
                    options=["all-purpose", "jobs", "dlt-advanced", "sql-warehouse-serverless", "model-serving", "vector-search", "workflow-orchestration", "feature-store", "automl", "unity-catalog"],
                    required=True
                ),
                "インスタンスタイプ": st.column_config.SelectboxColumn(
                    "インスタンスタイプ",
                    options=["r5.large", "r5.xlarge", "r5.2xlarge", "r5.4xlarge", "r5.8xlarge", "r5.12xlarge", "r5.16xlarge", "r5.24xlarge", "r6i.large", "r6i.xlarge", "r6i.2xlarge", "r6i.4xlarge", "r6i.8xlarge", "r6i.12xlarge", "r6i.16xlarge", "r6i.24xlarge", "r6i.32xlarge", "x1e.xlarge", "x1e.2xlarge", "x1e.4xlarge", "x1e.8xlarge", "x1e.16xlarge", "x1e.32xlarge", "z1d.large", "z1d.xlarge", "z1d.2xlarge", "z1d.3xlarge", "z1d.6xlarge", "z1d.12xlarge", "m5.large", "m5.xlarge", "m5.2xlarge", "m5.4xlarge", "m5.8xlarge", "m5.12xlarge", "m5.16xlarge", "m5.24xlarge"],
                    required=True
                ),
                "ノード数": st.column_config.NumberColumn(
                    "ノード数",
                    min_value=1,
                    max_value=100,
                    step=1,
                    required=True
                ),
                "実行時間(時間)": st.column_config.NumberColumn(
                    "実行時間(時間)",
                    min_value=0.1,
                    max_value=24.0,
                    step=0.1,
                    format="%.1f",
                    required=True
                ),
                "リージョン": st.column_config.SelectboxColumn(
                    "リージョン",
                    options=["ap-northeast-1"],
                    required=True
                )
            },
            num_rows="dynamic",
            use_container_width=True
        )
        
        st.session_state.workloads_df = workloads_df
        
        # 計算ボタン
        if st.button("💰 料金計算実行", type="primary", use_container_width=True):
            if len(workloads_df) > 0:
                st.session_state.calculation_results = []
                
                for index, row in workloads_df.iterrows():
                    config = WorkloadConfig(
                        workload_type=row["ワークロードタイプ"],
                        instance_type=row["インスタンスタイプ"],
                        node_count=int(row["ノード数"]),
                        hours=float(row["実行時間(時間)"]),
                        region=row["リージョン"]
                    )
                    
                    costs = calculator.calculate_total_cost(config)
                    costs["workload_name"] = f"{config.workload_type}-{index+1}"
                    st.session_state.calculation_results.append(costs)
                
                st.success("料金計算が完了しました！")
            else:
                st.warning("ワークロードを追加してください。")
    
    with col2:
        st.header("📊 計算結果")
        
        if "calculation_results" in st.session_state and st.session_state.calculation_results:
            results = st.session_state.calculation_results
            
            # 合計コスト表示
            total_databricks = sum(r["databricks_cost"] for r in results)
            total_ec2 = sum(r["ec2_cost"] for r in results)
            grand_total = total_databricks + total_ec2
            
            st.metric("総Databricks料金", f"${total_databricks:,.2f}")
            st.metric("総EC2料金", f"${total_ec2:,.2f}")
            st.metric("総合計", f"${grand_total:,.2f}")
            
            st.markdown("---")
            
            # 詳細結果テーブル
            st.subheader("詳細内訳")
            results_df = pd.DataFrame(results)
            
            if not results_df.empty:
                results_display = results_df[[
                    "workload_name", "databricks_cost", "ec2_cost", "total_cost"
                ]].copy()
                results_display.columns = ["ワークロード", "Databricks料金", "EC2料金", "合計"]
                
                # 通貨フォーマット
                for col in ["Databricks料金", "EC2料金", "合計"]:
                    results_display[col] = results_display[col].apply(lambda x: f"${x:,.2f}")
                
                st.dataframe(results_display, use_container_width=True)
                
                # チャート表示
                st.subheader("料金内訳チャート")
                chart_data = pd.DataFrame({
                    "Databricks": [total_databricks],
                    "EC2": [total_ec2]
                })
                st.bar_chart(chart_data)
        else:
            st.info("料金計算を実行してください。")
    
    # フッター
    st.markdown("---")
    st.markdown(
        "💡 **ヒント**: 料金データは定期的に更新され、実際の請求額と異なる場合があります。"
    )

if __name__ == "__main__":
    main()
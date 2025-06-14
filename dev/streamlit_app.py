import streamlit as st
import pandas as pd
import json
import os
from typing import Dict, List, Any
from dataclasses import dataclass
from pathlib import Path

@dataclass
class WorkloadConfig:
    workload_type: str
    workload_purpose: str
    driver_instance_type: str
    executor_instance_type: str
    executor_node_count: int
    daily_hours: float
    monthly_hours: float
    photon_enabled: bool
    region: str
    # SQL Warehouse専用フィールド
    sql_warehouse_size: str = ""
    sql_warehouse_cluster_count: int = 1

class DatabricksCostCalculator:
    def __init__(self):
        self.pricing_data = self.load_pricing_data()
        
    def load_pricing_data(self) -> Dict[str, Any]:
        """料金データを読み込み"""
        try:
            # 開発用：絶対パスでdataフォルダを参照
            current_file = Path(__file__).absolute()
            project_root = current_file.parent.parent
            data_path = project_root / "data"
            
            # ファイル存在確認
            databricks_file = data_path / "databricks_pricing.json"
            ec2_file = data_path / "ec2_pricing.json"
            specs_file = data_path / "ec2_specs.json"
            
            # デバッグ情報（開発時のみ）
            # st.sidebar.write(f"Data path: {data_path.absolute()}")
            # st.sidebar.write(f"Files exist: DB={databricks_file.exists()}, EC2={ec2_file.exists()}, Specs={specs_file.exists()}")
            
            if not databricks_file.exists():
                st.error(f"Databricksファイルが見つかりません: {databricks_file}")
                return {"databricks": {}, "ec2": {}, "ec2_specs": {}}
            
            with open(databricks_file, "r") as f:
                databricks_pricing = json.load(f)
            
            with open(ec2_file, "r") as f:
                ec2_pricing = json.load(f)
                
            with open(specs_file, "r") as f:
                ec2_specs = json.load(f)
            
            # インスタンス別DBUレートファイル読み込み
            dbu_rates_file = data_path / "instance_dbu_rates.json"
            if dbu_rates_file.exists():
                with open(dbu_rates_file, "r") as f:
                    instance_dbu_rates = json.load(f)
            else:
                st.warning("instance_dbu_rates.json が見つかりません")
                instance_dbu_rates = {}
            
            # SQL Warehouseサイズ別DBUレートファイル読み込み
            sql_warehouse_file = data_path / "sql_warehouse_sizes.json"
            if sql_warehouse_file.exists():
                with open(sql_warehouse_file, "r") as f:
                    sql_warehouse_sizes = json.load(f)
            else:
                st.warning("sql_warehouse_sizes.json が見つかりません")
                sql_warehouse_sizes = {}
                
            return {
                "databricks": databricks_pricing,
                "ec2": ec2_pricing,
                "ec2_specs": ec2_specs,
                "instance_dbu_rates": instance_dbu_rates,
                "sql_warehouse_sizes": sql_warehouse_sizes
            }
        except FileNotFoundError as e:
            st.error(f"料金データファイルが見つかりません: {e}")
            return {"databricks": {}, "ec2": {}, "ec2_specs": {}}
        except Exception as e:
            st.error(f"データ読み込みエラー: {e}")
            return {"databricks": {}, "ec2": {}, "ec2_specs": {}, "instance_dbu_rates": {}}
    
    def calculate_sql_warehouse_cost(self, config: WorkloadConfig) -> Dict[str, float]:
        """SQL Warehouse Serverless料金計算"""
        try:
            workload_pricing = self.pricing_data["databricks"][config.workload_type]
            region_pricing = workload_pricing[config.region]
            dbu_price = region_pricing["price_per_dbu"]
            
            # SQL Warehouseサイズ別DBU消費量を取得
            sql_warehouse_sizes = self.pricing_data["sql_warehouse_sizes"]
            size_dbu_rate = sql_warehouse_sizes.get(config.sql_warehouse_size, {}).get("dbu_per_hour", 0.0)
            
            # クラスタ数とサイズに基づく計算
            total_dbu_per_hour = size_dbu_rate * config.sql_warehouse_cluster_count
            total_dbu_monthly = total_dbu_per_hour * config.monthly_hours
            total_cost_monthly = total_dbu_monthly * dbu_price
            
            # 1日あたりの料金
            daily_cost = total_cost_monthly / 30
            
            return {
                "dbu_price": dbu_price,
                "size_dbu_rate": size_dbu_rate,
                "cluster_count": config.sql_warehouse_cluster_count,
                "total_dbu_per_hour": total_dbu_per_hour,
                "total_dbu_monthly": total_dbu_monthly,
                "databricks_cost_monthly": total_cost_monthly,
                "databricks_cost_daily": daily_cost,
                # インスタンス型の計算と互換性を保つためのダミー値
                "driver_dbu_rate": 0.0,
                "executor_dbu_rate": total_dbu_per_hour,
                "driver_dbu_monthly": 0.0,
                "executor_dbu_monthly": total_dbu_monthly
            }
        except KeyError as e:
            st.warning(f"SQL Warehouse料金データが見つかりません: {e}")
            return {
                "dbu_price": 0.0,
                "size_dbu_rate": 0.0,
                "cluster_count": 0,
                "total_dbu_per_hour": 0.0,
                "total_dbu_monthly": 0.0,
                "databricks_cost_monthly": 0.0,
                "databricks_cost_daily": 0.0,
                "driver_dbu_rate": 0.0,
                "executor_dbu_rate": 0.0,
                "driver_dbu_monthly": 0.0,
                "executor_dbu_monthly": 0.0
            }

    def calculate_databricks_cost(self, config: WorkloadConfig) -> Dict[str, float]:
        """Databricks料金計算（インスタンスタイプ別DBU消費量を考慮）"""
        try:
            workload_pricing = self.pricing_data["databricks"][config.workload_type]
            region_pricing = workload_pricing[config.region]
            dbu_price = region_pricing["price_per_dbu"]
            
            # インスタンス別DBU消費量を取得
            instance_dbu_rates = self.pricing_data["instance_dbu_rates"]
            
            # DriverのDBU消費量（固定1ノード）
            driver_dbu_rate = instance_dbu_rates.get(config.driver_instance_type, {}).get("dbu_per_hour", 0.0)
            # Photon有効時は2倍のDBU消費
            if config.photon_enabled:
                driver_dbu_rate *= 2.0
            driver_dbu_monthly = driver_dbu_rate * 1 * config.monthly_hours
            driver_cost_monthly = driver_dbu_monthly * dbu_price
            
            # ExecutorのDBU消費量
            executor_dbu_rate = instance_dbu_rates.get(config.executor_instance_type, {}).get("dbu_per_hour", 0.0)
            # Photon有効時は2倍のDBU消費
            if config.photon_enabled:
                executor_dbu_rate *= 2.0
            executor_dbu_monthly = executor_dbu_rate * config.executor_node_count * config.monthly_hours
            executor_cost_monthly = executor_dbu_monthly * dbu_price
            
            # 合計
            total_dbu_monthly = driver_dbu_monthly + executor_dbu_monthly
            total_cost_monthly = driver_cost_monthly + executor_cost_monthly
            
            # 1日あたりの料金
            daily_cost = total_cost_monthly / 30  # 月30日として計算
            
            return {
                "dbu_price": dbu_price,
                "driver_dbu_rate": driver_dbu_rate,
                "executor_dbu_rate": executor_dbu_rate,
                "driver_dbu_monthly": driver_dbu_monthly,
                "executor_dbu_monthly": executor_dbu_monthly,
                "total_dbu_monthly": total_dbu_monthly,
                "databricks_cost_monthly": total_cost_monthly,
                "databricks_cost_daily": daily_cost
            }
        except KeyError as e:
            st.error(f"料金データが見つかりません: ワークロード={config.workload_type}, リージョン={config.region}")
            st.error(f"詳細エラー: {e}")
            st.error(f"利用可能なワークロードタイプ: {list(self.pricing_data.get('databricks', {}).keys())}")
            return {
                "dbu_price": 0.0,
                "driver_dbu_rate": 0.0,
                "executor_dbu_rate": 0.0,
                "driver_dbu_monthly": 0.0,
                "executor_dbu_monthly": 0.0,
                "total_dbu_monthly": 0.0,
                "databricks_cost_monthly": 0.0,
                "databricks_cost_daily": 0.0
            }
    
    def get_ec2_hourly_rate(self, instance_type: str, region: str) -> float:
        """EC2インスタンスの時間単価を取得（データがない場合は概算）"""
        try:
            return self.pricing_data["ec2"][instance_type][region]["price_per_hour"]
        except KeyError:
            # データがない場合は、インスタンスサイズから概算
            base_rates = {
                "large": 0.15, "xlarge": 0.30, "2xlarge": 0.60, "3xlarge": 0.90,
                "4xlarge": 1.20, "6xlarge": 1.80, "8xlarge": 2.40, "9xlarge": 2.70,
                "12xlarge": 3.60, "16xlarge": 4.80, "18xlarge": 5.40, "24xlarge": 7.20,
                "32xlarge": 9.60, "48xlarge": 14.40, "metal": 10.00, "medium": 0.08
            }
            
            # インスタンスサイズを抽出
            for size, rate in base_rates.items():
                if size in instance_type:
                    # インスタンスファミリーによる調整
                    if instance_type.startswith(('c5', 'c6', 'c7')):  # Compute optimized
                        return rate * 0.9
                    elif instance_type.startswith(('r5', 'r6', 'r7', 'r8')):  # Memory optimized
                        return rate * 1.2
                    elif instance_type.startswith(('m5', 'm6', 'm7', 'm8')):  # General purpose
                        return rate
                    elif instance_type.startswith(('i3', 'i4')):  # Storage optimized
                        return rate * 1.1
                    elif instance_type.startswith(('p2', 'p3', 'p4', 'p5', 'g4', 'g5')):  # GPU
                        return rate * 3.0
                    else:
                        return rate
            
            # デフォルト値
            return 0.20
    
    def calculate_ec2_cost(self, config: WorkloadConfig) -> Dict[str, float]:
        """EC2料金計算（Driver/Executor別々）"""
        try:
            # Driver料金計算（常に1ノード）
            driver_hourly_rate = self.get_ec2_hourly_rate(config.driver_instance_type, config.region)
            driver_cost_monthly = driver_hourly_rate * 1 * config.monthly_hours
            driver_cost_daily = driver_cost_monthly / 30
            
            # Executor料金計算
            executor_hourly_rate = self.get_ec2_hourly_rate(config.executor_instance_type, config.region)
            executor_cost_monthly = executor_hourly_rate * config.executor_node_count * config.monthly_hours
            executor_cost_daily = executor_cost_monthly / 30
            
            # データが不足している場合の警告
            missing_instances = []
            if config.driver_instance_type not in self.pricing_data.get("ec2", {}):
                missing_instances.append(config.driver_instance_type)
            if config.executor_instance_type not in self.pricing_data.get("ec2", {}):
                missing_instances.append(config.executor_instance_type)
                
            if missing_instances:
                st.info(f"⚠️ EC2料金データが不足：{', '.join(missing_instances)} - 概算値を使用中")
            
            return {
                "driver_cost_monthly": driver_cost_monthly,
                "driver_cost_daily": driver_cost_daily,
                "executor_cost_monthly": executor_cost_monthly,
                "executor_cost_daily": executor_cost_daily,
                "total_ec2_cost_monthly": driver_cost_monthly + executor_cost_monthly,
                "total_ec2_cost_daily": driver_cost_daily + executor_cost_daily
            }
        except Exception as e:
            st.error(f"EC2料金計算エラー: {e}")
            return {
                "driver_cost_monthly": 0.0,
                "driver_cost_daily": 0.0,
                "executor_cost_monthly": 0.0,
                "executor_cost_daily": 0.0,
                "total_ec2_cost_monthly": 0.0,
                "total_ec2_cost_daily": 0.0
            }
    
    def calculate_total_cost(self, config: WorkloadConfig) -> Dict[str, float]:
        """総料金計算"""
        # SQL Warehouse Serverlessの場合は専用計算を使用
        if config.workload_type == "sql-warehouse-serverless":
            databricks_costs = self.calculate_sql_warehouse_cost(config)
            # SQL WarehouseはServerlessなのでEC2料金は0
            ec2_costs = {
                "driver_cost_monthly": 0.0,
                "driver_cost_daily": 0.0,
                "executor_cost_monthly": 0.0,
                "executor_cost_daily": 0.0,
                "total_ec2_cost_monthly": 0.0,
                "total_ec2_cost_daily": 0.0
            }
        else:
            databricks_costs = self.calculate_databricks_cost(config)
            ec2_costs = self.calculate_ec2_cost(config)
        
        # 月間・日間の総料金計算
        total_cost_monthly = databricks_costs["databricks_cost_monthly"] + ec2_costs["total_ec2_cost_monthly"]
        total_cost_daily = databricks_costs["databricks_cost_daily"] + ec2_costs["total_ec2_cost_daily"]
        
        # 全ての情報をマージして返す
        result = {**databricks_costs, **ec2_costs}
        result.update({
            "total_cost_monthly": total_cost_monthly,
            "total_cost_daily": total_cost_daily
        })
        
        return result
    
    def get_instance_spec(self, instance_type: str) -> str:
        """インスタンススペック取得"""
        try:
            spec = self.pricing_data["ec2_specs"][instance_type]
            return f"vCPU: {spec['vcpu']}, メモリ: {spec['memory_gb']}GB"
        except KeyError:
            return "スペック情報なし"

def main():
    st.set_page_config(
        page_title="Databricks料金計算ツール",
        page_icon="💰",
        layout="wide"
    )
    
    st.title("💰 Databricks料金計算ツール")
    st.markdown("複数のDatabricksワークロード料金を計算し、コスト分析を行います。")
    
    # デバッグ情報
    st.sidebar.header("🔍 デバッグ情報")
    st.sidebar.write(f"Current directory: {os.getcwd()}")
    st.sidebar.write(f"Files in current directory: {os.listdir('.')}")
    
    calculator = DatabricksCostCalculator()
    
    # サイドバー設定
    with st.sidebar:
        st.header("⚙️ アプリ設定")
        default_region = st.selectbox(
            "リージョン",
            ["ap-northeast-1"],
            index=0
        )
        
        currency = st.selectbox("通貨", ["USD", "JPY"], index=0)
    
    # メインコンテンツ
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.header("📝 ワークロード設定")
        
        # 計算ボタンを最上部に配置
        if st.button("💰 料金計算実行", type="primary", use_container_width=True, key="calc_button_top"):
            st.session_state.calculation_results = []
            
            # 通常ワークロードの計算
            if "workloads_df" in st.session_state and len(st.session_state.workloads_df) > 0:
                for index, row in st.session_state.workloads_df.iterrows():
                    config = WorkloadConfig(
                        workload_type=row["ワークロードタイプ"],
                        workload_purpose=row["用途"],
                        driver_instance_type=row["Driverインスタンス"],
                        executor_instance_type=row["Executorインスタンス"],
                        executor_node_count=int(row["Executorノード数"]),
                        daily_hours=float(row["1日利用時間"]),
                        monthly_hours=float(row["月間利用時間"]),
                        photon_enabled=bool(row["Photon"]),
                        region=default_region
                    )
                    
                    costs = calculator.calculate_total_cost(config)
                    costs["workload_name"] = f"{config.workload_purpose}"
                    costs["workload_type"] = config.workload_type
                    # 通常ワークロード用の詳細情報を追加
                    costs["photon_enabled"] = config.photon_enabled
                    costs["driver_instance_type"] = config.driver_instance_type
                    costs["executor_instance_type"] = config.executor_instance_type
                    costs["executor_node_count"] = config.executor_node_count
                    st.session_state.calculation_results.append(costs)
            
            # SQL Warehouseワークロードの計算
            if "sql_warehouse_df" in st.session_state and len(st.session_state.sql_warehouse_df) > 0:
                for index, row in st.session_state.sql_warehouse_df.iterrows():
                    config = WorkloadConfig(
                        workload_type="sql-warehouse-serverless",
                        workload_purpose=row["用途"],
                        driver_instance_type="",  # SQL Warehouseでは不要
                        executor_instance_type="",  # SQL Warehouseでは不要
                        executor_node_count=0,  # SQL Warehouseでは不要
                        daily_hours=float(row["1日利用時間"]),
                        monthly_hours=float(row["月間利用時間"]),
                        photon_enabled=False,  # SQL Warehouseでは不要
                        region=default_region,
                        sql_warehouse_size=row["サイズ"],
                        sql_warehouse_cluster_count=int(row["クラスタ数"])
                    )
                    
                    costs = calculator.calculate_total_cost(config)
                    costs["workload_name"] = f"{config.workload_purpose} (SQL Warehouse)"
                    costs["workload_type"] = config.workload_type
                    # SQL Warehouse用の詳細情報を追加
                    costs["sql_warehouse_size"] = config.sql_warehouse_size
                    costs["sql_warehouse_cluster_count"] = config.sql_warehouse_cluster_count
                    costs["photon_enabled"] = False  # SQL Warehouseでは不要
                    st.session_state.calculation_results.append(costs)
            
            if len(st.session_state.calculation_results) > 0:
                st.success("料金計算が完了しました！")
            else:
                st.warning("ワークロードを追加してください。")
        
        st.markdown("---")
        
        # 通常ワークロード用初期データフレーム
        if "workloads_df" not in st.session_state:
            st.session_state.workloads_df = pd.DataFrame({
                "ワークロードタイプ": ["all-purpose"],
                "用途": ["データ分析"],
                "Driverインスタンス": ["r5.large"],
                "Executorインスタンス": ["r5.xlarge"],
                "Executorノード数": [2],
                "1日利用時間": [8],
                "月間利用時間": [160],
                "Photon": [False]
            })
            
        # SQL Warehouse専用初期データフレーム
        if "sql_warehouse_df" not in st.session_state:
            st.session_state.sql_warehouse_df = pd.DataFrame({
                "用途": ["SQL分析"],
                "サイズ": ["Medium"],
                "クラスタ数": [1],
                "1日利用時間": [8],
                "月間利用時間": [160]
            })
        
        # インスタンスタイプオプション（Databricks公式対応インスタンス）
        instance_options = list(calculator.pricing_data.get("instance_dbu_rates", {}).keys())
        if not instance_options:  # フォールバック
            instance_options = ["m4.large", "m5.large", "r5.large", "c5.large"]
        
        # SQL Warehouseサイズオプション
        sql_warehouse_sizes = list(calculator.pricing_data.get("sql_warehouse_sizes", {}).keys())
        if not sql_warehouse_sizes:  # フォールバック
            sql_warehouse_sizes = ["2X-Small", "X-Small", "Small", "Medium", "Large"]
        
        # クラスターベースワークロード設定
        st.subheader("⚡ クラスターベースワークロード設定")
        st.caption("All-Purpose、Jobs、Delta Live Tables等のインスタンス型ワークロード")
        
        # データエディター
        workloads_df = st.data_editor(
            st.session_state.workloads_df,
            column_config={
                "ワークロードタイプ": st.column_config.SelectboxColumn(
                    "ワークロードタイプ",
                    options=["all-purpose", "jobs", "dlt-advanced"],
                    required=True,
                    width="medium"
                ),
                "用途": st.column_config.TextColumn(
                    "用途",
                    help="ワークロードの用途を自由に入力",
                    max_chars=50,
                    width="medium"
                ),
                "Driverインスタンス": st.column_config.SelectboxColumn(
                    "Driverインスタンス",
                    options=instance_options,
                    required=True,
                    width="medium"
                ),
                "Executorインスタンス": st.column_config.SelectboxColumn(
                    "Executorインスタンス",
                    options=instance_options,
                    required=True,
                    width="medium"
                ),
                "Executorノード数": st.column_config.NumberColumn(
                    "Executorノード数",
                    help="ワーカーノード数",
                    min_value=1,
                    max_value=100,
                    step=1,
                    required=True,
                    width="small"
                ),
                "1日利用時間": st.column_config.NumberColumn(
                    "1日利用時間",
                    help="1日あたりの利用時間数",
                    min_value=1,
                    max_value=24,
                    step=1,
                    format="%d",
                    required=True,
                    width="small"
                ),
                "月間利用時間": st.column_config.NumberColumn(
                    "月間利用時間",
                    help="月間総利用時間数",
                    min_value=1,
                    max_value=744,
                    step=1,
                    format="%d",
                    required=True,
                    width="small"
                ),
                "Photon": st.column_config.CheckboxColumn(
                    "Photon",
                    help="Photon有効時はDBU消費量が2倍になります",
                    default=False,
                    width="small"
                ),
            },
            num_rows="dynamic",
            use_container_width=True
        )
        
        st.session_state.workloads_df = workloads_df
        
        # SQL Warehouse専用設定
        st.subheader("🏢 SQL Warehouse Serverless 設定")
        st.caption("サーバーレス型SQLワークロード（サイズベース料金）")
        
        sql_warehouse_df = st.data_editor(
            st.session_state.sql_warehouse_df,
            column_config={
                "用途": st.column_config.TextColumn(
                    "用途",
                    help="SQL Warehouseの用途を自由に入力",
                    max_chars=50,
                    width="medium"
                ),
                "サイズ": st.column_config.SelectboxColumn(
                    "サイズ",
                    help="SQL Warehouseのサイズ",
                    options=sql_warehouse_sizes,
                    required=True,
                    width="medium"
                ),
                "クラスタ数": st.column_config.NumberColumn(
                    "クラスタ数",
                    help="SQL Warehouseクラスタ数",
                    min_value=1,
                    max_value=10,
                    step=1,
                    required=True,
                    width="small"
                ),
                "1日利用時間": st.column_config.NumberColumn(
                    "1日利用時間",
                    help="1日あたりの利用時間数",
                    min_value=1,
                    max_value=24,
                    step=1,
                    format="%d",
                    required=True,
                    width="small"
                ),
                "月間利用時間": st.column_config.NumberColumn(
                    "月間利用時間",
                    help="月間総利用時間数",
                    min_value=1,
                    max_value=744,
                    step=1,
                    format="%d",
                    required=True,
                    width="small"
                ),
            },
            num_rows="dynamic",
            use_container_width=True,
            key="sql_warehouse_editor"
        )
        
        st.session_state.sql_warehouse_df = sql_warehouse_df
        
        # ワークロードタイプ別DBU情報表示
        st.subheader("💎 Databricks DBU情報")
        if calculator.pricing_data.get("databricks"):
            dbu_data = calculator.pricing_data["databricks"]
            
            # ワークロードタイプ別にDBU情報を表示
            workload_tabs = st.tabs(["All-Purpose", "Jobs", "DLT Advanced"])
            
            workload_mapping = {
                "All-Purpose": "all-purpose",
                "Jobs": "jobs", 
                "DLT Advanced": "dlt-advanced"
            }
            
            for i, (tab_name, workload_key) in enumerate(workload_mapping.items()):
                with workload_tabs[i]:
                    if workload_key in dbu_data and default_region in dbu_data[workload_key]:
                        pricing_info = dbu_data[workload_key][default_region]
                        
                        st.metric(
                            "DBU単価", 
                            f"${pricing_info['price_per_dbu']:.3f}",
                            help="1DBUあたりの料金"
                        )
                        
                        # インスタンス別DBU消費量の例
                        st.write("**💡 インスタンス別DBU消費量例:**")
                        instance_dbu_rates = calculator.pricing_data.get("instance_dbu_rates", {})
                        
                        if instance_dbu_rates:
                            example_instances = ["r5.large", "r5.xlarge", "m5.large"]
                            for instance in example_instances:
                                if instance in instance_dbu_rates:
                                    dbu_rate = instance_dbu_rates[instance]["dbu_per_hour"]
                                    hourly_cost = dbu_rate * pricing_info['price_per_dbu']
                                    st.write(f"- {instance}: {dbu_rate} DBU/h → ${hourly_cost:.3f}/h")
                        
                        # 実際の計算例
                        st.write("**🧮 計算例 (Driver: r5.large, Executor: r5.xlarge × 2ノード, 8時間):**")
                        if "r5.large" in instance_dbu_rates and "r5.xlarge" in instance_dbu_rates:
                            driver_dbu = instance_dbu_rates["r5.large"]["dbu_per_hour"]
                            executor_dbu = instance_dbu_rates["r5.xlarge"]["dbu_per_hour"]
                            
                            # 通常の計算
                            total_dbu = (driver_dbu * 1 + executor_dbu * 2) * 8
                            total_cost = total_dbu * pricing_info['price_per_dbu']
                            st.write(f"**通常時:**")
                            st.write(f"- Driver: {driver_dbu} DBU/h × 1ノード × 8h = {driver_dbu * 8:.1f} DBU")
                            st.write(f"- Executor: {executor_dbu} DBU/h × 2ノード × 8h = {executor_dbu * 2 * 8:.1f} DBU")
                            st.write(f"- 合計: {total_dbu:.1f} DBU × ${pricing_info['price_per_dbu']:.3f} = ${total_cost:.2f}")
                            
                            # Photon有効時の計算
                            photon_total_dbu = (driver_dbu * 2 * 1 + executor_dbu * 2 * 2) * 8
                            photon_total_cost = photon_total_dbu * pricing_info['price_per_dbu']
                            st.write(f"**Photon有効時（2倍DBU消費）:**")
                            st.write(f"- Driver: {driver_dbu * 2} DBU/h × 1ノード × 8h = {driver_dbu * 2 * 8:.1f} DBU")
                            st.write(f"- Executor: {executor_dbu * 2} DBU/h × 2ノード × 8h = {executor_dbu * 2 * 2 * 8:.1f} DBU")
                            st.write(f"- 合計: {photon_total_dbu:.1f} DBU × ${pricing_info['price_per_dbu']:.3f} = ${photon_total_cost:.2f}")
                    else:
                        st.warning(f"{tab_name}の料金情報が見つかりません")
        
        # SQL Warehouse サイズ別DBU情報表示
        st.subheader("🏢 SQL Warehouse Serverless サイズ別情報")
        if calculator.pricing_data.get("sql_warehouse_sizes"):
            sql_warehouse_sizes_data = calculator.pricing_data["sql_warehouse_sizes"]
            
            # サイズ別の料金表示
            cols = st.columns(3)
            for i, (size, info) in enumerate(sql_warehouse_sizes_data.items()):
                with cols[i % 3]:
                    dbu_per_hour = info["dbu_per_hour"]
                    hourly_cost = dbu_per_hour * 1.0  # $1.0/DBU
                    st.metric(
                        size,
                        f"{dbu_per_hour} DBU/h",
                        help=f"${hourly_cost:.1f}/h - {info.get('description', '')}"
                    )
            
            # 計算例
            st.write("**🧮 計算例 (Medium サイズ × 2クラスタ × 8時間):**")
            medium_dbu = sql_warehouse_sizes_data.get("Medium", {}).get("dbu_per_hour", 8)
            total_dbu = medium_dbu * 2 * 8
            total_cost = total_dbu * 1.0
            st.write(f"- {medium_dbu} DBU/h × 2クラスタ × 8h = {total_dbu} DBU")
            st.write(f"- {total_dbu} DBU × $1.0 = ${total_cost:.2f}")
        else:
            st.warning("SQL Warehouseサイズ情報が見つかりません")

        # 選択されたインスタンスのスペック表示
        if len(workloads_df) > 0:
            st.subheader("📊 選択インスタンスのスペック情報")
            
            for index, row in workloads_df.iterrows():
                with st.expander(f"行 {index + 1}: {row['用途']} のスペック"):
                    col_spec1, col_spec2 = st.columns(2)
                    
                    with col_spec1:
                        st.write("**🚗 Driver (固定1ノード):**")
                        driver_spec = calculator.get_instance_spec(row['Driverインスタンス'])
                        st.write(f"- {row['Driverインスタンス']}: {driver_spec}")
                        
                        # DriverのDBU情報
                        instance_dbu_rates = calculator.pricing_data.get("instance_dbu_rates", {})
                        if row['Driverインスタンス'] in instance_dbu_rates:
                            driver_dbu = instance_dbu_rates[row['Driverインスタンス']]["dbu_per_hour"]
                            st.write(f"- DBU消費量: {driver_dbu} DBU/h")
                        
                    with col_spec2:
                        st.write("**⚡ Executor:**")
                        executor_spec = calculator.get_instance_spec(row['Executorインスタンス'])
                        st.write(f"- {row['Executorインスタンス']}: {executor_spec}")
                        st.write(f"- ノード数: {row['Executorノード数']}")
                        
                        # ExecutorのDBU情報
                        if row['Executorインスタンス'] in instance_dbu_rates:
                            executor_dbu = instance_dbu_rates[row['Executorインスタンス']]["dbu_per_hour"]
                            total_executor_dbu = executor_dbu * row['Executorノード数']
                            st.write(f"- DBU消費量: {executor_dbu} DBU/h × {row['Executorノード数']} = {total_executor_dbu} DBU/h")
        
    
    with col2:
        st.header("📊 計算結果")
        
        if "calculation_results" in st.session_state and st.session_state.calculation_results:
            results = st.session_state.calculation_results
            
            # 合計コスト表示
            total_databricks_monthly = sum(r["databricks_cost_monthly"] for r in results)
            total_databricks_daily = sum(r["databricks_cost_daily"] for r in results)
            total_ec2_monthly = sum(r["total_ec2_cost_monthly"] for r in results)
            total_ec2_daily = sum(r["total_ec2_cost_daily"] for r in results)
            grand_total_monthly = total_databricks_monthly + total_ec2_monthly
            grand_total_daily = total_databricks_daily + total_ec2_daily
            
            # 月間料金メトリクス
            st.subheader("📅 月間料金")
            col_m1, col_m2, col_m3 = st.columns(3)
            with col_m1:
                st.metric("Databricks月間", f"${total_databricks_monthly:,.2f}")
            with col_m2:
                st.metric("EC2月間", f"${total_ec2_monthly:,.2f}")
            with col_m3:
                st.metric("月間合計", f"${grand_total_monthly:,.2f}")
            
            # 日間料金メトリクス
            st.subheader("📆 日間料金")
            col_d1, col_d2, col_d3 = st.columns(3)
            with col_d1:
                st.metric("Databricks日間", f"${total_databricks_daily:,.2f}")
            with col_d2:
                st.metric("EC2日間", f"${total_ec2_daily:,.2f}")
            with col_d3:
                st.metric("日間合計", f"${grand_total_daily:,.2f}")
            
            st.markdown("---")
            
            # ワークロードごとの料金サマリー
            st.subheader("📋 ワークロード別料金サマリー")
            
            # ワークロード別料金テーブル
            workload_summary = []
            for result in results:
                workload_summary.append({
                    "ワークロード": result["workload_name"],
                    "タイプ": result["workload_type"],
                    "月間Databricks": f"${result['databricks_cost_monthly']:,.2f}",
                    "月間EC2": f"${result['total_ec2_cost_monthly']:,.2f}",
                    "月間合計": f"${result['total_cost_monthly']:,.2f}",
                    "日間合計": f"${result['total_cost_daily']:,.2f}"
                })
            
            summary_df = pd.DataFrame(workload_summary)
            st.dataframe(summary_df, use_container_width=True, hide_index=True)
            
            st.markdown("---")
            
            # 詳細結果テーブル
            st.subheader("🔍 詳細内訳")
            
            # DBU情報と料金内訳表示
            for i, result in enumerate(results):
                with st.expander(f"🔍 {result['workload_name']} - 詳細分析"):
                    
                    # DBU情報表示
                    st.subheader("💎 Databricks DBU情報")
                    col_dbu1, col_dbu2, col_dbu3, col_dbu4 = st.columns(4)
                    with col_dbu1:
                        st.metric("DBU単価", f"${result['dbu_price']:.3f}")
                    with col_dbu2:
                        st.metric("Driver DBU/h", f"{result['driver_dbu_rate']:.1f}")
                    with col_dbu3:
                        st.metric("Executor DBU/h", f"{result['executor_dbu_rate']:.1f}")
                    with col_dbu4:
                        st.metric("月間総DBU", f"{result['total_dbu_monthly']:,.1f}")
                    
                    # DBU内訳詳細
                    st.write("**🔢 DBU計算内訳:**")
                    
                    if result["workload_type"] == "sql-warehouse-serverless":
                        # SQL Warehouse Serverlessの場合
                        st.write(f"- **SQL Warehouseサイズ:** {result.get('sql_warehouse_size', 'N/A')}")
                        st.write(f"- **クラスタ数:** {result.get('sql_warehouse_cluster_count', 1)}")
                        if 'size_dbu_rate' in result:
                            st.write(f"- サイズ別DBU消費量: {result['size_dbu_rate']:.1f} DBU/h")
                            st.write(f"- 総DBU消費量: {result['size_dbu_rate']:.1f} × {result.get('sql_warehouse_cluster_count', 1)} = {result.get('total_dbu_per_hour', 0):.1f} DBU/h")
                        st.write(f"- 合計DBU: {result['total_dbu_monthly']:,.1f} DBU × ${result['dbu_price']:.3f} = ${result['databricks_cost_monthly']:,.2f}/月")
                    else:
                        # 通常のクラスターの場合
                        photon_status = "✅ 有効" if result.get("photon_enabled", False) else "❌ 無効"
                        st.write(f"- **Photon設定:** {photon_status}")
                        st.write(f"- Driver: {result['driver_dbu_rate']:.1f} DBU/h × 1ノード = {result['driver_dbu_monthly']:,.1f} DBU/月")
                        st.write(f"- Executor: {result['executor_dbu_rate']:.1f} DBU/h × ノード数 = {result['executor_dbu_monthly']:,.1f} DBU/月")
                        st.write(f"- 合計DBU: {result['total_dbu_monthly']:,.1f} DBU × ${result['dbu_price']:.3f} = ${result['databricks_cost_monthly']:,.2f}/月")
                    
                    # 月間料金内訳
                    st.subheader("📅 月間料金内訳")
                    monthly_data = {
                        "コンポーネント": ["Databricks", "Driver EC2", "Executor EC2", "総合計"],
                        "料金": [
                            f"${result['databricks_cost_monthly']:,.2f}",
                            f"${result['driver_cost_monthly']:,.2f}",
                            f"${result['executor_cost_monthly']:,.2f}",
                            f"${result['total_cost_monthly']:,.2f}"
                        ]
                    }
                    monthly_df = pd.DataFrame(monthly_data)
                    st.dataframe(monthly_df, use_container_width=True, hide_index=True)
                    
                    # 日間料金内訳
                    st.subheader("📆 日間料金内訳")
                    daily_data = {
                        "コンポーネント": ["Databricks", "Driver EC2", "Executor EC2", "総合計"],
                        "料金": [
                            f"${result['databricks_cost_daily']:,.2f}",
                            f"${result['driver_cost_daily']:,.2f}",
                            f"${result['executor_cost_daily']:,.2f}",
                            f"${result['total_cost_daily']:,.2f}"
                        ]
                    }
                    daily_df = pd.DataFrame(daily_data)
                    st.dataframe(daily_df, use_container_width=True, hide_index=True)
            
            # チャート表示
            st.subheader("📊 料金内訳チャート")
            
            # ワークロード別チャート
            st.write("**ワークロード別月間料金**")
            workload_chart_data = {}
            workload_names = []
            databricks_costs = []
            ec2_costs = []
            
            for result in results:
                workload_names.append(result["workload_name"])
                databricks_costs.append(result["databricks_cost_monthly"])
                ec2_costs.append(result["total_ec2_cost_monthly"])
            
            chart_df = pd.DataFrame({
                "Databricks": databricks_costs,
                "EC2": ec2_costs
            }, index=workload_names)
            st.bar_chart(chart_df)
            
            # 全体合計チャート
            st.write("**全体月間料金**")
            total_chart_data = pd.DataFrame({
                "Databricks": [total_databricks_monthly],
                "EC2": [total_ec2_monthly]
            })
            st.bar_chart(total_chart_data)
        else:
            st.info("料金計算を実行してください。")

if __name__ == "__main__":
    main()
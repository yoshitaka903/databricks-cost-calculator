import streamlit as st
import pandas as pd
import json
from pathlib import Path
import re
import io
from datetime import datetime

def natural_sort_key(instance_type: str):
    """インスタンスタイプを自然な順序でソートするためのキー関数"""
    parts = instance_type.split('.')
    if len(parts) != 2:
        return (instance_type, 0)
    
    family, size = parts
    size_match = re.match(r'^(\d*)(.*)$', size)
    if size_match:
        num_str, size_suffix = size_match.groups()
        num = int(num_str) if num_str else 0
    else:
        num = 0
        size_suffix = size
    
    size_order = {
        'nano': 0.1, 'micro': 0.2, 'small': 0.3, 'medium': 0.4, 'large': 1,
        'xlarge': 2, '2xlarge': 3, '4xlarge': 4, '8xlarge': 5, '12xlarge': 6,
        '16xlarge': 7, '24xlarge': 8, '32xlarge': 9, '48xlarge': 10, 'metal': 100
    }
    
    if num > 0 and size_suffix in ['xlarge']:
        final_order = size_order.get('xlarge', 1) + num - 1
    else:
        final_order = size_order.get(size, 1)
    
    return (family, final_order, size)

def load_data():
    """データ読み込み"""
    try:
        base_path = Path(__file__).parent
        data_path = base_path / "data"
        
        with open(data_path / "databricks_compute_pricing_updated.json", "r") as f:
            databricks_data = json.load(f)
        
        with open(data_path / "ec2_pricing_tokyo.json", "r") as f:
            ec2_data = json.load(f)
        
        # SQL Warehouseサイズデータ読み込み
        sql_warehouse_file = data_path / "sql_warehouse_sizes.json"
        sql_warehouse_data = {}
        if sql_warehouse_file.exists():
            with open(sql_warehouse_file, "r") as f:
                sql_warehouse_data = json.load(f)
            
        return databricks_data, ec2_data.get("pricing", {}), sql_warehouse_data
    except Exception as e:
        st.error(f"データ読み込みエラー: {e}")
        return {}, {}, {}

def format_instance_option(instance_type: str, ec2_data: dict) -> str:
    """インスタンスタイプにスペック情報を追加して表示"""
    if instance_type == "same_as_driver":
        return "Driverと同じ"
    spec_info = ec2_data.get(instance_type, {})
    vcpu = spec_info.get("vcpu", "N/A")
    memory = spec_info.get("memory", "N/A")
    return f"{instance_type} ({vcpu} vCPU, {memory})"

def calculate_workload_cost(config: dict, databricks_data: dict, ec2_data: dict, sql_warehouse_data: dict = None) -> dict:
    """ワークロードの料金を計算"""
    try:
        # SQL Warehouseの場合
        if config["workload_type"] == "sql-warehouse-serverless":
            if not sql_warehouse_data:
                st.error("SQL Warehouseデータが見つかりません")
                return {}
            
            # SQL Warehouseサイズ別DBU消費量を取得
            size_info = sql_warehouse_data.get(config["sql_warehouse_size"], {})
            dbu_per_hour = size_info.get("dbu_per_hour", 0)
            
            # クラスタ数とサイズに基づく計算
            total_dbu_per_hour = dbu_per_hour * config["sql_warehouse_clusters"]
            total_dbu_monthly = total_dbu_per_hour * config["monthly_hours"]
            
            # DBU単価（固定値として$1.0を仮定、実際の値に応じて調整）
            dbu_price = 1.0
            total_databricks_monthly = total_dbu_monthly * dbu_price
            
            return {
                "workload_name": config["workload_name"],
                "workload_type": config["workload_type"],
                "sql_warehouse_size": config["sql_warehouse_size"],
                "sql_warehouse_clusters": config["sql_warehouse_clusters"],
                "driver_instance": "",
                "executor_instance": "",
                "actual_executor_instance": "",
                "executor_nodes": 0,
                "photon_enabled": False,
                "monthly_hours": config["monthly_hours"],
                "daily_hours": config.get("daily_hours", 8),
                "monthly_days": config.get("monthly_days", 20),
                "driver_dbu": 0,
                "executor_dbu": total_dbu_per_hour,
                "total_dbu": total_dbu_monthly,
                "databricks_monthly": total_databricks_monthly,
                "ec2_monthly": 0,  # SQL WarehouseはServerlessなのでEC2料金なし
                "total_monthly": total_databricks_monthly
            }
        
        # クラスター型ワークロードの場合
        region_data = databricks_data["enterprise"]["aws"]["ap-northeast-1"]
        
        # ワークロードキー決定
        workload_type = config["workload_type"]
        if config["photon_enabled"]:
            if workload_type == "all-purpose":
                workload_key = "all-purpose-photon"
            elif workload_type == "jobs":
                workload_key = "jobs-photon"
            elif workload_type == "dlt-advanced":
                workload_key = "dlt-advanced-photon"
        else:
            workload_key = workload_type
        
        workload_pricing = region_data.get(workload_key, {})
        
        # Executorインスタンスがdriverと同じ場合の処理
        actual_executor_instance = config["executor_instance"]
        if actual_executor_instance == "same_as_driver":
            actual_executor_instance = config["driver_instance"]
        
        # Driver計算
        driver_data = workload_pricing.get(config["driver_instance"], {})
        driver_dbu = driver_data.get("dbu_per_hour", 0)
        driver_rate = driver_data.get("rate_per_hour", 0)
        driver_monthly = driver_rate * config["monthly_hours"]
        
        # Executor計算
        executor_data = workload_pricing.get(actual_executor_instance, {})
        executor_dbu = executor_data.get("dbu_per_hour", 0)
        executor_rate = executor_data.get("rate_per_hour", 0)
        executor_monthly = executor_rate * config["executor_nodes"] * config["monthly_hours"]
        
        # EC2料金
        driver_ec2 = ec2_data.get(config["driver_instance"], {}).get("price_per_hour", 0) * config["monthly_hours"]
        executor_ec2 = ec2_data.get(actual_executor_instance, {}).get("price_per_hour", 0) * config["executor_nodes"] * config["monthly_hours"]
        
        return {
            "workload_name": config["workload_name"],
            "workload_type": workload_key,
            "driver_instance": config["driver_instance"],
            "executor_instance": config["executor_instance"],  # UI表示用（"same_as_driver"の場合もあり）
            "actual_executor_instance": actual_executor_instance,  # 実際の計算用インスタンス
            "executor_nodes": config["executor_nodes"],
            "photon_enabled": config["photon_enabled"],
            "monthly_hours": config["monthly_hours"],
            "daily_hours": config.get("daily_hours", 8),
            "monthly_days": config.get("monthly_days", 20),
            "driver_dbu": driver_dbu,
            "executor_dbu": executor_dbu,
            "total_dbu": (driver_dbu + executor_dbu * config["executor_nodes"]) * config["monthly_hours"],
            "databricks_monthly": driver_monthly + executor_monthly,
            "ec2_monthly": driver_ec2 + executor_ec2,
            "total_monthly": driver_monthly + executor_monthly + driver_ec2 + executor_ec2,
            "sql_warehouse_size": config.get("sql_warehouse_size", ""),
            "sql_warehouse_clusters": config.get("sql_warehouse_clusters", 1)
        }
    except Exception as e:
        st.error(f"計算エラー: {e}")
        return {}

def main():
    st.set_page_config(page_title="Databricks料金計算", layout="wide")
    st.title("💰 Databricks料金計算ツール")
    
    # セッション状態初期化
    if "workloads" not in st.session_state:
        st.session_state.workloads = []
    
    # データ読み込み
    databricks_data, ec2_data, sql_warehouse_data = load_data()
    
    if not databricks_data:
        st.error("料金データが読み込めません")
        return
    
    # インスタンスタイプ取得とソート
    try:
        region_data = databricks_data["enterprise"]["aws"]["ap-northeast-1"]
        instance_types = set()
        for workload_data in region_data.values():
            if isinstance(workload_data, dict):
                instance_types.update(workload_data.keys())
        instance_types = sorted(list(instance_types), key=natural_sort_key)
        st.sidebar.success(f"{len(instance_types)}個のインスタンスタイプが利用可能")
    except Exception as e:
        st.error(f"インスタンス取得エラー: {e}")
        return
    
    # スペック付きインスタンスオプション作成
    instance_options = [format_instance_option(inst, ec2_data) for inst in instance_types]
    instance_mapping = {opt: inst for opt, inst in zip(instance_options, instance_types)}
    
    # Executorインスタンス用オプション（"Driverと同じ"を先頭に追加）
    executor_options = ["Driverと同じ"] + instance_options
    executor_mapping = {"Driverと同じ": "same_as_driver"}
    executor_mapping.update(instance_mapping)
    
    # サイドバーにワークロード設定を移動
    with st.sidebar:
        st.header("📝 ワークロード設定")
        
        with st.form("workload_form"):
            workload_name = st.text_input("ワークロード名", value=f"ワークロード{len(st.session_state.workloads)+1}")
            workload_type = st.selectbox("ワークロードタイプ", ["all-purposeクラスター", "jobsクラスター", "dlt-advancedクラスター"])
            
            # クラスター型ワークロード用フィールド
            # デフォルトインデックス設定
            default_driver_idx = next((i for i, opt in enumerate(instance_options) if "r5.large" in opt), 0)
            
            driver_option = st.selectbox("Driverインスタンス", instance_options, index=default_driver_idx)
            executor_option = st.selectbox("Executorインスタンス", executor_options, index=0)  # 初期値は"Driverと同じ"
            
            executor_nodes = st.number_input("Executorノード数", min_value=0, max_value=100, value=2)
            daily_hours = st.number_input("1日あたりの利用時間", min_value=1, max_value=24, value=8)
            monthly_days = st.number_input("月間利用日数", min_value=1, max_value=31, value=20)
            photon_enabled = st.checkbox("Photon有効")
            
            # 月間利用時間を自動計算して表示
            monthly_hours = daily_hours * monthly_days
            st.info(f"📅 月間利用時間: {monthly_hours}時間 ({daily_hours}時間/日 × {monthly_days}日)")
            
            submitted = st.form_submit_button("➕ ワークロードを追加", type="primary")
            
            if submitted:
                # クラスター型ワークロード設定
                # ワークロードタイプから"クラスター"を除去
                clean_workload_type = workload_type.replace("クラスター", "")
                
                workload_config = {
                    "workload_name": workload_name,
                    "workload_type": clean_workload_type,
                    "driver_instance": instance_mapping[driver_option],
                    "executor_instance": executor_mapping[executor_option],
                    "executor_nodes": executor_nodes,
                    "daily_hours": daily_hours,
                    "monthly_days": monthly_days,
                    "monthly_hours": monthly_hours,
                    "photon_enabled": photon_enabled,
                    "sql_warehouse_size": "",
                    "sql_warehouse_clusters": 1
                }
                
                # 計算実行
                result = calculate_workload_cost(workload_config, databricks_data, ec2_data, sql_warehouse_data)
                if result:
                    st.session_state.workloads.append(result)
                    st.success(f"ワークロード '{workload_name}' を追加しました！")
                    st.rerun()

        # ワークロード管理もサイドバーに
        if st.session_state.workloads:
            st.subheader("🗂️ ワークロード管理")
            for i, workload in enumerate(st.session_state.workloads):
                col_name, col_edit, col_del = st.columns([2, 1, 1])
                with col_name:
                    st.write(f"**{workload['workload_name']}**")
                with col_edit:
                    if st.button("✏️", key=f"edit_{i}", help="編集"):
                        st.session_state.editing_index = i
                        st.rerun()
                with col_del:
                    if st.button("🗑️", key=f"del_{i}", help="削除"):
                        st.session_state.workloads.pop(i)
                        if hasattr(st.session_state, 'editing_index') and st.session_state.editing_index >= i:
                            if st.session_state.editing_index == i:
                                del st.session_state.editing_index
                            else:
                                st.session_state.editing_index -= 1
                        st.rerun()
            
            if st.button("🧹 全クリア"):
                st.session_state.workloads = []
                if hasattr(st.session_state, 'editing_index'):
                    del st.session_state.editing_index
                st.rerun()

        # SQL Warehouse専用セクション
        st.header("🏢 SQL Warehouse設定")
        
        with st.form("sql_warehouse_form"):
            sql_workload_name = st.text_input("SQL Warehouseワークロード名", value=f"SQL Warehouse {len([w for w in st.session_state.workloads if w.get('workload_type') == 'sql-warehouse-serverless'])+1}")
            
            # SQL Warehouseサイズ選択
            sql_warehouse_sizes = list(sql_warehouse_data.keys()) if sql_warehouse_data else ["2X-Small", "X-Small", "Small", "Medium", "Large"]
            sql_warehouse_size = st.selectbox("SQL Warehouseサイズ", sql_warehouse_sizes, index=3)  # Mediumをデフォルト
            sql_warehouse_clusters = st.number_input("クラスタ数", min_value=1, max_value=10, value=1)
            
            sql_daily_hours = st.number_input("1日あたりの利用時間", min_value=1, max_value=24, value=8, key="sql_daily")
            sql_monthly_days = st.number_input("月間利用日数", min_value=1, max_value=31, value=20, key="sql_monthly_days")
            
            # 月間利用時間を自動計算して表示
            sql_monthly_hours = sql_daily_hours * sql_monthly_days
            st.info(f"📅 月間利用時間: {sql_monthly_hours}時間 ({sql_daily_hours}時間/日 × {sql_monthly_days}日)")
            
            sql_submitted = st.form_submit_button("➕ SQL Warehouseを追加", type="primary")
            
            if sql_submitted:
                sql_workload_config = {
                    "workload_name": sql_workload_name,
                    "workload_type": "sql-warehouse-serverless",
                    "sql_warehouse_size": sql_warehouse_size,
                    "sql_warehouse_clusters": sql_warehouse_clusters,
                    "daily_hours": sql_daily_hours,
                    "monthly_days": sql_monthly_days,
                    "monthly_hours": sql_monthly_hours,
                    "driver_instance": "",
                    "executor_instance": "",
                    "executor_nodes": 0,
                    "photon_enabled": False
                }
                
                # 計算実行
                sql_result = calculate_workload_cost(sql_workload_config, databricks_data, ec2_data, sql_warehouse_data)
                if sql_result:
                    st.session_state.workloads.append(sql_result)
                    st.success(f"SQL Warehouseワークロード '{sql_workload_name}' を追加しました！")
                    st.rerun()

        # 編集フォームもサイドバーに
        if hasattr(st.session_state, 'editing_index'):
            editing_workload = st.session_state.workloads[st.session_state.editing_index]
            st.subheader("✏️ ワークロード編集")
            
            with st.form("edit_workload_form"):
                edit_name = st.text_input("ワークロード名", value=editing_workload['workload_name'])
                # SQL Warehouseかクラスターかで分岐
                if editing_workload['workload_type'] == "sql-warehouse-serverless":
                    # SQL Warehouse編集
                    sql_warehouse_sizes = list(sql_warehouse_data.keys()) if sql_warehouse_data else ["2X-Small", "X-Small", "Small", "Medium", "Large"]
                    current_size_idx = sql_warehouse_sizes.index(editing_workload.get('sql_warehouse_size', 'Medium')) if editing_workload.get('sql_warehouse_size', 'Medium') in sql_warehouse_sizes else 3
                    
                    edit_sql_size = st.selectbox("SQL Warehouseサイズ", sql_warehouse_sizes, index=current_size_idx)
                    edit_sql_clusters = st.number_input("クラスタ数", min_value=1, max_value=10, 
                                                      value=editing_workload.get('sql_warehouse_clusters', 1))
                    
                    edit_daily = st.number_input("1日あたりの利用時間", min_value=1, max_value=24, 
                                               value=editing_workload.get('daily_hours', 8))
                    edit_monthly_days = st.number_input("月間利用日数", min_value=1, max_value=31, 
                                                      value=editing_workload.get('monthly_days', 20))
                    
                    # 月間利用時間を自動計算して表示
                    edit_monthly = edit_daily * edit_monthly_days
                    st.info(f"📅 月間利用時間: {edit_monthly}時間 ({edit_daily}時間/日 × {edit_monthly_days}日)")
                    
                    col_update, col_cancel = st.columns(2)
                    with col_update:
                        update_submitted = st.form_submit_button("💾 更新", type="primary")
                    with col_cancel:
                        cancel_submitted = st.form_submit_button("❌ キャンセル")
                    
                    if update_submitted:
                        # SQL Warehouse更新設定
                        updated_config = {
                            "workload_name": edit_name,
                            "workload_type": "sql-warehouse-serverless",
                            "sql_warehouse_size": edit_sql_size,
                            "sql_warehouse_clusters": edit_sql_clusters,
                            "daily_hours": edit_daily,
                            "monthly_days": edit_monthly_days,
                            "monthly_hours": edit_monthly,
                            "driver_instance": "",
                            "executor_instance": "",
                            "executor_nodes": 0,
                            "photon_enabled": False
                        }
                        
                        # 再計算
                        result = calculate_workload_cost(updated_config, databricks_data, ec2_data, sql_warehouse_data)
                        if result:
                            st.session_state.workloads[st.session_state.editing_index] = result
                            del st.session_state.editing_index
                            st.success(f"SQL Warehouseワークロード '{edit_name}' を更新しました！")
                            st.rerun()
                    
                    if cancel_submitted:
                        del st.session_state.editing_index
                        st.rerun()
                    
                    return
                
                # 編集時のワークロードタイプ表示名の変換  
                current_type_display = editing_workload['workload_type'] + "クラスター"
                
                edit_type = st.selectbox("ワークロードタイプ", 
                                       ["all-purposeクラスター", "jobsクラスター", "dlt-advancedクラスター"],
                                       index=["all-purposeクラスター", "jobsクラスター", "dlt-advancedクラスター"].index(current_type_display))
                
                # クラスター編集（SQL Warehouseは除外済み）
                # 現在のインスタンスを選択状態にする
                current_driver_option = format_instance_option(editing_workload['driver_instance'], ec2_data)
                current_executor_option = format_instance_option(editing_workload['executor_instance'], ec2_data)
                
                edit_driver_idx = instance_options.index(current_driver_option) if current_driver_option in instance_options else 0
                edit_executor_idx = executor_options.index(current_executor_option) if current_executor_option in executor_options else 0
                
                edit_driver = st.selectbox("Driverインスタンス", instance_options, index=edit_driver_idx)
                edit_executor = st.selectbox("Executorインスタンス", executor_options, index=edit_executor_idx)
                
                col_edit1, col_edit2 = st.columns(2)
                with col_edit1:
                    edit_nodes = st.number_input("Executorノード数", min_value=0, max_value=100, 
                                               value=editing_workload['executor_nodes'])
                    edit_daily = st.number_input("1日あたりの利用時間", min_value=1, max_value=24, 
                                               value=editing_workload.get('daily_hours', 8))
                with col_edit2:
                    edit_monthly_days = st.number_input("月間利用日数", min_value=1, max_value=31, 
                                                      value=editing_workload.get('monthly_days', 20))
                    edit_photon = st.checkbox("Photon有効", value=editing_workload['photon_enabled'])
                
                # 月間利用時間を自動計算して表示
                edit_monthly = edit_daily * edit_monthly_days
                st.info(f"📅 月間利用時間: {edit_monthly}時間 ({edit_daily}時間/日 × {edit_monthly_days}日)")
                
                col_update, col_cancel = st.columns(2)
                with col_update:
                    update_submitted = st.form_submit_button("💾 更新", type="primary")
                with col_cancel:
                    cancel_submitted = st.form_submit_button("❌ キャンセル")
                
                if update_submitted:
                    # クラスター型ワークロード更新設定（SQL Warehouseは除外済み）
                    # ワークロードタイプから"クラスター"を除去
                    clean_edit_type = edit_type.replace("クラスター", "")
                    
                    updated_config = {
                        "workload_name": edit_name,
                        "workload_type": clean_edit_type,
                        "driver_instance": instance_mapping[edit_driver],
                        "executor_instance": executor_mapping[edit_executor],
                        "executor_nodes": edit_nodes,
                        "daily_hours": edit_daily,
                        "monthly_days": edit_monthly_days,
                        "monthly_hours": edit_monthly,
                        "photon_enabled": edit_photon,
                        "sql_warehouse_size": "",
                        "sql_warehouse_clusters": 1
                    }
                    
                    # 再計算
                    result = calculate_workload_cost(updated_config, databricks_data, ec2_data, sql_warehouse_data)
                    if result:
                        st.session_state.workloads[st.session_state.editing_index] = result
                        del st.session_state.editing_index
                        st.success(f"ワークロード '{edit_name}' を更新しました！")
                        st.rerun()
                
                if cancel_submitted:
                    del st.session_state.editing_index
                    st.rerun()

    # メインコンテンツエリア（サイドバーに設定を移動したので全幅使用）
    st.header("📊 料金計算結果")
    
    if not st.session_state.workloads:
        st.info("サイドバーでワークロードを設定・追加してください")
    else:
        # 合計計算
        total_databricks = sum(w["databricks_monthly"] for w in st.session_state.workloads)
        total_ec2 = sum(w["ec2_monthly"] for w in st.session_state.workloads)
        total_dbu = sum(w["total_dbu"] for w in st.session_state.workloads)
        grand_total = total_databricks + total_ec2
        
        # サマリーメトリクス
        col_m1, col_m2, col_m3 = st.columns(3)
        with col_m1:
            st.metric("Databricks月間", f"${total_databricks:,.2f}")
        with col_m2:
            st.metric("EC2月間", f"${total_ec2:,.2f}")
        with col_m3:
            st.metric("月間合計", f"${grand_total:,.2f}")
        
        # スプレッドシート出力機能
        st.subheader("📊 データ出力")
        col_export1, col_export2 = st.columns(2)
        
        with col_export1:
            if st.button("📊 Excel形式でダウンロード", type="secondary"):
                # 詳細データをDataFrameに変換
                export_data = []
                for w in st.session_state.workloads:
                    # DBU単価を計算
                    dbu_unit_price = w['databricks_monthly'] / w['total_dbu'] if w['total_dbu'] > 0 else 0
                    
                    # SQL Warehouseの場合とクラスターの場合で分岐
                    if w["workload_type"] == "sql-warehouse-serverless":
                        # SQL Warehouse用のエクスポートデータ
                        export_data.append({
                            "ワークロード名": w['workload_name'],
                            "ワークロードタイプ": w['workload_type'],
                            "SQL Warehouseサイズ": w.get('sql_warehouse_size', ''),
                            "クラスタ数": w.get('sql_warehouse_clusters', 1),
                            "月間利用時間": f"{w['monthly_hours']}時間 ({w.get('daily_hours', 8)}時間/日 × {w.get('monthly_days', 20)}日)",
                            "DBU/h": f"{w['executor_dbu']:.2f}",
                            "月間総DBU": f"{w['total_dbu']:.0f}",
                            "DBU単価": f"${dbu_unit_price:.3f}",
                            "Databricks料金(月)": f"${w['databricks_monthly']:,.2f}",
                            "EC2料金(月)": f"${w['ec2_monthly']:,.2f}",
                            "合計料金(月)": f"${w['total_monthly']:,.2f}"
                        })
                    else:
                        # クラスター型ワークロード用のエクスポートデータ
                        photon_status = "有効" if w["photon_enabled"] else "無効"
                        actual_executor = w['actual_executor_instance'] if w['executor_instance'] == 'same_as_driver' else w['executor_instance']
                        executor_display = f"{actual_executor} ×{w['executor_nodes']}"
                        if w['executor_instance'] == 'same_as_driver':
                            executor_display += " (Driverと同じ)"
                        
                        export_data.append({
                            "ワークロード名": w['workload_name'],
                            "ワークロードタイプ": w['workload_type'],
                            "Photon": photon_status,
                            "Driverインスタンス": w['driver_instance'],
                            "Executorインスタンス": executor_display,
                            "月間利用時間": f"{w['monthly_hours']}時間 ({w.get('daily_hours', 8)}時間/日 × {w.get('monthly_days', 20)}日)",
                            "Driver DBU/h": f"{w['driver_dbu']:.2f}",
                            "Executor DBU/h": f"{w['executor_dbu']:.2f}",
                            "月間総DBU": f"{w['total_dbu']:.0f}",
                            "DBU単価": f"${dbu_unit_price:.3f}",
                            "Databricks料金(月)": f"${w['databricks_monthly']:,.2f}",
                            "EC2料金(月)": f"${w['ec2_monthly']:,.2f}",
                            "合計料金(月)": f"${w['total_monthly']:,.2f}"
                        })
                
                export_df = pd.DataFrame(export_data)
                
                # Excel出力
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    export_df.to_excel(writer, sheet_name='Databricks料金計算結果', index=False)
                    
                    # サマリー情報も追加
                    summary_df = pd.DataFrame({
                        "項目": ["Databricks月間合計", "EC2月間合計", "総月間料金", "総DBU消費量"],
                        "金額・数量": [f"${total_databricks:,.2f}", f"${total_ec2:,.2f}", f"${grand_total:,.2f}", f"{total_dbu:,.0f} DBU"]
                    })
                    summary_df.to_excel(writer, sheet_name='サマリー', index=False)
                
                excel_buffer.seek(0)
                
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                
                st.download_button(
                    label="📊 Excelファイルをダウンロード",
                    data=excel_buffer.getvalue(),
                    file_name=f"databricks_料金計算_{timestamp}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                )
        
        with col_export2:
            # CSV出力
            export_data = []
            for w in st.session_state.workloads:
                dbu_unit_price = w['databricks_monthly'] / w['total_dbu'] if w['total_dbu'] > 0 else 0
                
                # SQL Warehouseの場合とクラスターの場合で分岐
                if w["workload_type"] == "sql-warehouse-serverless":
                    # SQL Warehouse用のエクスポートデータ
                    export_data.append({
                        "ワークロード名": w['workload_name'],
                        "ワークロードタイプ": w['workload_type'],
                        "SQL Warehouseサイズ": w.get('sql_warehouse_size', ''),
                        "クラスタ数": w.get('sql_warehouse_clusters', 1),
                        "月間利用時間": f"{w['monthly_hours']}時間 ({w.get('daily_hours', 8)}時間/日 × {w.get('monthly_days', 20)}日)",
                        "DBU/h": f"{w['executor_dbu']:.2f}",
                        "月間総DBU": f"{w['total_dbu']:.0f}",
                        "DBU単価": f"${dbu_unit_price:.3f}",
                        "Databricks料金(月)": f"${w['databricks_monthly']:,.2f}",
                        "EC2料金(月)": f"${w['ec2_monthly']:,.2f}",
                        "合計料金(月)": f"${w['total_monthly']:,.2f}"
                    })
                else:
                    # クラスター型ワークロード用のエクスポートデータ
                    photon_status = "有効" if w["photon_enabled"] else "無効"
                    actual_executor = w['actual_executor_instance'] if w['executor_instance'] == 'same_as_driver' else w['executor_instance']
                    executor_display = f"{actual_executor} ×{w['executor_nodes']}"
                    if w['executor_instance'] == 'same_as_driver':
                        executor_display += " (Driverと同じ)"
                    
                    export_data.append({
                        "ワークロード名": w['workload_name'],
                        "ワークロードタイプ": w['workload_type'],
                        "Photon": photon_status,
                        "Driverインスタンス": w['driver_instance'],
                        "Executorインスタンス": executor_display,
                        "月間利用時間": f"{w['monthly_hours']}時間 ({w.get('daily_hours', 8)}時間/日 × {w.get('monthly_days', 20)}日)",
                        "Driver DBU/h": f"{w['driver_dbu']:.2f}",
                        "Executor DBU/h": f"{w['executor_dbu']:.2f}",
                        "月間総DBU": f"{w['total_dbu']:.0f}",
                        "DBU単価": f"${dbu_unit_price:.3f}",
                        "Databricks料金(月)": f"${w['databricks_monthly']:,.2f}",
                        "EC2料金(月)": f"${w['ec2_monthly']:,.2f}",
                        "合計料金(月)": f"${w['total_monthly']:,.2f}"
                    })
            
            export_df = pd.DataFrame(export_data)
            csv_buffer = io.StringIO()
            export_df.to_csv(csv_buffer, index=False, encoding='utf-8-sig')
            csv_data = csv_buffer.getvalue()
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            st.download_button(
                label="📄 CSVファイルをダウンロード",
                data=csv_data,
                file_name=f"databricks_料金計算_{timestamp}.csv",
                mime="text/csv"
            )
        
        # ワークロード明細テーブル
        st.subheader("📋 ワークロード明細")
        
        workload_summary = []
        for w in st.session_state.workloads:
            # SQL Warehouseの場合とクラスターの場合で分岐
            if w["workload_type"] == "sql-warehouse-serverless":
                # SQL Warehouse用のサマリー表示
                workload_summary.append({
                    "ワークロード名": w['workload_name'],
                    "タイプ": w["workload_type"],
                    "サイズ": w.get('sql_warehouse_size', ''),
                    "クラスタ数": f"×{w.get('sql_warehouse_clusters', 1)}",
                    "月間時間": f"{w['monthly_hours']}h ({w.get('daily_hours', 8)}h/日×{w.get('monthly_days', 20)}日)",
                    "Databricks": f"${w['databricks_monthly']:,.0f}",
                    "EC2": f"${w['ec2_monthly']:,.0f}",
                    "合計": f"${w['total_monthly']:,.0f}"
                })
            else:
                # クラスター型ワークロード用のサマリー表示
                photon_mark = "⚡" if w["photon_enabled"] else ""
                workload_summary.append({
                    "ワークロード名": f"{w['workload_name']} {photon_mark}",
                    "タイプ": w["workload_type"],
                    "Driver": w["driver_instance"],
                    "Executor": f"{w['actual_executor_instance'] if w['executor_instance'] == 'same_as_driver' else w['executor_instance']} ×{w['executor_nodes']}",
                    "月間時間": f"{w['monthly_hours']}h ({w.get('daily_hours', 8)}h/日×{w.get('monthly_days', 20)}日)",
                    "Databricks": f"${w['databricks_monthly']:,.0f}",
                    "EC2": f"${w['ec2_monthly']:,.0f}",
                    "合計": f"${w['total_monthly']:,.0f}"
                })
        
        st.dataframe(pd.DataFrame(workload_summary), use_container_width=True, hide_index=True)
        
        # 詳細分析
        with st.expander("🔍 詳細分析"):
            st.write(f"**総DBU消費量:** {total_dbu:,.0f} DBU/月")
            st.write(f"**実効DBU単価:** ${total_databricks/total_dbu:.3f}/DBU" if total_dbu > 0 else "DBU単価計算不可")
            
            # 個別ワークロード詳細
            for w in st.session_state.workloads:
                st.write(f"**{w['workload_name']}:**")
                if w["workload_type"] == "sql-warehouse-serverless":
                    # SQL Warehouse用の詳細表示
                    st.write(f"- SQL Warehouseサイズ: {w.get('sql_warehouse_size', '')}, クラスタ数: {w.get('sql_warehouse_clusters', 1)}")
                    st.write(f"- DBU消費量: {w['executor_dbu']:.2f}/h per cluster")
                    st.write(f"- 月間DBU: {w['total_dbu']:,.0f} DBU")
                else:
                    # クラスター型ワークロード用の詳細表示
                    st.write(f"- Driver DBU: {w['driver_dbu']:.2f}/h, Executor DBU: {w['executor_dbu']:.2f}/h")
                    st.write(f"- 月間DBU: {w['total_dbu']:,.0f} DBU")
                st.write("")
        
        # 計算式表示
        with st.expander("📐 計算式の詳細"):
            st.markdown("### 💡 料金計算の仕組み")
            st.markdown("""
            **クラスター型ワークロード:**
            - Databricks料金 = DBU消費量 × DBU単価（ワークロード別）
            - Driver DBU消費量 = Driver DBU/h × 1ノード × 月間時間
            - Executor DBU消費量 = Executor DBU/h × ノード数 × 月間時間
            - EC2料金 = インスタンス時間料金 × 利用時間
            
            **SQL Warehouse（Serverless）:**
            - Databricks料金 = サイズ別DBU/h × クラスタ数 × 月間時間 × DBU単価
            - EC2料金 = $0（Serverlessのため）
            """)
            
            # 個別ワークロードの計算式
            for i, w in enumerate(st.session_state.workloads):
                if w["workload_type"] == "sql-warehouse-serverless":
                    # SQL Warehouse用の計算式表示
                    st.markdown(f"### 📋 {w['workload_name']} (SQL Warehouse)")
                    
                    st.markdown(f"""
                    **🏢 SQL Warehouse構成:**
                    - サイズ: {w.get('sql_warehouse_size', '')}
                    - クラスタ数: {w.get('sql_warehouse_clusters', 1)}
                    - 月間稼働時間: {w['monthly_hours']}時間 ({w.get('daily_hours', 8)}時間/日 × {w.get('monthly_days', 20)}日)
                    
                    **💎 Databricks料金計算:**
                    ```
                    SQL Warehouse DBU: {w['executor_dbu']:.2f} DBU/h per cluster
                    総DBU消費量: {w['executor_dbu']:.2f} DBU/h × {w.get('sql_warehouse_clusters', 1)}クラスタ × {w['monthly_hours']}h = {w['total_dbu']:.0f} DBU
                    DBU単価: ${w['databricks_monthly'] / w['total_dbu'] if w['total_dbu'] > 0 else 0:.3f}/DBU
                    Databricks料金: {w['total_dbu']:.0f} DBU × ${w['databricks_monthly'] / w['total_dbu'] if w['total_dbu'] > 0 else 0:.3f}/DBU = ${w['databricks_monthly']:,.2f}
                    ```
                    
                    **🔧 EC2料金:**
                    ```
                    EC2料金: $0.00 (Serverlessのため)
                    ```
                    
                    **💰 総合計:**
                    ```
                    ${w['databricks_monthly']:,.2f} (Databricks) + $0.00 (EC2) = ${w['total_monthly']:,.2f}
                    ```
                    """)
                else:
                    # クラスター型ワークロード用の計算式表示
                    photon_note = " (Photon有効)" if w['photon_enabled'] else ""
                    st.markdown(f"### 📋 {w['workload_name']}{photon_note}")
                    
                    # EC2料金情報を取得
                    driver_ec2_rate = ec2_data.get(w['driver_instance'], {}).get("price_per_hour", 0)
                    executor_ec2_rate = ec2_data.get(w['actual_executor_instance'], {}).get("price_per_hour", 0)
                    
                    st.markdown(f"""
                    **🖥️ インスタンス構成:**
                    - Driver: {w['driver_instance']} × 1ノード
                    - Executor: {w['actual_executor_instance'] if w['executor_instance'] == 'same_as_driver' else w['executor_instance']} × {w['executor_nodes']}ノード{' (Driverと同じ)' if w['executor_instance'] == 'same_as_driver' else ''}
                    - 月間稼働時間: {w['monthly_hours']}時間 ({w.get('daily_hours', 8)}時間/日 × {w.get('monthly_days', 20)}日)
                    
                    **💎 Databricks料金計算:**
                    ```
                    Driver:  {w['driver_dbu']:.2f} DBU/h × 1ノード × {w['monthly_hours']}h = {w['driver_dbu'] * w['monthly_hours']:.0f} DBU
                    Executor: {w['executor_dbu']:.2f} DBU/h × {w['executor_nodes']}ノード × {w['monthly_hours']}h = {w['executor_dbu'] * w['executor_nodes'] * w['monthly_hours']:.0f} DBU
                    合計DBU: {w['total_dbu']:.0f} DBU
                    DBU単価: {w['databricks_monthly'] / w['total_dbu'] if w['total_dbu'] > 0 else 0:.3f}$/DBU
                    Databricks料金: {w['total_dbu']:.0f} DBU × {w['databricks_monthly'] / w['total_dbu'] if w['total_dbu'] > 0 else 0:.3f}$/DBU = ${w['databricks_monthly']:,.2f}
                    ```
                    
                    **🔧 EC2料金計算:**
                    ```
                    Driver EC2:  ${driver_ec2_rate:.4f}/h × 1ノード × {w['monthly_hours']}h = ${driver_ec2_rate * w['monthly_hours']:,.2f}
                    Executor EC2: ${executor_ec2_rate:.4f}/h × {w['executor_nodes']}ノード × {w['monthly_hours']}h = ${executor_ec2_rate * w['executor_nodes'] * w['monthly_hours']:,.2f}
                    EC2合計: ${w['ec2_monthly']:,.2f}
                    ```
                    
                    **💰 総合計:**
                    ```
                    ${w['databricks_monthly']:,.2f} (Databricks) + ${w['ec2_monthly']:,.2f} (EC2) = ${w['total_monthly']:,.2f}
                    ```
                    """)
                
                if i < len(st.session_state.workloads) - 1:
                    st.markdown("---")

if __name__ == "__main__":
    main()
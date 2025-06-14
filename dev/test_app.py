import streamlit as st

st.title("🧪 Databricks Apps Test")
st.write("Hello from Databricks Apps!")
st.write("Current working directory:", st.text("/workspace"))

# 基本的な動作確認
if st.button("Test Button"):
    st.success("Button clicked successfully!")
    
# ファイル存在確認
import os
st.write("Files in current directory:")
try:
    files = os.listdir(".")
    st.write(files)
except Exception as e:
    st.error(f"Error listing files: {e}")

# データファイル確認
st.write("Checking data files:")
try:
    if os.path.exists("data"):
        data_files = os.listdir("data")
        st.write("Data files:", data_files)
    else:
        st.warning("data/ directory not found")
except Exception as e:
    st.error(f"Error checking data files: {e}")
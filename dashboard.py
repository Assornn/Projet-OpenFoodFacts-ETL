import streamlit as st
import mysql.connector
import pandas as pd
import plotly.express as px

# Config de la page
st.set_page_config(page_title="OpenFoodFacts Dashboard", layout="wide")
st.title("🍎 OpenFoodFacts - Data Quality & Insights")

# Connexion MySQL
@st.cache_resource
def init_connection():
    return mysql.connector.connect(
        host="localhost", user="root", password="Lolaclmo01.", database="openfoodfacts"
    )

conn = init_connection()

# 1. KPI Globaux
st.header("1. Métriques Globales")
col1, col2, col3 = st.columns(3)
with col1:
    count = pd.read_sql("SELECT COUNT(*) as c FROM gold_products", conn).iloc[0]['c']
    st.metric("Total Produits", f"{count:,.0f}")
with col2:
    brands = pd.read_sql("SELECT COUNT(DISTINCT brands) as c FROM gold_products", conn).iloc[0]['c']
    st.metric("Marques Uniques", f"{brands:,.0f}")
with col3:
    anomalies = pd.read_sql("SELECT COUNT(*) as c FROM gold_products WHERE quality_issues IS NOT NULL AND quality_issues != ''", conn).iloc[0]['c']
    st.metric("Produits avec Anomalies", f"{anomalies:,.0f}", delta_color="inverse")

# 2. Qualité des données (Pie Chart)
st.header("2. Distribution des Nutri-Scores")
df_nutri = pd.read_sql("SELECT nutriscore_grade, COUNT(*) as count FROM gold_products GROUP BY nutriscore_grade", conn)
fig = px.pie(df_nutri, values='count', names='nutriscore_grade', title='Répartition Nutri-Score')
st.plotly_chart(fig)

# 3. Top Marques (Bar Chart)
st.header("3. Top 10 Marques")
df_brands = pd.read_sql("SELECT brands, COUNT(*) as c FROM gold_products WHERE brands IS NOT NULL GROUP BY brands ORDER BY c DESC LIMIT 10", conn)
st.bar_chart(df_brands.set_index('brands'))

# 4. Focus Qualité
st.header("4. Dernières Anomalies Détectées")
df_issues = pd.read_sql("SELECT code, product_name, quality_issues FROM gold_products WHERE quality_issues != '' LIMIT 10", conn)
st.dataframe(df_issues)
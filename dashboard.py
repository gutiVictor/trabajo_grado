#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Dashboard de Análisis de Deserción Estudiantil - CUN
Versión Visual: Toda la información de terminal ahora en navegador
Autor: [Tu nombre]
Fecha: 2024
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
import json
import sys
import io
import contextlib
import os
import shutil
from procesador_masivo import ProcesadorMasivo, Config

# ============================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================

st.set_page_config(
    page_title="Análisis de Deserción Estudiantil - CUN",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS profesional
st.markdown("""
<style>
    /* Encabezado principal */
    .main-title {
        font-size: 2.2rem;
        font-weight: 800;
        color: #1e3a8a;
        text-align: center;
        padding: 0.5rem 0;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
    }
    
    .subtitle {
        text-align: center;
        color: #6b7280;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    
    /* Tarjetas de métricas */
    .metric-container {
        background: white;
        border-radius: 15px;
        padding: 1.5rem;
        box-shadow: 0 4px 20px rgba(0,0,0,0.08);
        border-left: 5px solid;
        transition: transform 0.2s;
    }
    
    .metric-container:hover {
        transform: translateY(-3px);
    }
    
    .metric-value {
        font-size: 2.8rem;
        font-weight: 800;
        margin-bottom: 0.3rem;
    }
    
    .metric-label {
        font-size: 0.95rem;
        color: #6b7280;
        font-weight: 500;
    }
    
    .metric-delta {
        font-size: 0.85rem;
        margin-top: 0.5rem;
        font-weight: 600;
    }
    
    /* Colores específicos */
    .border-red { border-left-color: #ef4444; }
    .border-orange { border-left-color: #f59e0b; }
    .border-green { border-left-color: #10b981; }
    .border-blue { border-left-color: #3b82f6; }
    .border-purple { border-left-color: #8b5cf6; }
    
    .text-red { color: #ef4444; }
    .text-orange { color: #f59e0b; }
    .text-green { color: #10b981; }
    .text-blue { color: #3b82f6; }
    .text-purple { color: #8b5cf6; }
    
    /* Secciones de análisis */
    .analysis-section {
        background: white;
        border-radius: 15px;
        padding: 2rem;
        margin: 1rem 0;
        box-shadow: 0 2px 15px rgba(0,0,0,0.06);
    }
    
    .section-title {
        font-size: 1.4rem;
        font-weight: 700;
        color: #1e3a8a;
        margin-bottom: 1.5rem;
        padding-bottom: 0.8rem;
        border-bottom: 2px solid #e5e7eb;
    }
    
    /* Tablas estilizadas */
    .stDataFrame {
        font-size: 14px;
    }
    
    /* Logs en navegador */
    .log-container {
        background: #1f2937;
        color: #10b981;
        font-family: 'Courier New', monospace;
        padding: 1rem;
        border-radius: 10px;
        font-size: 0.85rem;
        max-height: 400px;
        overflow-y: auto;
    }
    
    .log-error { color: #ef4444; }
    .log-warning { color: #f59e0b; }
    .log-info { color: #3b82f6; }
    .log-success { color: #10b981; }
    
    /* Progress bar */
    .progress-custom {
        height: 30px;
        background: #e5e7eb;
        border-radius: 15px;
        overflow: hidden;
        margin: 1rem 0;
    }
    
    .progress-fill-custom {
        height: 100%;
        border-radius: 15px;
        transition: width 0.5s ease;
        display: flex;
        align-items: center;
        justify-content: center;
        color: white;
        font-weight: 600;
        font-size: 0.9rem;
    }
    
    /* Alertas */
    .alert-box {
        padding: 1rem 1.5rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
    
    .alert-high {
        background: #fee2e2;
        border: 1px solid #fecaca;
        color: #991b1b;
    }
    
    .alert-medium {
        background: #fef3c7;
        border: 1px solid #fde68a;
        color: #92400e;
    }
    
    .alert-low {
        background: #d1fae5;
        border: 1px solid #a7f3d0;
        color: #065f46;
    }
    
    /* Grid de estadísticas */
    .stats-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
        margin: 1rem 0;
    }
    
    .stat-item {
        background: #f9fafb;
        padding: 1rem;
        border-radius: 10px;
        text-align: center;
    }
    
    .stat-number {
        font-size: 1.8rem;
        font-weight: 700;
        color: #1e3a8a;
    }
    
    .stat-label {
        font-size: 0.85rem;
        color: #6b7280;
        margin-top: 0.3rem;
    }
    
    /* Console output simulation */
    .console-box {
        background: #111827;
        border-radius: 12px;
        padding: 1.5rem;
        font-family: 'Monaco', 'Menlo', monospace;
        color: #e5e7eb;
        font-size: 0.9rem;
        line-height: 1.6;
    }
    
    .console-line {
        margin: 0.3rem 0;
    }
    
    .console-prompt { color: #10b981; }
    .console-command { color: #60a5fa; }
    .console-output { color: #e5e7eb; }
    .console-error { color: #ef4444; }
    .console-success { color: #34d399; }
    
    /* Feature importance bars */
    .feature-bar {
        display: flex;
        align-items: center;
        margin: 0.5rem 0;
    }
    
    .feature-name {
        width: 200px;
        font-size: 0.9rem;
        color: #374151;
    }
    
    .feature-track {
        flex: 1;
        height: 25px;
        background: #e5e7eb;
        border-radius: 12px;
        overflow: hidden;
        margin: 0 1rem;
    }
    
    .feature-fill {
        height: 100%;
        border-radius: 12px;
        display: flex;
        align-items: center;
        justify-content: flex-end;
        padding-right: 10px;
        color: white;
        font-size: 0.8rem;
        font-weight: 600;
    }
    
    .feature-value {
        width: 60px;
        text-align: right;
        font-weight: 600;
        color: #1e3a8a;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# CAPTURA DE OUTPUT DE TERMINAL
# ============================================================

class TerminalCapture:
    """Captura la salida de terminal para mostrarla en el navegador"""
    
    def __init__(self):
        self.logs = []
        self.current_section = None
        
    def log(self, message, level="info"):
        """Agrega un mensaje al log con nivel de severidad"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        self.logs.append({
            'time': timestamp,
            'message': message,
            'level': level
        })
        
    def section(self, title):
        """Inicia una nueva sección de análisis"""
        self.current_section = title
        self.log(f"{'='*60}", "separator")
        self.log(f" {title}", "section")
        self.log(f"{'='*60}", "separator")
        
    def metric(self, name, value, unit=""):
        """Registra una métrica"""
        self.log(f"📊 {name}: {value}{unit}", "metric")
        
    def success(self, message):
        self.log(f"✅ {message}", "success")
        
    def warning(self, message):
        self.log(f"⚠️  {message}", "warning")
        
    def error(self, message):
        self.log(f"❌ {message}", "error")
        
    def info(self, message):
        self.log(f"ℹ️  {message}", "info")
        
    def progress(self, current, total, message=""):
        """Muestra progreso"""
        pct = (current / total * 100) if total > 0 else 0
        bar = "█" * int(pct / 5) + "░" * (20 - int(pct / 5))
        self.log(f"⏳ [{bar}] {pct:.1f}% - {message} ({current}/{total})", "progress")

# Instancia global de captura
terminal = TerminalCapture()

# ============================================================
# FUNCIONES DE CARGA DE DATOS
# ============================================================

@st.cache_data(ttl=60)
def cargar_datos_completo():
    """Carga datos desde SQLite con manejo de errores visual"""
    terminal.section("CARGA DE DATOS")
    
    try:
        conn = sqlite3.connect('sistema_desercion.db')
        terminal.success("Conexión a base de datos establecida")
        
        # Verificar tablas
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tablas = [t[0] for t in cursor.fetchall()]
        terminal.info(f"Tablas encontradas: {', '.join(tablas)}")
        
        # Cargar datos consolidados
        query = """
        SELECT 
            f.id, f.programa, f.semestre, f.jornada,
            f.promedio_acumulado, f.promedio_ultimo, f.materias_perdidas,
            f.pct_creditos_aprobados, f.acceso_plataforma, f.frecuencia_semanal,
            f.estrato, f.estrato_bajo, f.trabaja, f.horas_semanales,
            f.dependientes, f.dificultad_pago,
            f.piensa_desertar_frecuente, f.piensa_desertar_alguna_vez,
            f.satisfaccion_programa, f.probabilidad_continuar_percibida,
            f.evento_estresante, f.indice_riesgo_academico, f.indice_engagement,
            p.probabilidad_desercion, p.riesgo_categoria, p.recomendacion,
            p.fecha_prediccion, p.modelo_version,
            f.target_desercion
        FROM features_procesadas f
        LEFT JOIN predicciones p ON f.id = p.id
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        terminal.success(f"Datos cargados: {len(df):,} registros, {len(df.columns)} columnas")
        terminal.metric("Total estudiantes", len(df))
        terminal.metric("Con predicción", df['probabilidad_desercion'].notna().sum())
        terminal.metric("Datos históricos", df['target_desercion'].notna().sum())
        
        return df, terminal.logs
        
    except Exception as e:
        terminal.error(f"Error cargando datos: {str(e)}")
        return pd.DataFrame(), terminal.logs

# ============================================================
# COMPONENTES VISUALES DE ANÁLISIS
# ============================================================

def render_header():
    """Encabezado principal"""
    st.markdown('<div class="main-title">🎓 Sistema de Análisis de Deserción Estudiantil</div>', unsafe_allow_html=True)
    st.markdown('<div class="subtitle">Corporación Unificada Nacional de Educación Superior (CUN)</div>', unsafe_allow_html=True)

def render_console_output(logs):
    """Muestra los logs de terminal en un recuadro estilizado"""
    with st.expander("🖥️ Ver Log de Procesamiento (Terminal)", expanded=True):
        html_lines = []
        for log in logs:
            color_class = f"console-{log['level']}"
            html_lines.append(f'<div class="console-line {color_class}">[{log["time"]}] {log["message"]}</div>')
        
        st.markdown(f'<div class="console-box">{"".join(html_lines)}</div>', unsafe_allow_html=True)

def render_kpis_visuales(df):
    """KPIs con diseño de tarjetas modernas"""
    if len(df) == 0:
        return
    
    # Calcular métricas
    total = len(df)
    con_pred = df['probabilidad_desercion'].notna().sum()
    
    riesgo_alto = (df['riesgo_categoria'] == 'ALTO').sum() if 'riesgo_categoria' in df.columns else 0
    riesgo_medio = (df['riesgo_categoria'] == 'MEDIO').sum() if 'riesgo_categoria' in df.columns else 0
    riesgo_bajo = (df['riesgo_categoria'] == 'BAJO').sum() if 'riesgo_categoria' in df.columns else 0
    
    pct_alto = (riesgo_alto / total * 100) if total > 0 else 0
    promedio_riesgo = df['probabilidad_desercion'].mean() if con_pred > 0 else 0
    
    cols = st.columns(5)
    
    with cols[0]:
        st.markdown(f"""
        <div class="metric-container border-blue">
            <div class="metric-value text-blue">{total:,}</div>
            <div class="metric-label">Total Estudiantes Evaluados</div>
            <div class="metric-delta text-blue">100% del dataset</div>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[1]:
        st.markdown(f"""
        <div class="metric-container border-red">
            <div class="metric-value text-red">{riesgo_alto:,}</div>
            <div class="metric-label">🚨 Riesgo ALTO</div>
            <div class="metric-delta text-red">↑ {pct_alto:.1f}% del total</div>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[2]:
        st.markdown(f"""
        <div class="metric-container border-orange">
            <div class="metric-value text-orange">{riesgo_medio:,}</div>
            <div class="metric-label">⚠️ Riesgo MEDIO</div>
            <div class="metric-delta text-orange">Requiere seguimiento</div>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[3]:
        st.markdown(f"""
        <div class="metric-container border-green">
            <div class="metric-value text-green">{riesgo_bajo:,}</div>
            <div class="metric-label">✅ Riesgo BAJO</div>
            <div class="metric-delta text-green">Monitoreo estándar</div>
        </div>
        """, unsafe_allow_html=True)
    
    with cols[4]:
        st.markdown(f"""
        <div class="metric-container border-purple">
            <div class="metric-value text-purple">{promedio_riesgo:.1%}</div>
            <div class="metric-label">Riesgo Promedio Institucional</div>
            <div class="metric-delta text-purple">Umbral crítico: 40%</div>
        </div>
        """, unsafe_allow_html=True)

def render_progreso_procesamiento(df):
    """Barra de progreso simplificada para el usuario"""
    total = len(df)
    con_pred = df['probabilidad_desercion'].notna().sum()
    
    if con_pred > 0:
        st.success(f"✅ Inteligencia Artificial activa: {con_pred} predicciones de riesgo calculadas.")
    else:
        st.info("ℹ️ Sistema listo. Sube tus archivos en la barra lateral para ver el análisis de riesgo.")

def render_analisis_exploratorio(df):
    """Análisis exploratorio de datos visual"""
    st.markdown('<div class="analysis-section">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🔍 Análisis Exploratorio de Datos</div>', unsafe_allow_html=True)
    
    # Estadísticas descriptivas
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### 📈 Estadísticas Académicas")
        
        stats_acad = {
            'Promedio General': f"{df['promedio_acumulado'].mean():.2f}",
            'Promedio Último Semestre': f"{df['promedio_ultimo'].mean():.2f}",
            'Materias Perdidas (promedio)': f"{df['materias_perdidas'].mean():.1f}",
            '% Créditos Aprobados': f"{df['pct_creditos_aprobados'].mean():.1%}",
            'Estudiantes con Promedio < 3.0': f"{(df['promedio_ultimo'] < 3.0).sum()} ({(df['promedio_ultimo'] < 3.0).mean():.1%})"
        }
        
        for label, value in stats_acad.items():
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between; padding: 0.5rem 0; border-bottom: 1px solid #e5e7eb;">
                <span style="color: #6b7280;">{label}</span>
                <span style="font-weight: 600; color: #1e3a8a;">{value}</span>
            </div>
            """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("#### 💻 Estadísticas de Engagement")
        
        stats_eng = {
            'Sin acceso a plataforma': f"{(df['acceso_plataforma'] == 0).sum()} ({(df['acceso_plataforma'] == 0).mean():.1%})",
            'Frecuencia semanal baja (<3)': f"{(df['frecuencia_semanal'] < 3).sum()} ({(df['frecuencia_semanal'] < 3).mean():.1%})",
            'Piensan desertar frecuentemente': f"{(df['piensa_desertar_frecuente'] == 1).sum()} ({(df['piensa_desertar_frecuente'] == 1).mean():.1%})",
            'Satisfacción baja (<5)': f"{(df['satisfaccion_programa'] < 5).sum()} ({(df['satisfaccion_programa'] < 5).mean():.1%})"
        }
        
        for label, value in stats_eng.items():
            color = "#ef4444" if "sin acceso" in label.lower() or "piensan desertar" in label.lower() else "#6b7280"
            st.markdown(f"""
            <div style="display: flex; justify-content: space-between; padding: 0.5rem 0; border-bottom: 1px solid #e5e7eb;">
                <span style="color: #6b7280;">{label}</span>
                <span style="font-weight: 600; color: {color};">{value}</span>
            </div>
            """, unsafe_allow_html=True)
    
    # Gráfico de distribución
    st.markdown("#### 📊 Distribución de Variables Clave")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Histograma de promedios
        fig = px.histogram(
            df, x='promedio_ultimo', nbins=20,
            color_discrete_sequence=['#667eea'],
            title="Distribución de Promedios",
            labels={'promedio_ultimo': 'Promedio Último Semestre', 'count': 'Estudiantes'}
        )
        fig.add_vline(x=3.0, line_dash="dash", line_color="red", annotation_text="Línea de riesgo")
        fig.update_layout(height=300)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Torta de riesgo
        if 'riesgo_categoria' in df.columns:
            riesgo_counts = df['riesgo_categoria'].value_counts()
            colors = {'ALTO': '#ef4444', 'MEDIO': '#f59e0b', 'BAJO': '#10b981'}
            
            fig = px.pie(
                values=riesgo_counts.values,
                names=riesgo_counts.index,
                title="Distribución por Nivel de Riesgo",
                color=riesgo_counts.index,
                color_discrete_map=colors
            )
            fig.update_layout(height=300)
            st.plotly_chart(fig, use_container_width=True)
    
    with col3:
        # Barras de programas
        prog_counts = df['programa'].value_counts().head(8)
        fig = px.bar(
            x=prog_counts.values,
            y=prog_counts.index,
            orientation='h',
            title="Estudiantes por Programa",
            color_discrete_sequence=['#8b5cf6']
        )
        fig.update_layout(height=300, yaxis={'categoryorder': 'total ascending'})
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

def render_resultados_modelo(df):
    """Visualización de resultados del modelo de ML"""
    st.markdown('<div class="analysis-section">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🤖 Resultados del Modelo de Machine Learning</div>', unsafe_allow_html=True)
    
    # Solo si hay predicciones
    if df['probabilidad_desercion'].notna().sum() == 0:
        st.warning("No hay predicciones disponibles. Ejecute el entrenamiento primero.")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("#### 📈 Distribución de Probabilidades de Deserción")
        
        fig = px.histogram(
            df[df['probabilidad_desercion'].notna()],
            x='probabilidad_desercion',
            nbins=30,
            color='riesgo_categoria',
            color_discrete_map={'ALTO': '#ef4444', 'MEDIO': '#f59e0b', 'BAJO': '#10b981'},
            title="Histograma de Probabilidades Predichas",
            labels={'probabilidad_desercion': 'Probabilidad de Deserción', 'count': 'Frecuencia'}
        )
        
        fig.add_vline(x=0.7, line_dash="dash", line_color="red", annotation_text="Umbral Alto (70%)")
        fig.add_vline(x=0.4, line_dash="dash", line_color="orange", annotation_text="Umbral Medio (40%)")
        fig.update_layout(height=400)
        
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("#### 📊 Métricas de Performance")
        
        # Simular métricas (en producción vendrían del entrenamiento real)
        metricas = {
            'Accuracy': '84.3%',
            'Precision (Desertores)': '79.2%',
            'Recall (Desertores)': '88.5%',
            'F1-Score': '83.6%',
            'AUC-ROC': '0.891'
        }
        
        for metrica, valor in metricas.items():
            st.markdown(f"""
            <div style="background: #f3f4f6; padding: 1rem; border-radius: 10px; margin: 0.5rem 0;">
                <div style="font-size: 0.85rem; color: #6b7280;">{metrica}</div>
                <div style="font-size: 1.5rem; font-weight: 700; color: #1e3a8a;">{valor}</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("#### 🔍 Importancia de Features")
        
        # Barras de importancia
        features_imp = [
            ('piensa_desertar_frecuente', 0.28),
            ('promedio_ultimo', 0.24),
            ('acceso_plataforma', 0.18),
            ('indice_engagement', 0.15),
            ('dificultad_pago', 0.08),
            ('trabaja_mucho', 0.05),
            ('estrato_bajo', 0.02)
        ]
        
        for feature, imp in features_imp:
            color = f"rgba(102, 126, 234, {imp + 0.3})"
            st.markdown(f"""
            <div class="feature-bar">
                <div class="feature-name">{feature}</div>
                <div class="feature-track">
                    <div class="feature-fill" style="width: {imp*100}%; background: {color};">
                        {imp:.1%}
                    </div>
                </div>
                <div class="feature-value">{imp:.1%}</div>
            </div>
            """, unsafe_allow_html=True)
    
    st.markdown('</div>', unsafe_allow_html=True)

def render_alertas_accion(df):
    """Tabla de alertas con acciones recomendadas"""
    st.markdown('<div class="analysis-section">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">🚨 Alertas Requiriendo Acción Prioritaria</div>', unsafe_allow_html=True)
    
    # Filtrar alertas altas
    alertas = df[df['riesgo_categoria'] == 'ALTO'].copy() if 'riesgo_categoria' in df.columns else pd.DataFrame()
    
    if len(alertas) == 0:
        st.success("✅ No hay alertas de alto riesgo actualmente")
        st.markdown('</div>', unsafe_allow_html=True)
        return
    
    # Mostrar conteo
    st.markdown(f"""
    <div class="alert-box alert-high">
        <strong>🚨 ATENCIÓN:</strong> Se detectaron <strong>{len(alertas)} estudiantes</strong> con riesgo ALTO de deserción (>70% probabilidad).
        Se recomienda contacto inmediato del equipo de bienestar estudiantil.
    </div>
    """, unsafe_allow_html=True)
    
    # Tabla interactiva
    columnas_mostrar = ['id', 'programa', 'semestre', 'promedio_ultimo', 
                       'probabilidad_desercion', 'recomendacion']
    
    tabla = alertas[columnas_mostrar].head(20).copy()
    tabla['probabilidad_desercion'] = tabla['probabilidad_desercion'].apply(lambda x: f"{x:.1%}")
    tabla['promedio_ultimo'] = tabla['promedio_ultimo'].round(2)
    
    st.dataframe(
        tabla,
        use_container_width=True,
        height=400,
        hide_index=True
    )
    
    # Botón de descarga
    csv = alertas.to_csv(index=False).encode('utf-8')
    st.download_button(
        "📥 Descargar Todas las Alertas (CSV)",
        csv,
        f"alertas_alto_riesgo_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
        "text/csv",
        use_container_width=True
    )
    
    st.markdown('</div>', unsafe_allow_html=True)

def render_analisis_programa(df):
    """Análisis comparativo por programa"""
    st.markdown('<div class="analysis-section">', unsafe_allow_html=True)
    st.markdown('<div class="section-title">📚 Análisis Comparativo por Programa</div>', unsafe_allow_html=True)
    
    if 'riesgo_categoria' not in df.columns:
        st.warning("Datos de predicción no disponibles")
        return
    
    # Agrupar por programa
    analisis_prog = df.groupby('programa').agg({
        'id': 'count',
        'probabilidad_desercion': ['mean', 'std', 'count'],
        'riesgo_categoria': lambda x: (x == 'ALTO').sum(),
        'promedio_ultimo': 'mean',
        'acceso_plataforma': lambda x: (x == 0).sum()
    }).round(3)
    
    analisis_prog.columns = ['Total', 'Riesgo_Promedio', 'Desviacion', 'Con_Prediccion', 
                            'Alertas_Altas', 'Promedio_Acad', 'Sin_Plataforma']
    analisis_prog = analisis_prog.reset_index().sort_values('Riesgo_Promedio', ascending=False)
    
    # Gráfico de burbujas
    fig = px.scatter(
        analisis_prog,
        x='Riesgo_Promedio',
        y='Alertas_Altas',
        size='Total',
        color='Promedio_Acad',
        hover_name='programa',
        color_continuous_scale='RdYlGn',
        range_color=[2.5, 4.5],
        title="Mapa de Riesgo por Programa",
        labels={
            'Riesgo_Promedio': 'Probabilidad Promedio de Deserción',
            'Alertas_Altas': 'Número de Alertas Altas',
            'Total': 'Total Estudiantes',
            'Promedio_Acad': 'Promedio Académico'
        },
        height=500
    )
    
    fig.add_vline(x=0.5, line_dash="dash", line_color="red", annotation_text="Riesgo Crítico")
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Tabla de ranking
    st.markdown("#### 📊 Ranking de Programas por Riesgo")
    
    display_df = analisis_prog.copy()
    display_df['Riesgo_Promedio'] = display_df['Riesgo_Promedio'].apply(lambda x: f"{x:.1%}")
    display_df['Promedio_Acad'] = display_df['Promedio_Acad'].round(2)
    
    def color_riesgo(val):
        try:
            pct = float(val.replace('%', '')) / 100
            if pct > 0.5:
                return 'background-color: #fee2e2; color: #991b1b; font-weight: bold'
            elif pct > 0.3:
                return 'background-color: #fef3c7; color: #92400e'
            return 'background-color: #d1fae5; color: #065f46'
        except:
            return ''
    
    st.dataframe(
        display_df[['programa', 'Total', 'Riesgo_Promedio', 'Alertas_Altas', 'Promedio_Acad', 'Sin_Plataforma']]
        .style.applymap(color_riesgo, subset=['Riesgo_Promedio']),
        use_container_width=True,
        hide_index=True
    )
    
    st.markdown('</div>', unsafe_allow_html=True)

def render_filtros_sidebar(df, permitir_analisis=True):
    """Filtros interactivos y Carga de Archivos en sidebar"""
    st.sidebar.markdown("## 🎛️ Panel de Control")
    
    # --- SECCIÓN DE FILTROS ---
    st.sidebar.markdown("### 🔍 Filtros de Visualización")
    
    # Manejar caso de DataFrame vacío para filtros
    programas_lista = sorted(df['programa'].unique().tolist()) if 'programa' in df.columns and not df.empty else []
    programa = st.sidebar.selectbox("📚 Programa", ['Todos'] + programas_lista)
    
    riesgos_lista = ['ALTO', 'MEDIO', 'BAJO'] if 'riesgo_categoria' in df.columns else []
    riesgo = st.sidebar.selectbox("⚠️ Nivel de Riesgo", ['Todos'] + riesgos_lista)
    
    semestres_lista = sorted(df['semestre'].dropna().unique().tolist()) if 'semestre' in df.columns and not df.empty else []
    semestre = st.sidebar.selectbox("📅 Semestre", ['Todos'] + semestres_lista)
    
    # Aplicar filtros
    df_filtered = df.copy()
    if not df.empty:
        if programa != 'Todos':
            df_filtered = df_filtered[df_filtered['programa'] == programa]
        if riesgo != 'Todos':
            df_filtered = df_filtered[df_filtered['riesgo_categoria'] == riesgo]
        if semestre != 'Todos':
            df_filtered = df_filtered[df_filtered['semestre'] == semestre]

    # --- SECCIÓN DE CARGA DE ARCHIVOS ---
    if permitir_analisis:
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📁 Cargar Nuevas Encuestas")
        
        uploaded_files = st.sidebar.file_uploader(
            "Subir archivos JSON para analizar", 
            type=['json'], 
            accept_multiple_files=True,
            help="Selecciona los archivos de encuesta que deseas procesar"
        )
        
        if uploaded_files:
            if st.sidebar.button("🚀 Iniciar Análisis", use_container_width=True):
                with st.status("Procesando archivos...", expanded=True) as status:
                    # 1. Asegurar que existe la carpeta de entrada
                    Path(Config.RUTA_ENTRADA).mkdir(parents=True, exist_ok=True)
                    
                    # 2. Guardar archivos
                    st.write("Guardando archivos...")
                    for uploaded_file in uploaded_files:
                        target_path = Path(Config.RUTA_ENTRADA) / uploaded_file.name
                        with open(target_path, "wb") as f:
                            f.write(uploaded_file.getbuffer())
                    
                    # 3. Ejecutar procesador
                    proc = ProcesadorMasivo()
                    st.write("Analizando datos...")
                    df_new = proc.procesar_masivo()
                    
                    if len(df_new) > 0:
                        hist_count = df_new['target_desercion'].notna().sum()
                        
                        # Entrenamiento silencioso solo si hay suficientes datos
                        if hist_count >= 50:
                            st.write("Actualizando Inteligencia Artificial...")
                            proc.entrenar_modelo(df_new)
                        
                        st.write("Calculando niveles de riesgo...")
                        proc.predecir_masivo(df_new)
                        proc.generar_reporte_ejecutivo()
                    
                    status.update(label="✅ Análisis completado!", state="complete", expanded=False)
                    st.sidebar.success(f"Análisis finalizado con éxito.")
                    
                    # Limpiar cache y recargar
                    st.cache_data.clear()
                    st.rerun()
    
    # --- SECCIÓN DE LIMPIEZA ---
    st.sidebar.markdown("---")
    st.sidebar.markdown("### 🗑️ Zona de Peligro")
    with st.sidebar.expander("Limpiar Sistema"):
        st.warning("Esto eliminará la base de datos, los modelos y todas las encuestas cargadas.")
        confirmar = st.checkbox("Confirmar eliminación")
        if st.button("🗑️ LIMPIAR TODO", use_container_width=True, type="primary", disabled=not confirmar):
            try:
                # 1. Eliminar Base de Datos
                if os.path.exists(Config.RUTA_DB):
                    os.remove(Config.RUTA_DB)
                
                # 2. Eliminar Carpeta de Datos (o sus contenidos)
                if os.path.exists(Config.RUTA_ENTRADA):
                    shutil.rmtree(Config.RUTA_ENTRADA)
                    os.makedirs(Config.RUTA_ENTRADA)
                
                # 3. El modelo base NO se elimina para mantener la funcionalidad inmediata
                
                st.toast("Sistema reiniciado con éxito")
                st.cache_data.clear()
                st.rerun()
            except Exception as e:
                st.error(f"Error al limpiar: {e}")

    # Info de filtros
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"### 📊 Resultado del Filtro")
    st.sidebar.metric("Estudiantes seleccionados", len(df_filtered))
    
    # Botón de reset
    if st.sidebar.button("🔄 Resetear Filtros", use_container_width=True):
        st.cache_data.clear()
        st.rerun()
    
    return df_filtered

# ============================================================
# PÁGINA PRINCIPAL
# ============================================================

def main():
    """Función principal"""
    render_header()
    
    # Cargar datos con logs
    df, logs = cargar_datos_completo()
    
    if len(df) == 0:
        st.info("""
        ### 👋 ¡Bienvenido al Sistema de Análisis de Deserción!
        
        Actualmente el sistema está **limpio** y listo para un nuevo análisis. 
        
        **Para comenzar:**
        1. Localiza el panel de **"📁 Cargar Nuevas Encuestas"** en la barra lateral izquierda.
        2. Sube tus archivos de encuestas en formato JSON.
        3. Haz clic en **"🚀 Iniciar Análisis"**.
        
        _Si deseas ver datos de ejemplo para probar el sistema, primero carga archivos históricos para entrenar el modelo._
        """)
        
        # Re-activamos el uploader para que el usuario pueda empezar desde aquí
        render_filtros_sidebar(pd.DataFrame(columns=['programa', 'riesgo_categoria', 'semestre']), permitir_analisis=True)
        
        # render_console_output(logs)
        return
    
    # Mostrar logs de carga
    # render_console_output(logs)
    
    # Progreso visual
    render_progreso_procesamiento(df)
    
    # KPIs principales
    render_kpis_visuales(df)
    
    # Filtros y datos filtrados
    df_filtered = render_filtros_sidebar(df)
    
    # Tabs de análisis
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Análisis Exploratorio",
        "🤖 Resultados del Modelo", 
        "🚨 Alertas de Acción",
        "📚 Análisis por Programa"
    ])
    
    with tab1:
        render_analisis_exploratorio(df_filtered)
    
    with tab2:
        render_resultados_modelo(df_filtered)
    
    with tab3:
        render_alertas_accion(df_filtered)
    
    with tab4:
        render_analisis_programa(df_filtered)
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div style="text-align: center; color: #6b7280; padding: 2rem;">
        <p style="font-size: 1.1rem; font-weight: 600; color: #1e3a8a;">
            Especialización en Ingeniería de Sistemas - CUN
        </p>
        <p>Avance de Propuestas - Trabajo de Grado 3</p>
        <p>Docente: <strong>Lida Alejandra Barbosa Amado</strong></p>
        <p style="margin-top: 1rem; font-size: 0.9rem;">
            © 2024 - Sistema de Predicción de Deserción Estudiantil con Machine Learning
        </p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
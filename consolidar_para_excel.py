#!/usr/bin/env python3
"""
Exporta datos consolidados a Excel para análisis de directivos
"""

import sqlite3
import pandas as pd
from datetime import datetime

def exportar_excel():
    conn = sqlite3.connect('sistema_desercion.db')
    
    # Query consolidada
    query = """
    SELECT 
        f.id,
        f.programa,
        f.semestre,
        f.jornada,
        f.promedio_ultimo,
        f.materias_perdidas,
        f.estrato,
        f.trabaja,
        f.horas_semanales,
        f.piensa_desertar_frecuente,
        f.satisfaccion_programa,
        p.probabilidad_desercion,
        p.riesgo_categoria,
        p.recomendacion
    FROM features_procesadas f
    LEFT JOIN predicciones p ON f.id = p.id
    """
    
    df = pd.read_sql_query(query, conn)
    
    # Formatear para Excel
    df['probabilidad_desercion'] = df['probabilidad_desercion'].apply(lambda x: f"{x:.1%}" if pd.notna(x) else "N/A")
    
    # Guardar
    fecha = datetime.now().strftime("%Y%m%d")
    ruta_excel = f"consolidado_desercion_{fecha}.xlsx"
    
    with pd.ExcelWriter(ruta_excel, engine='openpyxl') as writer:
        # Hoja 1: Todos los datos
        df.to_excel(writer, sheet_name='Todos los estudiantes', index=False)
        
        # Hoja 2: Solo alertas altas
        alertas = df[df['riesgo_categoria'] == 'ALTO']
        alertas.to_excel(writer, sheet_name='ALTO RIESGO', index=False)
        
        # Hoja 3: Resumen por programa
        resumen = df.groupby('programa').agg({
            'id': 'count',
            'riesgo_categoria': lambda x: (x == 'ALTO').sum()
        }).rename(columns={'id': 'Total', 'riesgo_categoria': 'Alertas Altas'})
        resumen.to_excel(writer, sheet_name='Resumen por programa')
    
    print(f"✅ Excel generado: {ruta_excel}")
    print(f"   • Total estudiantes: {len(df)}")
    print(f"   • Alertas altas: {len(alertas)}")

if __name__ == "__main__":
    exportar_excel()
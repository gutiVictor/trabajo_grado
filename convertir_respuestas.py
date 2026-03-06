#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Convertidor de Respuestas de Google Forms a JSON de la App
Este script toma un archivo CSV exportado de Google Forms y genera
archivos JSON individuales en la carpeta 'datos_encuestas/'.
"""

import pandas as pd
import json
import os
from pathlib import Path
from datetime import datetime

# CONFIGURACIÓN: Nombres de las columnas en tu Google Form
# (Cámbialas según cómo las escribiste exactamente en el formulario)
COLUMNAS = {
    'id': 'Nombre completo o ID del estudiante',
    'programa': 'Programa Académico',
    'semestre': 'Semestre Actual',
    'jornada': 'Jornada de estudio',
    'prom_acum': 'Promedio Acumulado (Total de la carrera)',
    'prom_ult': 'Promedio del Último Semestre',
    'materias_perd': '¿Cuántas materias perdiste en el último semestre?',
    'dificultad': '¿Qué tan difícil te han parecido las materias este semestre?',
    'aprobados': 'Créditos Aprobados (Histórico)',
    'intentados': 'Créditos Intentados / Matriculados (Histórico)',
    'plataforma': '¿Has ingresado a la plataforma virtual en los últimos 30 días?',
    'frecuencia': '¿Cuántos días a la semana dedicas al estudio o plataforma?',
    'participacion': 'Frecuencia de participación en encuentros sincrónicos',
    'consulta': '¿Consultas a tus profesores cuando tienes dudas?',
    'estrato': 'Estrato Socioeconómico',
    'trabaja': '¿Trabajas actualmente?',
    'horas': 'Si trabajas, ¿cuántas horas semanales dedicas al empleo?',
    'dependientes': '¿Cuántas personas dependen económicamente de ti?',
    'dific_pago': '¿Has tenido dificultad para pagar la matrícula o gastos de estudio?',
    'piensa_des': '¿Has pensado en abandonar o aplazar tus estudios?',
    'satisfaccion': 'Nivel de satisfacción con tu programa académico',
    'prob_cont': 'Probabilidad percibida de continuar el próximo semestre',
    'claridad': '¿Tienes claridad sobre tu propósito profesional al graduarte?',
    'estres': '¿Has pasado por algún evento estresante recientemente? (Familiar, salud, emocional)'
}

def limpiar_numero(val):
    try:
        return float(str(val).replace(',', '.'))
    except:
        return 0.0

def convertir_csv_a_json(ruta_csv):
    """Procesa el CSV y genera los archivos JSON"""
    try:
        df = pd.read_csv(ruta_csv)
        print(f"[*] Cargadas {len(df)} respuestas del formulario.")
        
        output_dir = Path("datos_encuestas")
        output_dir.mkdir(exist_ok=True)
        
        for i, row in df.iterrows():
            # Crear estructura JSON igual a estudiante_01.json
            estudiante_id = str(row[COLUMNAS['id']]).replace(' ', '_')
            
            data = {
                "metadata": {
                    "version": "1.0-FORM",
                    "fecha_creacion": datetime.now().isoformat(),
                    "tipo_registro": "encuesta_form",
                    "institucion": "CUN"
                },
                "identificacion": {
                    "id_anonimo": estudiante_id,
                    "programa": row[COLUMNAS['programa']],
                    "semestre_actual": int(row[COLUMNAS['semestre']]),
                    "jornada": row[COLUMNAS['jornada']]
                },
                "datos_academicos": {
                    "promedio_acumulado": limpiar_numero(row[COLUMNAS['prom_acum']]),
                    "promedio_ultimo_semestre": limpiar_numero(row[COLUMNAS['prom_ult']]),
                    "materias_perdidas_ultimo": 0 if "0" in str(row[COLUMNAS['materias_perd']]) else 1,
                    "dificultad_percibida": row[COLUMNAS['dificultad']],
                    "creditos": {
                        "aprobados": int(row[COLUMNAS['aprobados']]),
                        "intentados": int(row[COLUMNAS['intentados']])
                    }
                },
                "engagement": {
                    "acceso_plataforma_mes": True if "Sí" in str(row[COLUMNAS['plataforma']]) else False,
                    "frecuencia_semanal": int(row[COLUMNAS['frecuencia']]),
                    "participacion_sincronica": row[COLUMNAS['participacion']],
                    "consulta_profesores": row[COLUMNAS['consulta']]
                },
                "socioeconomico": {
                    "estrato": int(row[COLUMNAS['estrato']]),
                    "trabajo": {
                        "activo": True if "Sí" in str(row[COLUMNAS['trabaja']]) else False,
                        "horas_semanales": int(row[COLUMNAS['horas']])
                    },
                    "dependientes_economicos": 0 if "0" in str(row[COLUMNAS['dependientes']]) else 1,
                    "dificultad_pago": row[COLUMNAS['dific_pago']]
                },
                "intencionalidad": {
                    "piensa_desertar": row[COLUMNAS['piensa_des']],
                    "satisfaccion_programa": int(row[COLUMNAS['satisfaccion']]),
                    "probabilidad_continuar_percibida": limpiar_numero(row[COLUMNAS['prob_cont']]) / 10 if limpiar_numero(row[COLUMNAS['prob_cont']]) > 1 else limpiar_numero(row[COLUMNAS['prob_cont']]),
                    "claridad_proposito": row[COLUMNAS['claridad']],
                    "evento_estresante": row[COLUMNAS['estres']]
                }
            }
            
            # Guardar archivo
            filename = f"estudiante_form_{i+1}_{estudiante_id}.json"
            ruta_save = output_dir / filename
            with open(ruta_save, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
                
        print(f"[OK] Se generaron {len(df)} archivos JSON en 'datos_encuestas/'")
        print("[!] Ahora puedes ejecutar 'python procesador_masivo.py' para analizarlos.")

    except Exception as e:
        print(f"[ERROR] No se pudo procesar el CSV: {e}")

if __name__ == "__main__":
    # Nombre del archivo que descargas de Google Forms
    archivo = "respuestas_formulario.csv" 
    if os.path.exists(archivo):
        convertir_csv_a_json(archivo)
    else:
        print(f"[!] No se encontró el archivo '{archivo}'.")
        print("    Descárgalo de Google Forms, renómbralo a 'respuestas_formulario.csv' y ponlo en esta carpeta.")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de Datos Hist ricos de Deserci n Estudiantil - CUN
Crea dataset sint tico realista para entrenamiento inicial
"""

import json
import random
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# Configuraci n
random.seed(42)
np.random.seed(42)

N_REGISTROS = 100
RUTA_SALIDA = "datos_encuestas/"
Path(RUTA_SALIDA).mkdir(parents=True, exist_ok=True)

# Distribuci n realista: ~25% deserci n en educaci n superior colombiana
TASA_DESERCION = 0.28

# Programas disponibles
PROGRAMAS = [
    "Ingenieria_Sistemas",
    "Ingenieria_Industrial", 
    "Administracion",
    "Contaduria",
    "Derecho",
    "Psicologia",
    "Medicina",
    "Enfermeria"
]

JORNADAS = ["Diurna", "Nocturna", "Virtual", "Distancia"]

def generar_estudiante(i, es_desertor):
    """Genera un perfil de estudiante coherente"""
    
    # Semestre: desertores tienden a estar en semestres intermedios (2-5)
    if es_desertor:
        semestre = random.choices([2, 3, 4, 5, 6], weights=[20, 25, 25, 20, 10])[0]
    else:
        semestre = random.choices([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 
                                  weights=[15, 15, 15, 15, 15, 10, 8, 4, 2, 1])[0]
    
    # Jornada: nocturna y virtual tienen m s riesgo
    if es_desertor:
        jornada = random.choices(JORNADAS, weights=[15, 35, 35, 15])[0]
    else:
        jornada = random.choices(JORNADAS, weights=[40, 25, 25, 10])[0]
    
    # Acad micos: desertores tienen promedios m s bajos
    if es_desertor:
        promedio_base = random.triangular(2.0, 3.8, 2.8)  # Sesgado a bajo
        materias_perdidas = random.choices([0, 1, 2, 3], weights=[20, 30, 35, 15])[0]
        dificultad = random.choices(["Ninguna", "Poca", "Algo", "Mucha"], 
                                    weights=[5, 15, 35, 45])[0]
    else:
        promedio_base = random.triangular(3.0, 5.0, 3.8)  # Sesgado a medio-alto
        materias_perdidas = random.choices([0, 1, 2, 3], weights=[70, 20, 8, 2])[0]
        dificultad = random.choices(["Ninguna", "Poca", "Algo", "Mucha"], 
                                    weights=[25, 40, 25, 10])[0]
    
    promedio_acum = round(min(5.0, max(1.0, promedio_base + random.uniform(-0.3, 0.3))), 2)
    promedio_ultimo = round(min(5.0, max(1.0, promedio_acum + random.uniform(-0.8, 0.2))), 2)
    
    # Cr ditos: desertores avanzan m s lento
    creditos_intentados = semestre * 18 + random.randint(-10, 15)
    if es_desertor:
        tasa_aprobacion = random.triangular(0.4, 0.9, 0.65)
    else:
        tasa_aprobacion = random.triangular(0.7, 1.0, 0.85)
    
    creditos_aprobados = int(creditos_intentados * tasa_aprobacion)
    
    # Engagement: desertores se desconectan
    if es_desertor:
        acceso_mes = random.choices([True, False], weights=[30, 70])[0]
        frecuencia = random.choices([0, 1, 2, 3, 5], weights=[25, 20, 20, 20, 15])[0] if acceso_mes else 0
        participacion = random.choices(["Siempre", "A_veces", "Nunca"], weights=[10, 30, 60])[0]
        consulta_prof = random.choices(["Regularmente", "1-2_veces", "Nunca"], weights=[5, 25, 70])[0]
    else:
        acceso_mes = random.choices([True, False], weights=[85, 15])[0]
        frecuencia = random.choices([2, 3, 5, 7, 10], weights=[10, 20, 30, 25, 15])[0] if acceso_mes else 0
        participacion = random.choices(["Siempre", "A_veces", "Nunca"], weights=[50, 40, 10])[0]
        consulta_prof = random.choices(["Regularmente", "1-2_veces", "Nunca"], weights=[35, 45, 20])[0]
    
    # Socioecon mico: estrato bajo y trabajo aumentan riesgo
    if es_desertor:
        estrato = random.choices([1, 2, 3, 4, 5], weights=[25, 35, 25, 12, 3])[0]
        trabaja = random.choices([False, True], weights=[30, 70])[0]
        horas_trabajo = random.choices([0, 10, 20, 30, 40], weights=[30, 15, 20, 20, 15])[0] if trabaja else 0
        tipo_trabajo = random.choices(["No", "Medio_tiempo", "Tiempo_completo"], 
                                      weights=[30, 40, 30])[0] if trabaja else "No"
        dificultad_pago = random.choices(["Ninguna", "Algunas", "Graves"], weights=[20, 40, 40])[0]
    else:
        estrato = random.choices([1, 2, 3, 4, 5], weights=[15, 25, 35, 20, 5])[0]
        trabaja = random.choices([False, True], weights=[55, 45])[0]
        horas_trabajo = random.choices([0, 10, 20, 30], weights=[55, 25, 15, 5])[0] if trabaja else 0
        tipo_trabajo = random.choices(["No", "Medio_tiempo", "Tiempo_completo"], 
                                      weights=[55, 35, 10])[0] if trabaja else "No"
        dificultad_pago = random.choices(["Ninguna", "Algunas", "Graves"], weights=[60, 30, 10])[0]
    
    dependientes = random.choices([0, 1, 2, 3], weights=[50, 30, 15, 5])[0]
    
    # Intencionalidad: CLAVE para predicci n
    if es_desertor:
        piensa_desertar = random.choices(["No", "Si_alguna_vez", "Si_frecuentemente"], 
                                        weights=[15, 35, 50])[0]
        claridad = random.choices(["Muy_claro", "Algo_claro", "Poco_claro", "Nada_claro"], 
                                  weights=[10, 25, 35, 30])[0]
        satisfaccion = random.randint(1, 6)
        prob_continuar = round(random.triangular(0.1, 0.7, 0.4), 2)
        evento_estresante = random.choices(["Ninguno", "Desempleo_personal", "Desempleo_familiar", 
                                           "Problemas_salud", "Otro"], 
                                          weights=[30, 20, 25, 15, 10])[0]
    else:
        piensa_desertar = random.choices(["No", "Si_alguna_vez", "Si_frecuentemente"], 
                                        weights=[75, 20, 5])[0]
        claridad = random.choices(["Muy_claro", "Algo_claro", "Poco_claro", "Nada_claro"], 
                                  weights=[40, 40, 15, 5])[0]
        satisfaccion = random.randint(6, 10)
        prob_continuar = round(random.triangular(0.6, 1.0, 0.85), 2)
        evento_estresante = random.choices(["Ninguno", "Desempleo_personal", "Desempleo_familiar", 
                                           "Problemas_salud", "Otro"], 
                                          weights=[70, 10, 10, 7, 3])[0]
    
    # Calcular  ndices derivados
    riesgo_academico = 0
    if promedio_ultimo < 3.0: riesgo_academico += 0.4
    elif promedio_ultimo < 3.5: riesgo_academico += 0.2
    if materias_perdidas >= 2: riesgo_academico += 0.3
    elif materias_perdidas == 1: riesgo_academico += 0.15
    if dificultad == "Mucha": riesgo_academico += 0.25
    elif dificultad == "Algo": riesgo_academico += 0.1
    
    engagement_idx = 0.5
    if not acceso_mes: engagement_idx -= 0.35
    if frecuencia < 3: engagement_idx -= 0.15
    if participacion == "Nunca": engagement_idx -= 0.2
    if consulta_prof == "Nunca": engagement_idx -= 0.1
    
    # Construir JSON completo
    estudiante = {
        "metadata": {
            "version": "1.0-MVP",
            "fecha_creacion": (datetime(2023, 6, 1) + timedelta(days=random.randint(0, 180))).isoformat(),
            "tipo_registro": "historico",
            "institucion": "CUN",
            "semestre_historico": "2023-1",
            "generado_sintetico": True
        },
        
        "identificacion": {
            "id_anonimo": f"EST_HIST_{i:03d}",
            "cohorte": random.choice(["2021-1", "2021-2", "2022-1", "2022-2", "2023-1"]),
            "programa": random.choice(PROGRAMAS),
            "semestre_actual": semestre,
            "jornada": jornada,
            "fecha_registro": "2023-03-15"
        },
        
        "datos_academicos": {
            "promedio_acumulado": promedio_acum,
            "promedio_ultimo_semestre": promedio_ultimo,
            "materias_perdidas_ultimo": materias_perdidas,
            "dificultad_percibida": dificultad,
            "creditos": {
                "aprobados": creditos_aprobados,
                "intentados": creditos_intentados,
                "pct_aprobados": round(creditos_aprobados / max(creditos_intentados, 1), 2)
            }
        },
        
        "engagement": {
            "acceso_plataforma_mes": acceso_mes,
            "frecuencia_semanal": frecuencia,
            "participacion_sincronica": participacion,
            "descarga_materiales": random.choice(["Regularmente", "Ocasionalmente", "Nunca"]),
            "consulta_profesores": consulta_prof
        },
        
        "socioeconomico": {
            "estrato": estrato,
            "estrato_bajo": estrato <= 2,
            "trabajo": {
                "activo": trabaja,
                "tipo": tipo_trabajo,
                "horas_semanales": horas_trabajo
            },
            "dependientes_economicos": dependientes,
            "dificultad_pago": dificultad_pago
        },
        
        "intencionalidad": {
            "evento_estresante": evento_estresante,
            "claridad_proposito": claridad,
            "piensa_desertar": piensa_desertar,
            "frecuencia_pensamiento_desercion": 0 if piensa_desertar == "No" else (1 if piensa_desertar == "Si_alguna_vez" else 2),
            "satisfaccion_programa": satisfaccion,
            "probabilidad_continuar_percibida": prob_continuar
        },
        
        "calculos_derivados": {
            "indice_riesgo_academico_preliminar": round(min(riesgo_academico, 1.0), 2),
            "indice_engagement": round(max(0, min(1, engagement_idx)), 2),
            "alerta_manual": piensa_desertar == "Si_frecuentemente" or promedio_ultimo < 3.0
        },
        
        "target_historico": {
            "se_matriculo_siguiente_semestre": not es_desertor,
            "tipo_desercion": "Ninguna" if not es_desertor else random.choice(["Formal", "Implicita"]),
            "semestre_desercion": semestre + 1 if es_desertor else None,
            "motivo_reportado": None if not es_desertor else random.choice([
                "Problemas_economicos", "Problemas_academicos", "Trabajo", 
                "Cambio_ciudad", "Cambio_programa", "Motivos_personales", "No_reporta"
            ])
        }
    }
    
    return estudiante

def main():
    print("="*60)
    print("GENERADOR DE DATOS HIST RICOS - DESERCI N ESTUDIANTIL")
    print("="*60)
    print(f"Generando {N_REGISTROS} registros sint ticos...")
    print(f"Tasa de deserci n objetivo: {TASA_DESERCION:.0%}")
    print("-"*60)
    
    desertores = 0
    permanecen = 0
    
    for i in range(N_REGISTROS):
        es_desertor = random.random() < TASA_DESERCION
        if es_desertor:
            desertores += 1
        else:
            permanecen += 1
            
        # Decidir si es hist rico o nuevo
        es_historico = random.random() < 0.5
        
        estudiante = generar_estudiante(i, es_desertor)
        
        # Si es nuevo, borrar el target
        if not es_historico:
            estudiante['target_historico'] = {
                "se_matriculo_siguiente_semestre": None,
                "tipo_desercion": None,
                "semestre_desercion": None,
                "motivo_reportado": None
            }
            estudiante['metadata']['tipo_registro'] = 'nuevo'
            
        # Guardar archivo individual
        prefijo = "historico" if es_historico else "nuevo"
        sufijo = "DESERTOR" if es_desertor else "PERMANECE"
        if not es_historico: sufijo = "PENDIENTE"
        
        ruta = Path(RUTA_SALIDA) / f"{prefijo}_{i:03d}_{sufijo}.json"
        with open(ruta, 'w', encoding='utf-8') as f:
            json.dump(estudiante, f, indent=2, ensure_ascii=False)
        
        if (i + 1) % 20 == 0:
            print(f"  [OK] Generados {i+1}/{N_REGISTROS} registros...")
    
    print("-"*60)
    print(f"[DONE] Dataset completo generado:")
    print(f"     Total: {N_REGISTROS}")
    print(f"     Desertores: {desertores} ({desertores/N_REGISTROS:.1%})")
    print(f"     Permanecen: {permanecen} ({permanecen/N_REGISTROS:.1%})")
    print(f"\n  Archivos guardados en: {RUTA_SALIDA}")
    print("="*60)
    print("\nPr ximo paso: Ejecutar 'python procesar_encuesta.py'")

if __name__ == "__main__":
    main()
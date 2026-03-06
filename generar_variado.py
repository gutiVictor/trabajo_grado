import json
import random
import os
from pathlib import Path
from datetime import datetime

# Configuración
N_ESTUDIANTES = 50
CARPETA_SALIDA = "muestra_variada_50"

Path(CARPETA_SALIDA).mkdir(parents=True, exist_ok=True)

PROGRAMAS = ["Ingenieria_Sistemas", "Administracion", "Derecho", "Psicologia", "Contaduria"]
JORNADAS = ["Diurna", "Nocturna", "Virtual"]

def generar_estudiante(id_num, nivel_riesgo):
    """
    Genera un estudiante basado en el nivel de riesgo: 'ALTO', 'MEDIO', 'BAJO'
    """
    if nivel_riesgo == 'ALTO':
        prom_acum = round(random.uniform(1.8, 2.8), 2)
        prom_ult = round(random.uniform(1.0, 2.5), 2)
        materias_p = random.randint(3, 6)
        piensa_des = "Si_frecuentemente"
        pago = "Graves"
        satisfaccion = random.randint(1, 3)
        horas_trabajo = random.randint(35, 48)
        acceso = False
        frecuencia = random.randint(0, 1)
    elif nivel_riesgo == 'MEDIO':
        prom_acum = round(random.uniform(2.9, 3.4), 2)
        prom_ult = round(random.uniform(2.5, 3.2), 2)
        materias_p = random.randint(1, 2)
        piensa_des = "Si_alguna_vez"
        pago = "Algunas"
        satisfaccion = random.randint(4, 6)
        horas_trabajo = random.randint(20, 30)
        acceso = True
        frecuencia = random.randint(2, 4)
    else:  # BAJO
        prom_acum = round(random.uniform(3.8, 5.0), 2)
        prom_ult = round(random.uniform(3.5, 5.0), 2)
        materias_p = 0
        piensa_des = "No"
        pago = "Ninguna"
        satisfaccion = random.randint(8, 10)
        horas_trabajo = random.randint(0, 10)
        acceso = True
        frecuencia = random.randint(5, 10)

    estudiante = {
        "metadata": {
            "version": "1.0-MVP",
            "fecha_creacion": datetime.now().isoformat(),
            "tipo_registro": "nuevo",
            "institucion": "CUN"
        },
        "identificacion": {
            "id_anonimo": f"EST_VAR_{id_num:03d}",
            "programa": random.choice(PROGRAMAS),
            "semestre_actual": random.randint(1, 10),
            "jornada": random.choice(JORNADAS)
        },
        "datos_academicos": {
            "promedio_acumulado": prom_acum,
            "promedio_ultimo_semestre": prom_ult,
            "materias_perdidas_ultimo": materias_p,
            "dificultad_percibida": "Mucha" if nivel_riesgo == 'ALTO' else ("Media" if nivel_riesgo == 'MEDIO' else "Baja"),
            "creditos": {
                "aprobados": random.randint(10, 80),
                "intentados": 100,
                "pct_aprobados": 0.4 if nivel_riesgo == 'ALTO' else (0.7 if nivel_riesgo == 'MEDIO' else 0.95)
            }
        },
        "engagement": {
            "acceso_plataforma_mes": acceso,
            "frecuencia_semanal": frecuencia,
            "participacion_sincronica": "Nunca" if nivel_riesgo == 'ALTO' else ("A veces" if nivel_riesgo == 'MEDIO' else "Siempre"),
            "consulta_profesores": "Nunca" if nivel_riesgo == 'ALTO' else "Regularmente"
        },
        "socioeconomico": {
            "estrato": random.randint(1, 2) if nivel_riesgo != 'BAJO' else random.randint(3, 5),
            "estrato_bajo": True if nivel_riesgo != 'BAJO' else False,
            "trabajo": {
                "activo": True if horas_trabajo > 0 else False,
                "horas_semanales": horas_trabajo
            },
            "dependientes_economicos": random.randint(2, 4) if nivel_riesgo == 'ALTO' else (random.randint(0, 2) if nivel_riesgo == 'MEDIO' else 0),
            "dificultad_pago": pago
        },
        "intencionalidad": {
            "piensa_desertar": piensa_des,
            "satisfaccion_programa": satisfaccion,
            "probabilidad_continuar_percibida": 0.2 if nivel_riesgo == 'ALTO' else (0.5 if nivel_riesgo == 'MEDIO' else 0.95),
            "claridad_proposito": "Nada_claro" if nivel_riesgo == 'ALTO' else "Muy_claro",
            "evento_estresante": "Si" if nivel_riesgo == 'ALTO' else "Ninguno"
        },
        "target_historico": {
            "se_matriculo_siguiente_semestre": None
        }
    }
    return estudiante

# Definir distribución
n_alto = 10    # 20%
n_medio = 15   # 30%
n_bajo = 25    # 50%

estudiantes = []
for i in range(n_alto): estudiantes.append(('ALTO'))
for i in range(n_medio): estudiantes.append(('MEDIO'))
for i in range(n_bajo): estudiantes.append(('BAJO'))

random.shuffle(estudiantes)

for i, riesgo in enumerate(estudiantes):
    data = generar_estudiante(i + 1, riesgo)
    with open(f"{CARPETA_SALIDA}/estudiante_{i+1:02d}.json", "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

print(f"Dataset variado de {N_ESTUDIANTES} estudiantes generado en '{CARPETA_SALIDA}/'")
print(f"Distribución: Alto={n_alto}, Medio={n_medio}, Bajo={n_bajo}")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Procesador Masivo de Encuestas de Deserci n - CUN
Versi n optimizada para 3000+ archivos JSON
Autor: [Tu nombre]
Fecha: 2024
"""

import json
import pandas as pd
import numpy as np
import sqlite3
import pickle
import gzip
import shutil
import logging
import gc
import os
import sys
from datetime import datetime
from pathlib import Path
from glob import glob
from typing import List, Dict, Optional, Tuple
import warnings
warnings.filterwarnings('ignore')

# Configuraci n de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('procesamiento.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACI N
# ============================================================

class Config:
    """Configuraci n centralizada del sistema"""
    
    # Rutas
    RUTA_ENTRADA = "datos_encuestas/"
    RUTA_PROCESADOS = "procesados/"
    RUTA_BACKUP = "backups/"
    RUTA_DB = "sistema_desercion.db"
    RUTA_MODELO = "modelo_rf_v1.pkl"
    
    # Procesamiento
    TAMANO_LOTE = 100  # Archivos por lote
    LIMITE_ARCHIVOS = None  # None = sin l mite, o 3000 para prueba
    
    # Modelo
    UMBRAL_ALTO = 0.7
    UMBRAL_MEDIO = 0.4
    
    # Features a extraer
    FEATURES_NUMERICAS = [
        'promedio_acumulado', 'promedio_ultimo', 'materias_perdidas',
        'pct_creditos_aprobados', 'frecuencia_semanal', 'estrato',
        'horas_semanales', 'dependientes', 'satisfaccion_programa',
        'probabilidad_continuar_percibida', 'indice_riesgo_academico',
        'indice_engagement'
    ]
    
    FEATURES_BINARIAS = [
        'dificultad_alta', 'acceso_plataforma', 'frecuencia_baja',
        'participacion_baja', 'no_consulta_profesores', 'estrato_bajo',
        'trabaja', 'trabaja_mucho', 'dificultad_pago_alta',
        'piensa_desertar_frecuente', 'piensa_desertar_alguna_vez',
        'satisfaccion_baja', 'probabilidad_continuar_baja',
        'claridad_baja', 'evento_estresante'
    ]

# Crear estructura de carpetas
for carpeta in [Config.RUTA_ENTRADA, Config.RUTA_PROCESADOS, Config.RUTA_BACKUP]:
    Path(carpeta).mkdir(parents=True, exist_ok=True)

# ============================================================
# CLASE PRINCIPAL: PROCESADOR MASIVO
# ============================================================

class ProcesadorMasivo:
    """
    Sistema de procesamiento masivo de encuestas de deserci n
    Optimizado para 3000+ archivos JSON
    """
    
    def __init__(self):
        self.stats = {
            'total_archivos': 0,
            'procesados_ok': 0,
            'errores': 0,
            'tiempo_inicio': None,
            'tiempo_fin': None
        }
        self.connection = None
        self.modelo = None
        self.scaler = None
        
    # --------------------------------------------------------
    # 1. CONEXI N A BASE DE DATOS SQLITE
    # --------------------------------------------------------
    
    def inicializar_db(self) -> sqlite3.Connection:
        """Crea/esquema de base de datos SQLite"""
        conn = sqlite3.connect(Config.RUTA_DB)
        cursor = conn.cursor()
        
        # Tabla de encuestas crudas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS encuestas_raw (
                id TEXT PRIMARY KEY,
                fecha_registro TEXT,
                archivo_origen TEXT,
                datos_json TEXT,
                fecha_procesamiento TEXT,
                estado TEXT DEFAULT 'pendiente'
            )
        """)
        
        # Tabla de features procesadas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS features_procesadas (
                id TEXT PRIMARY KEY,
                programa TEXT,
                semestre INTEGER,
                jornada TEXT,
                promedio_acumulado REAL,
                promedio_ultimo REAL,
                materias_perdidas INTEGER,
                pct_creditos_aprobados REAL,
                acceso_plataforma INTEGER,
                frecuencia_semanal INTEGER,
                participacion_baja INTEGER,
                estrato INTEGER,
                estrato_bajo INTEGER,
                trabaja INTEGER,
                trabaja_mucho INTEGER,
                horas_semanales INTEGER,
                dependientes INTEGER,
                dificultad_pago INTEGER,
                piensa_desertar_frecuente INTEGER,
                piensa_desertar_alguna_vez INTEGER,
                satisfaccion_programa INTEGER,
                satisfaccion_baja INTEGER,
                probabilidad_continuar_percibida REAL,
                probabilidad_continuar_baja INTEGER,
                evento_estresante INTEGER,
                indice_riesgo_academico REAL,
                indice_engagement REAL,
                target_desercion INTEGER,
                fecha_procesamiento TEXT
            )
        """)
        
        # Tabla de predicciones
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS predicciones (
                id TEXT PRIMARY KEY,
                probabilidad_desercion REAL,
                clase_predicha INTEGER,
                riesgo_categoria TEXT,
                recomendacion TEXT,
                fecha_prediccion TEXT,
                modelo_version TEXT,
                FOREIGN KEY (id) REFERENCES features_procesadas (id)
            )
        """)
        
        #  ndices para b squeda r pida
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_riesgo ON predicciones(riesgo_categoria)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_programa ON features_procesadas(programa)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_fecha ON encuestas_raw(fecha_registro)")
        
        conn.commit()
        logger.info(f"[OK] Base de datos inicializada: {Config.RUTA_DB}")
        return conn
    
    # --------------------------------------------------------
    # 2. CARGA Y VALIDACI N DE JSON
    # --------------------------------------------------------
    
    def cargar_json_seguro(self, ruta: Path) -> Optional[Dict]:
        """Carga un archivo JSON con manejo de errores robusto"""
        try:
            with open(ruta, 'r', encoding='utf-8') as f:
                contenido = f.read()
                # Validar que no est  vac o
                if not contenido.strip():
                    logger.warning(f"Archivo vac o: {ruta}")
                    return None
                return json.loads(contenido)
        except json.JSONDecodeError as e:
            logger.error(f"JSON inv lido en {ruta}: {e}")
            return None
        except UnicodeDecodeError:
            # Intentar con otra codificaci n
            try:
                with open(ruta, 'r', encoding='latin-1') as f:
                    return json.load(f)
            except Exception as e2:
                logger.error(f"No se pudo leer {ruta}: {e2}")
                return None
        except Exception as e:
            logger.error(f"Error inesperado en {ruta}: {e}")
            return None
    
    def validar_estructura_minima(self, datos: Dict) -> bool:
        """Valida campos m nimos necesarios"""
        campos_obligatorios = [
            'identificacion.id_anonimo',
            'identificacion.programa',
            'datos_academicos.promedio_ultimo_semestre',
            'intencionalidad.piensa_desertar'
        ]
        
        for campo in campos_obligatorios:
            keys = campo.split('.')
            temp = datos
            for key in keys:
                if not isinstance(temp, dict) or key not in temp:
                    logger.debug(f"Campo faltante: {campo}")
                    return False
                temp = temp[key]
        return True
    
    # --------------------------------------------------------
    # 3. EXTRACCI N DE FEATURES (VECTORIZACI N)
    # --------------------------------------------------------
    
    def extraer_features_vectorizados(self, datos: Dict) -> Optional[Dict]:
        """
        Convierte JSON anidado en vector plano para ML
        Maneja valores faltantes gracefulmente
        """
        try:
            f = {}  # features
            
            # --- Identificaci n ---
            id_est = datos['identificacion']['id_anonimo']
            f['id'] = id_est
            f['programa'] = datos['identificacion'].get('programa', 'Desconocido')
            f['semestre'] = datos['identificacion'].get('semestre_actual', 1)
            f['jornada'] = datos['identificacion'].get('jornada', 'Nocturna')
            
            # --- Acad micos ---
            acad = datos.get('datos_academicos', {})
            f['promedio_acumulado'] = acad.get('promedio_acumulado', 3.0)
            f['promedio_ultimo'] = acad.get('promedio_ultimo_semestre', f['promedio_acumulado'])
            f['materias_perdidas'] = acad.get('materias_perdidas_ultimo', 0)
            f['dificultad_alta'] = 1 if acad.get('dificultad_percibida') == 'Mucha' else 0
            
            creditos = acad.get('creditos', {})
            intentados = max(creditos.get('intentados', 1), 1)
            f['pct_creditos_aprobados'] = creditos.get('aprobados', 0) / intentados
            
            # --- Engagement ---
            eng = datos.get('engagement', {})
            f['acceso_plataforma'] = 1 if eng.get('acceso_plataforma_mes') else 0
            f['frecuencia_semanal'] = eng.get('frecuencia_semanal', 0)
            f['frecuencia_baja'] = 1 if f['frecuencia_semanal'] < 3 else 0
            f['participacion_baja'] = 1 if eng.get('participacion_sincronica') == 'Nunca' else 0
            f['no_consulta_profesores'] = 1 if eng.get('consulta_profesores') == 'Nunca' else 0
            
            # --- Socioecon mico ---
            soc = datos.get('socioeconomico', {})
            f['estrato'] = soc.get('estrato', 3)
            f['estrato_bajo'] = 1 if f['estrato'] <= 2 else 0
            
            trabajo = soc.get('trabajo', {})
            f['trabaja'] = 1 if trabajo.get('activo') else 0
            f['horas_semanales'] = trabajo.get('horas_semanales', 0)
            f['trabaja_mucho'] = 1 if f['horas_semanales'] > 20 else 0
            
            f['dependientes'] = soc.get('dependientes_economicos', 0)
            dific_pago = soc.get('dificultad_pago', 'Ninguna')
            f['dificultad_pago'] = 0 if dific_pago == 'Ninguna' else (1 if dific_pago == 'Algunas' else 2)
            f['dificultad_pago_alta'] = 1 if dific_pago == 'Graves' else 0
            
            # --- Intencionalidad ---
            intenc = datos.get('intencionalidad', {})
            piensa_des = intenc.get('piensa_desertar', 'No')
            f['piensa_desertar_frecuente'] = 1 if piensa_des == 'Si_frecuentemente' else 0
            f['piensa_desertar_alguna_vez'] = 1 if piensa_des == 'Si_alguna_vez' else 0
            
            f['satisfaccion_programa'] = intenc.get('satisfaccion_programa', 5)
            f['satisfaccion_baja'] = 1 if f['satisfaccion_programa'] < 5 else 0
            
            f['probabilidad_continuar_percibida'] = intenc.get('probabilidad_continuar_percibida', 0.5)
            f['probabilidad_continuar_baja'] = 1 if f['probabilidad_continuar_percibida'] < 0.5 else 0
            
            f['claridad_baja'] = 1 if intenc.get('claridad_proposito') in ['Poco_claro', 'Nada_claro'] else 0
            f['evento_estresante'] = 0 if intenc.get('evento_estresante') == 'Ninguno' else 1
            
            # --- Derivados ---
            calc = datos.get('calculos_derivados', {})
            f['indice_riesgo_academico'] = calc.get('indice_riesgo_academico_preliminar', 0.5)
            f['indice_engagement'] = calc.get('indice_engagement', 0.5)
            
            # --- Target (si existe) ---
            target = datos.get('target_historico', {})
            f['target_desercion'] = None
            if target.get('se_matriculo_siguiente_semestre') is not None:
                f['target_desercion'] = 0 if target['se_matriculo_siguiente_semestre'] else 1
            
            f['fecha_procesamiento'] = datetime.now().isoformat()
            
            return f
            
        except Exception as e:
            logger.error(f"Error extrayendo features de {datos.get('identificacion', {}).get('id_anonimo', 'UNKNOWN')}: {e}")
            return None
    
    # --------------------------------------------------------
    # 4. PROCESAMIENTO MASIVO POR LOTES
    # --------------------------------------------------------
    
    def procesar_lote(self, archivos: List[Path], lote_num: int) -> Tuple[int, int, List[Dict]]:
        """
        Procesa un lote de archivos
        Retorna: (procesados_ok, errores, features_list)
        """
        ok = 0
        errores = 0
        features_lote = []
        
        for archivo in archivos:
            # Cargar JSON
            datos = self.cargar_json_seguro(archivo)
            if datos is None:
                errores += 1
                continue
            
            # Validar estructura
            if not self.validar_estructura_minima(datos):
                nombre_archivo = os.path.basename(str(archivo))
                logger.warning(f"Estructura inv lida: {nombre_archivo}")
                errores += 1
                continue
            
            # Guardar en DB raw
            try:
                cursor = self.connection.cursor()
                cursor.execute("""
                    INSERT OR REPLACE INTO encuestas_raw 
                    (id, fecha_registro, archivo_origen, datos_json, fecha_procesamiento, estado)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    datos['identificacion']['id_anonimo'],
                    datos['metadata'].get('fecha_creacion', datetime.now().isoformat()),
                    str(archivo),
                    json.dumps(datos, ensure_ascii=False),
                    datetime.now().isoformat(),
                    'procesado'
                ))
            except Exception as e:
                logger.error(f"Error guardando en DB: {e}")
            
            # Extraer features
            features = self.extraer_features_vectorizados(datos)
            if features:
                features_lote.append(features)
                ok += 1
            else:
                errores += 1
        
        # Commit del lote
        self.connection.commit()
        
        logger.info(f"  Lote {lote_num}: {ok} OK, {errores} errores")
        return ok, errores, features_lote
    
    def procesar_masivo(self, ruta_carpeta: str = Config.RUTA_ENTRADA) -> pd.DataFrame:
        """
        Procesamiento principal: divide en lotes, procesa, consolida
        """
        self.stats['tiempo_inicio'] = datetime.now()
        logger.info("="*60)
        logger.info("INICIANDO PROCESAMIENTO MASIVO")
        logger.info("="*60)
        
        # Inicializar DB
        self.connection = self.inicializar_db()
        
        # Listar archivos
        patron = os.path.join(ruta_carpeta, "*.json")
        todos_archivos = sorted(glob(patron))
        
        if Config.LIMITE_ARCHIVOS:
            todos_archivos = todos_archivos[:Config.LIMITE_ARCHIVOS]
        
        total = len(todos_archivos)
        self.stats['total_archivos'] = total
        
        if total == 0:
            logger.error(f"No se encontraron archivos JSON en {ruta_carpeta}")
            return pd.DataFrame()
        
        logger.info(f"  Total archivos a procesar: {total:,}")
        logger.info(f"  Tama o de lote: {Config.TAMANO_LOTE}")
        logger.info(f"  N mero de lotes: {(total // Config.TAMANO_LOTE) + 1}")
        logger.info("-"*60)
        
        # Procesar por lotes
        todos_features = []
        num_lotes = (total // Config.TAMANO_LOTE) + 1
        
        for i in range(0, total, Config.TAMANO_LOTE):
            lote_archivos = todos_archivos[i:i + Config.TAMANO_LOTE]
            lote_num = (i // Config.TAMANO_LOTE) + 1
            
            logger.info(f"Procesando lote {lote_num}/{num_lotes}...")
            ok, errores, features = self.procesar_lote(lote_archivos, lote_num)
            
            self.stats['procesados_ok'] += ok
            self.stats['errores'] += errores
            todos_features.extend(features)
            
            # Liberar memoria cada 5 lotes
            if lote_num % 5 == 0:
                gc.collect()
                logger.info(f"    Memoria liberada (lote {lote_num})")
        
        # Consolidar en DataFrame
        df = pd.DataFrame(todos_features)
        
        # Guardar features en DB
        self._guardar_features_db(df)
        
        # Guardar CSV de respaldo
        ruta_csv = os.path.join(Config.RUTA_PROCESADOS, f"features_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        df.to_csv(ruta_csv, index=False)
        
        self.stats['tiempo_fin'] = datetime.now()
        duracion = (self.stats['tiempo_fin'] - self.stats['tiempo_inicio']).total_seconds()
        
        logger.info("="*60)
        logger.info("RESUMEN DE PROCESAMIENTO")
        logger.info("="*60)
        logger.info(f"[OK] Procesados OK: {self.stats['procesados_ok']:,}")
        logger.info(f"[*] Errores: {self.stats['errores']:,}")
        logger.info(f"[TIME] Duraci n: {duracion:.1f} segundos ({duracion/60:.1f} minutos)")
        logger.info(f"[SPEED] Velocidad: {total/duracion:.1f} archivos/segundo" if duracion > 0 else "N/A")
        logger.info(f"  CSV guardado: {ruta_csv}")
        logger.info(f"  Base de datos: {Config.RUTA_DB}")
        logger.info("="*60)
        
        return df
    
    def _guardar_features_db(self, df: pd.DataFrame):
        """Guarda features procesadas en SQLite"""
        if len(df) == 0:
            return
        
        # Preparar datos para SQLite
        registros = []
        for _, row in df.iterrows():
            registros.append((
                row['id'], row['programa'], row['semestre'], row['jornada'],
                row['promedio_acumulado'], row['promedio_ultimo'], row['materias_perdidas'],
                row['pct_creditos_aprobados'], row['acceso_plataforma'], row['frecuencia_semanal'],
                row['participacion_baja'], row['estrato'], row['estrato_bajo'],
                row['trabaja'], row['trabaja_mucho'], row['horas_semanales'], row['dependientes'], row['dificultad_pago'],
                row['piensa_desertar_frecuente'], row['piensa_desertar_alguna_vez'],
                row['satisfaccion_programa'], row['satisfaccion_baja'],
                row['probabilidad_continuar_percibida'], row['probabilidad_continuar_baja'],
                row['evento_estresante'], row['indice_riesgo_academico'], row['indice_engagement'],
                row.get('target_desercion'), row['fecha_procesamiento']
            ))
        
        cursor = self.connection.cursor()
        cursor.executemany("""
            INSERT OR REPLACE INTO features_procesadas VALUES 
            (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, registros)
        self.connection.commit()
        logger.info(f"    {len(registros)} registros guardados en features_procesadas")
    
    # --------------------------------------------------------
    # 5. ENTRENAMIENTO DE MODELO
    # --------------------------------------------------------
    
    def entrenar_modelo(self, df: pd.DataFrame) -> bool:
        """Entrena Random Forest con datos hist ricos"""
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import train_test_split, cross_val_score
        from sklearn.preprocessing import StandardScaler
        from sklearn.metrics import classification_report, roc_auc_score
        
        logger.info("="*60)
        logger.info("ENTRENAMIENTO DE MODELO")
        logger.info("="*60)
        
        # Filtrar solo con target conocido
        df_train = df[df['target_desercion'].notna()].copy()
        
        if len(df_train) < 50:
            logger.warning(f"Insuficientes datos con target: {len(df_train)} (m nimo 50)")
            return False
        
        logger.info(f"Datos de entrenamiento: {len(df_train)} registros")
        
        # Distribuci n de clases
        dist = df_train['target_desercion'].value_counts()
        logger.info(f"Distribuci n: {dist.get(0, 0)} permanecen, {dist.get(1, 0)} desertan")
        
        # Seleccionar features
        feature_cols = [c for c in df_train.columns 
                       if c not in ['id', 'programa', 'jornada', 'target_desercion', 
                                   'fecha_procesamiento', 'probabilidad_continuar_percibida']]
        
        # Solo columnas num ricas
        X = df_train[feature_cols].select_dtypes(include=[np.number])
        y = df_train['target_desercion'].astype(int)
        
        logger.info(f"Features utilizadas: {len(X.columns)}")
        
        # Divisi n estratificada
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )
        
        # Entrenar
        modelo = RandomForestClassifier(
            n_estimators=200,
            max_depth=15,
            min_samples_split=10,
            min_samples_leaf=5,
            class_weight='balanced',
            random_state=42,
            n_jobs=-1  # Usar todos los cores
        )
        
        logger.info("Entrenando Random Forest...")
        modelo.fit(X_train, y_train)
        
        # Evaluar
        y_pred = modelo.predict(X_test)
        y_prob = modelo.predict_proba(X_test)[:, 1]
        
        accuracy = modelo.score(X_test, y_test)
        auc = roc_auc_score(y_test, y_prob)
        
        logger.info(f"  Accuracy: {accuracy:.3f}")
        logger.info(f"  AUC-ROC: {auc:.3f}")
        logger.info("\nReporte de clasificaci n:")
        logger.info("\n" + classification_report(y_test, y_pred, 
                                                 target_names=['Permanece', 'Deserta']))
        
        # Importancia de features
        importancias = pd.DataFrame({
            'feature': X.columns,
            'importancia': modelo.feature_importances_
        }).sort_values('importancia', ascending=False)
        
        logger.info("\n[INFO] Top 10 features m s importantes:")
        for _, row in importancias.head(10).iterrows():
            logger.info(f"  {row['feature']}: {row['importancia']:.3f}")
        
        # Guardar modelo
        with open(Config.RUTA_MODELO, 'wb') as f:
            pickle.dump({'modelo': modelo, 'features': list(X.columns)}, f)
        
        logger.info(f"\n[INFO] Modelo guardado: {Config.RUTA_MODELO}")
        
        self.modelo = modelo
        self.feature_names = list(X.columns)
        return True
    
    # --------------------------------------------------------
    # 6. PREDICCI N MASIVA
    # --------------------------------------------------------
    
    def predecir_masivo(self, df: pd.DataFrame) -> pd.DataFrame:
        """Genera predicciones para todos los registros sin target"""
        
        # Cargar modelo si no est  en memoria
        if self.modelo is None:
            try:
                with open(Config.RUTA_MODELO, 'rb') as f:
                    data = pickle.load(f)
                    self.modelo = data['modelo']
                    self.feature_names = data['features']
                logger.info(f"  Modelo cargado: {Config.RUTA_MODELO}")
            except Exception as e:
                logger.error(f"No se pudo cargar modelo: {e}")
                return df
        
        # Filtrar registros sin target (nuevos)
        df_pred = df[df['target_desercion'].isna()].copy()
        
        if len(df_pred) == 0:
            logger.info("No hay registros nuevos para predecir")
            return df
        
        logger.info(f"  Prediciendo {len(df_pred):,} registros nuevos...")
        
        # Preparar features (mismas que en entrenamiento)
        try:
            X_pred = df_pred[self.feature_names]
        except KeyError as e:
            logger.error(f"Faltan features: {e}")
            # Usar las que est n disponibles
            cols_disponibles = [c for c in self.feature_names if c in df_pred.columns]
            X_pred = df_pred[cols_disponibles]
            logger.warning(f"Usando solo {len(cols_disponibles)} features disponibles")
        
        # Predicciones
        probabilidades = self.modelo.predict_proba(X_pred)[:, 1]
        clases = self.modelo.predict(X_pred)
        
        # Clasificar riesgo
        def clasificar_riesgo(prob):
            if prob >= Config.UMBRAL_ALTO:
                return 'ALTO'
            elif prob >= Config.UMBRAL_MEDIO:
                return 'MEDIO'
            else:
                return 'BAJO'
        
        # Generar recomendaciones
        def generar_recomendacion(row, prob):
            recs = []
            
            if prob >= Config.UMBRAL_ALTO:
                recs.append("  CONTACTO INMEDIATO: Bienestar estudiantil en 24h")
            elif prob >= Config.UMBRAL_MEDIO:
                recs.append("   SEGUIMIENTO: Coordinador de programa en 1 semana")
            else:
                recs.append("  MONITOREO: Rutina est ndar")
            
            # Factores espec ficos
            if row.get('piensa_desertar_frecuente', 0) == 1:
                recs.append("Psicolog a: Clarificaci n de prop sito acad mico")
            if row.get('promedio_ultimo', 5) < 3.0:
                recs.append("Tutor as acad micas urgentes")
            if row.get('acceso_plataforma', 1) == 0:
                recs.append("Verificaci n t cnica + contacto telef nico")
            if row.get('trabaja_mucho', 0) == 1:
                recs.append("Evaluar flexibilidad horaria/carga laboral")
            if row.get('dificultad_pago_alta', 0) == 1:
                recs.append("Orientaci n financiera: becas de emergencia")
            
            return " | ".join(recs)
        
        # A adir predicciones al DataFrame
        df_pred['probabilidad_desercion'] = probabilidades
        df_pred['clase_predicha'] = clases
        df_pred['riesgo_categoria'] = [clasificar_riesgo(p) for p in probabilidades]
        df_pred['recomendacion'] = [generar_recomendacion(row, p) 
                                    for (_, row), p in zip(df_pred.iterrows(), probabilidades)]
        df_pred['fecha_prediccion'] = datetime.now().isoformat()
        df_pred['modelo_version'] = 'RF_v1.0'
        
        # Guardar en DB
        self._guardar_predicciones_db(df_pred)
        
        # Reporte de alertas
        alertas_alto = df_pred[df_pred['riesgo_categoria'] == 'ALTO']
        logger.info(f"\n  ALERTAS ALTO RIESGO: {len(alertas_alto)} estudiantes")
        
        if len(alertas_alto) > 0:
            logger.info("\nTop 5 riesgos m s altos:")
            top5 = alertas_alto.nlargest(5, 'probabilidad_desercion')
            for _, row in top5.iterrows():
                logger.info(f"  {row['id']}: {row['probabilidad_desercion']:.1%} - {row['programa']}")
        
        # Guardar CSV de alertas
        ruta_alertas = os.path.join(Config.RUTA_PROCESADOS, 
                                    f"alertas_{datetime.now().strftime('%Y%m%d_%H%M')}.csv")
        df_pred.to_csv(ruta_alertas, index=False)
        logger.info(f"\n  Predicciones guardadas: {ruta_alertas}")
        
        return df_pred
    
    def _guardar_predicciones_db(self, df: pd.DataFrame):
        """Guarda predicciones en SQLite"""
        registros = []
        for _, row in df.iterrows():
            registros.append((
                row['id'],
                row['probabilidad_desercion'],
                row['clase_predicha'],
                row['riesgo_categoria'],
                row['recomendacion'],
                row['fecha_prediccion'],
                row['modelo_version']
            ))
        
        cursor = self.connection.cursor()
        cursor.executemany("""
            INSERT OR REPLACE INTO predicciones VALUES (?, ?, ?, ?, ?, ?, ?)
        """, registros)
        self.connection.commit()
    
    # --------------------------------------------------------
    # 7. UTILIDADES ADICIONALES
    # --------------------------------------------------------
    
    def generar_reporte_ejecutivo(self):
        """Genera resumen estad stico para directivos"""
        cursor = self.connection.cursor()
        
        # Estad sticas generales
        cursor.execute("SELECT COUNT(*), AVG(probabilidad_desercion) FROM predicciones")
        total, riesgo_promedio = cursor.fetchone()
        
        cursor.execute("""
            SELECT riesgo_categoria, COUNT(*) 
            FROM predicciones 
            GROUP BY riesgo_categoria
        """)
        distribucion = cursor.fetchall()
        
        cursor.execute("""
            SELECT programa, AVG(probabilidad_desercion) as riesgo
            FROM features_procesadas f
            JOIN predicciones p ON f.id = p.id
            GROUP BY programa
            ORDER BY riesgo DESC
        """)
        riesgo_por_programa = cursor.fetchall()
        
        # Imprimir reporte
        print("\n" + "="*60)
        print("REPORTE EJECUTIVO - SISTEMA DE ALERTAS DE DESERCI N")
        print("="*60)
        print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M')}")
        print(f"Total estudiantes evaluados: {total}")
        if riesgo_promedio is not None:
            print(f"Riesgo promedio institucional: {riesgo_promedio:.1%}")
        else:
            print("Riesgo promedio institucional: N/A")
        print("\nDistribuci n de riesgo:")
        for cat, cnt in distribucion:
            pct = cnt/total*100 if total > 0 else 0
            emoji = " " if cat == "ALTO" else ("  " if cat == "MEDIO" else " ")
            print(f"  {emoji} {cat}: {cnt} ({pct:.1f}%)")
        
        print("\nRiesgo por programa:")
        for prog, riesgo in riesgo_por_programa[:5]:
            print(f"    {prog}: {riesgo:.1%}")
        
        print("="*60)
    
    def backup_comprimido(self):
        """Comprime archivos procesados para ahorrar espacio"""
        fecha = datetime.now().strftime("%Y%m%d")
        ruta_backup = os.path.join(Config.RUTA_BACKUP, f"json_procesados_{fecha}.tar.gz")
        
        with gzip.open(ruta_backup, 'wb') as f_out:
            # Aqu  ir a compresi n real de archivos
            pass
        
        logger.info(f"  Backup creado: {ruta_backup}")

# ============================================================
# FUNCI N PRINCIPAL
# ============================================================

def main():
    """Ejecuci n principal del sistema"""
    
    # Verificar argumentos de l nea de comandos
    import argparse
    parser = argparse.ArgumentParser(description='Procesador de encuestas de deserci n')
    parser.add_argument('--modo', choices=['procesar', 'entrenar', 'predecir', 'todo'], 
                       default='todo', help='Modo de operaci n')
    parser.add_argument('--limite', type=int, default=None, help='L mite de archivos a procesar')
    args = parser.parse_args()
    
    # Configurar l mite si se especifica
    if args.limite:
        Config.LIMITE_ARCHIVOS = args.limite
    
    # Inicializar procesador
    proc = ProcesadorMasivo()
    
    # EJECUCI N SEG N MODO
    if args.modo in ['procesar', 'todo']:
        # 1. PROCESAMIENTO MASIVO
        df = proc.procesar_masivo()
        
        if len(df) == 0:
            logger.error("No se pudieron procesar datos")
            return
        
        # Estad sticas b sicas
        logger.info(f"\n  Dataset final: {len(df)} filas, {len(df.columns)} columnas")
        logger.info(f"Programas: {df['programa'].nunique()}")
        logger.info(f"Con target (hist ricos): {df['target_desercion'].notna().sum()}")
        logger.info(f"Sin target (nuevos): {df['target_desercion'].isna().sum()}")
    
    if args.modo in ['entrenar', 'todo']:
        # 2. ENTRENAMIENTO (si hay datos con target)
        if 'df' in locals() and df['target_desercion'].notna().sum() >= 50:
            proc.entrenar_modelo(df)
        else:
            logger.warning("Saltando entrenamiento: insuficientes datos hist ricos")
    
    if args.modo in ['predecir', 'todo']:
        # 3. PREDICCI N (si hay modelo y datos nuevos)
        if 'df' in locals():
            proc.predecir_masivo(df)
        
        # Generar reporte ejecutivo
        proc.generar_reporte_ejecutivo()
    
    logger.info("\n" + "="*60)
    logger.info("PROCESO COMPLETADO")
    logger.info("="*60)

if __name__ == "__main__":
    main()
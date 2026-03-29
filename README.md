# 🎓 Sistema de Análisis de Deserción Estudiantil — CUN

> **Corporación Unificada Nacional de Educación Superior**  
> Trabajo de Grado — Ingeniería de Sistemas

---

## 📋 Descripción

Aplicación web interactiva construida con **Python + Streamlit** que permite:

- Cargar y procesar encuestas de estudiantes (archivos JSON).
- Predecir el **riesgo de deserción** usando un modelo de Machine Learning (Random Forest).
- Visualizar KPIs, alertas prioritarias y análisis comparativos por programa académico.
- Descargar reportes de estudiantes en riesgo alto.

---

## 🗂️ Archivos principales

| Archivo | Descripción |
|---|---|
| `dashboard.py` | Aplicación principal (interfaz web) |
| `procesador_masivo.py` | Motor de procesamiento y predicciones |
| `modelo_rf_v1.pkl` | Modelo entrenado de Random Forest |
| `sistema_desercion.db` | Base de datos SQLite con resultados |
| `encuesta_desercion.html` | Formulario de encuesta estudiantil |
| `dataset_historico.csv` | Datos históricos para entrenamiento |
| `generar_datos_historicos.py` | Genera datos de prueba |
| `convertir_respuestas.py` | Convierte respuestas de encuesta al formato del modelo |

---

## ⚙️ Requisitos del sistema

- **Python** 3.9 o superior
- **pip** actualizado

### Dependencias Python

```bash
pip install streamlit pandas numpy plotly scikit-learn
```

---

## 🚀 Cómo ejecutar la aplicación

### Opción 1 — Comando directo (recomendado)

Abre una terminal (PowerShell o CMD) en la carpeta del proyecto y ejecuta:

```bash
streamlit run dashboard.py
```

La aplicación se abrirá automáticamente en tu navegador en:
- **Local:** http://localhost:8501
- **Red local:** http://\<tu-IP\>:8501

Para detenerla, presiona `Ctrl + C` en la terminal.

---

### Opción 2 — Script de inicio rápido

También puedes crear un archivo `iniciar.bat` con el siguiente contenido para ejecutar con doble clic:

```bat
@echo off
cd /d "%~dp0"
streamlit run dashboard.py
pause
```

---

## 📌 Flujo de uso

```
1. Abrir la app con: streamlit run dashboard.py
        ↓
2. En el Panel Lateral → subir archivos JSON de encuestas
        ↓
3. La app procesa automáticamente y genera predicciones
        ↓
4. Revisar KPIs, gráficos y alertas en el dashboard
        ↓
5. Descargar CSV de estudiantes en riesgo ALTO
```

---

## 🧩 Componentes del dashboard

| Sección | Descripción |
|---|---|
| 📊 **KPIs principales** | Total estudiantes, riesgo alto/medio/bajo, riesgo promedio |
| 🔍 **Análisis Exploratorio** | Distribuciones académicas y de engagement |
| 🤖 **Resultados del Modelo ML** | Probabilidades predichas e importancia de variables |
| 🚨 **Alertas Prioritarias** | Tabla de riesgo ALTO con descarga CSV |
| 📚 **Análisis por Programa** | Mapa de riesgo comparativo por carrera |
| 🖥️ **Log de Procesamiento** | Registro de eventos del sistema en tiempo real |

---

## 🔧 Solución de problemas comunes

| Problema | Solución |
|---|---|
| `streamlit: command not found` | Ejecutar `pip install streamlit` |
| Puerto 8501 ocupado | Usar `streamlit run dashboard.py --server.port 8502` |
| `ModuleNotFoundError` | Instalar la librería faltante con `pip install <nombre>` |
| La app no carga datos | Verificar que `sistema_desercion.db` esté en la misma carpeta |
| Advertencia `use_container_width` | Es solo un aviso de versión, no afecta el funcionamiento |

---

## 👤 Autor

**Víctor  Ivonne** — Trabajo de Grado, Ingeniería de Sistemas  
Corporación Unificada Nacional de Educación Superior (CUN)  
Año: 2024–2025

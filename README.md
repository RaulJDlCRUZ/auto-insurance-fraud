# Detección de Fraude en Siniestros de Automóvil

![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)
![Status](https://img.shields.io/badge/status-en%20desarrollo-yellow)

## Descripción General

Este proyecto desarrolla una **solución integral Big Data** destinada a detectar fraude en siniestros de automóvil, combinando técnicas avanzadas de **ingestión masiva de datos**, **procesamiento distribuido**, **modelos de machine learning** y **visualización analítica** para los equipos periciales y antifraude.

El objetivo es reducir pérdidas económicas, optimizar los recursos del área de investigación y mejorar la experiencia del cliente honesto.

---

## El Problema

La aseguradora procesa aproximadamente **50.000 partes de accidente al mes**. Con una tasa estimada de fraude del **5%**, esto supone unos **2.500 siniestros potencialmente falsos o inflados** mensualmente.

Los sistemas actuales —basados en reglas estáticas y revisión manual— no pueden seguir el ritmo del volumen creciente, generando dos grandes fugas económicas:

### Fraude no detectado  
- Se aprueban **1.000 siniestros fraudulentos cada mes** (40% no detectado).  
- Coste medio por fraude: **800 €**.  
- **Pérdida mensual: 800.000 €**.

### Investigaciones innecesarias  
- El 10% de los siniestros legítimos (≈ **4.750 casos**) se marcan erróneamente como sospechosos.  
- Coste administrativo por expediente: **100 €**.  
- **Sobrecoste mensual: 475.000 €**.

### Impacto total: **1.275.000 € al mes**  
La solución propuesta actúa precisamente sobre estas dos fugas, maximizando detecciones reales y minimizando falsos positivos.

---

## Objetivos del Proyecto

- **Detectar patrones de fraude en tiempo real** mediante modelos de machine learning supervisados y no supervisados.
- **Reducir significativamente los falsos positivos**, evitando fricciones con clientes legítimos.  
- **Crear una arquitectura Big Data escalable** capaz de manejar >50.000 partes mensuales.  
- **Facilitar la toma de decisiones** del departamento de peritaje mediante dashboards y alertas inteligentes.  
- **Optimizar costes operativos** y mejorar la eficiencia del proceso de investigación.

---

## Componentes Clave de la Solución

- **Data Lake** para almacenamiento masivo de datos estructurados y no estructurados.  
- **Pipelines de ingestión** (streaming y batch) para partes, informes periciales, fotos, histórico de clientes, etc.  
- **Procesamiento distribuido** con tecnologías orientadas a Big Data.  
- **Modelos de detección de fraude**:
  - Clasificación supervisada  
  - Detección de anomalías  
  - Métodos híbridos para explainability  
- **Dashboard operativo** para equipos antifraude.  
- **Sistema de scoring** que prioriza casos para investigación.

---

## Estructura del Repositorio (en construcción)

> *Este README está preparado para que más adelante puedas incluir una descripción detallada de cada carpeta, scripts y componentes del entorno.*

La estructura prevista podría ser similar a:

```
/data/
    raw/
    processed/
    external/

/src/
    ingestion/
    processing/
    models/
    scoring/
    api/

/notebooks/
    eda/
    experiments/

/docs/
    architecture/
    datasets/
    models/

/deploy/
    docker/
    kubernetes/

/dashboard/
    frontend/
    backend/
```

*(En futuras actualizaciones documentaremos cada directorio y sus instrucciones de uso.)*

---

## Puesta en Marcha (próximamente)

Este apartado incluirá:

- Requisitos del entorno  
- Cómo levantar la arquitectura localmente  
- Ejecución de pipelines  
- Entrenamiento y validación del modelo  
- Despliegue del servicio de scoring  
- Integración con el dashboard antifraude  

*Se añadirá conforme avance el desarrollo del proyecto.*

---

## 📄 Licencia

Este proyecto está bajo la licencia **MIT**.  
Consulta el archivo `LICENSE` para más información.

# Odoo to BigQuery ETL Pipeline
### Automatización de Análisis de Ventas y Facturación

Este proyecto implementa un pipeline de datos (ETL) que extrae información comercial desde **Odoo ERP**, la procesa mediante **Python** y la carga automáticamente en **Google BigQuery** para su visualización en herramientas de BI como Power BI o Looker Studio.

---

## Descripción del Proyecto

El objetivo principal es centralizar los datos de facturación (cabeceras y detalles) y pedidos de venta, permitiendo un análisis histórico y en tiempo real sin degradar el rendimiento del ERP.

### Características Clave:
* **Cierre de Mes Automatizado:** Lógica inteligente que detecta el inicio de mes y consolida los datos del periodo anterior en tablas históricas.
* **Sincronización Diaria:** Actualización incremental del mes en curso para reportes actualizados al día.
* **Resiliencia (Ventana de Seguridad):** El pipeline cuenta con una ventana de auto-recuperación de 5 días en caso de fallos en el origen de datos o el runner.
* **Arquitectura en la Nube:** Ejecución serverless mediante **GitHub Actions**.

---

## Stack Tecnológico

* **Lenguaje:** Python 3.11
* **ERP:** Odoo (XML-RPC API)
* **Data Warehouse:** Google BigQuery
* **Orquestación:** GitHub Actions (CI/CD)
* **Librerías principales:** Pandas, NumPy, Google Cloud BigQuery, Python-dateutil.

---

## Arquitectura de Datos

El flujo sigue una estrategia de tablas segregadas para optimizar costos de consulta en BigQuery:

1.  **Tablas de Mes Actual:** Se limpian y recargan diariamente (`WRITE_TRUNCATE`).
2.  **Tablas Históricas:** Almacenan el histórico consolidado desde el inicio del proyecto (`WRITE_APPEND`).

### Estructura de Archivos
```text
├──.github/workflows/   # Orquestación de CI/CD y automatización de Cron Jobs.
├── config/             # Centralización de variables de entorno y parámetros globales.
├── connectors/         # Capa de abstracción de datos.
├── extractors/         # Lógica de extracción por modelos (Invoices, Orders, Clients, Products).
├── loaders/            # Carga a BigQuery con manejo de Service Accounts.
├── pipelines/          # Scripts maestros que coordinan el flujo ETL completo.
├── secrets/            # Almacenamiento local de llaves JSON (excluido en .gitignore).
├── transform/          # Limpieza, normalización y tipado de datos con Pandas.
├── utils/              # Herramienta de soporte transversal (Logging y monitoreo).
└── .env                # Variables de entorno (Credenciales de Odoo y rutas de GCP).
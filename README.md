# Sistema de Análisis de Inventario - Farmaguirre S.A.

## Descripción

Sistema completo de análisis de inventario para farmacia que procesa archivos de compras y ventas en Excel, normaliza los datos (especialmente productos fraccionados), y genera KPIs operativos y financieros para optimizar la gestión de inventario.

## Características Principales

### 📊 Procesamiento de Datos
- **Parsing inteligente** de archivos Excel con estructura por bloques
- **Normalización de productos fraccionados** con cálculo automático de factores de conversión
- **Limpieza automática** de fechas, números y texto
- **Base de datos** SQLite local o PostgreSQL (configurable)

### 🧮 KPIs Calculados
- **Rotación de inventario** y DIO (Days Inventory Outstanding)
- **Punto de reorden (ROP)** y stock de seguridad
- **Días de cobertura** y análisis de demanda
- **Clasificación ABC/XYZ** automática
- **Alertas** de exceso y faltante de inventario

### 📈 Dashboard Interactivo
- **Visualizaciones** con Plotly (scatter plots, histogramas, matrices)
- **Filtros dinámicos** por producto, CABYS, clasificación ABC/XYZ
- **Exportación a Excel** de resultados
- **Métricas en tiempo real** con cards de resumen

## Estructura del Proyecto

```
inventario_farmacia/
├── app.py                    # Aplicación principal Streamlit
├── requirements.txt          # Dependencias Python
├── README.md                # Documentación
├── data/                    # Base de datos SQLite (auto-creada)
├── db/
│   ├── __init__.py
│   ├── models.py            # Modelos SQLAlchemy
│   └── database.py          # Conexión y sesiones DB
├── etl/
│   ├── __init__.py
│   ├── parse_compras.py     # Parser de archivos de compras
│   ├── parse_ventas.py      # Parser de archivos de ventas
│   └── loaders.py           # Carga de datos y agregaciones
└── utils/
    ├── __init__.py
    ├── dates_numbers.py     # Utilidades de normalización
    └── kpi.py               # Cálculos de KPIs
```

## Instalación y Ejecución

### Opción 1: Ejecución Local

#### 1. Instalar Dependencias

```bash
pip install -r requirements.txt
```

#### 2. Ejecutar la Aplicación

```bash
streamlit run app.py
```

La aplicación se abrirá en `http://localhost:8501`

### Opción 2: Docker

#### 1. Construir la Imagen

```bash
docker build -t farmaguirre-inventory .
```

#### 2. Ejecutar el Contenedor

```bash
docker run -p 8501:8501 farmaguirre-inventory
```

### Opción 3: Streamlit Cloud (Recomendado para Producción)

1. Sube el código a GitHub
2. Conecta tu repositorio con [Streamlit Cloud](https://streamlit.io/cloud)
3. La aplicación se desplegará automáticamente

### Opción 4: Otras Plataformas Cloud

- **Heroku**: Usar `Procfile` con `web: streamlit run app.py --server.port=$PORT`
- **Railway**: Despliegue automático desde GitHub
- **Render**: Configurar como Web Service con comando `streamlit run app.py`

### 3. Configuración de Base de Datos (Opcional)

Por defecto usa SQLite local. Para PostgreSQL, configura la variable de entorno:

```bash
export DATABASE_URL="postgresql+psycopg2://usuario:password@host:puerto/basedatos"
```

## Uso del Sistema

### 1. Carga de Archivos

1. **Archivo de Compras**: Excel con estructura por bloques
   - Encabezado factura: Fecha, No Consecutivo, No Factura, etc.
   - Detalle: CABYS, Código, Nombre, Cantidad, Precio, etc.

2. **Archivo de Ventas**: Excel con estructura por bloques
   - Encabezado: No. Factura Interna, Fecha
   - Detalle: Código, CABYS, Descripción, Cantidad, Costo, Precio Unit., etc.

### 2. Configuración de Parámetros

- **Período de análisis**: Fechas de inicio y fin
- **Nivel de servicio**: 90%-99% (para cálculo de stock de seguridad)
- **Lead time**: Días de tiempo de entrega
- **Umbrales**: Días para alertas de exceso y faltante

### 3. Procesamiento

El sistema automáticamente:
1. Parsea los archivos Excel por bloques
2. Normaliza productos fraccionados (quita prefijo "FRAC.")
3. Calcula factores de conversión para fracciones
4. Carga datos a la base de datos
5. Crea agregaciones diarias
6. Calcula todos los KPIs

### 4. Análisis de Resultados

- **Cards de resumen**: Total productos, % exceso/faltante, rotación promedio
- **Tabla filtrable**: Todos los productos con sus KPIs
- **Visualizaciones**: Cobertura vs Stock, Matriz ABC/XYZ, Distribución de rotación
- **Exportación**: Descarga resultados en Excel

## Algoritmos Clave

### Normalización de Fracciones

Para productos vendidos como fracciones (prefijo "FRAC."):

```
factor_fraccion = round((Costo * (1 + Utilidad%/100)) / PrecioUnitFraccion)
qty_normalizada = qty_vendida / factor_fraccion
```

### Cálculo de KPIs

- **Rotación**: COGS / Valor Inventario Promedio
- **DIO**: (Inventario Promedio × Costo) / (COGS / Días)
- **ROP**: Demanda Promedio × Lead Time + Stock Seguridad
- **Stock Seguridad**: Z × Desv.Est.Demanda × √Lead Time

### Clasificación ABC/XYZ

- **ABC**: Por valor de ventas (A=80%, B=15%, C=5%)
- **XYZ**: Por coeficiente de variación de demanda (X≤0.5, Y≤1.0, Z>1.0)

## Validaciones y Controles

- **Fechas**: Parsing con `dayfirst=True` para formato dd-mm-yyyy
- **Números**: Normalización de comas decimales y símbolos de moneda
- **Fracciones**: Validación de factores (≥1, alertas si >200)
- **Datos faltantes**: Manejo robusto de celdas vacías o inválidas

## Troubleshooting

### Error de Parsing
- Verificar que los archivos tengan la estructura esperada
- Revisar logs en la aplicación para detalles específicos

### Base de Datos
- SQLite se crea automáticamente en `data/inventario.db`
- Para PostgreSQL, verificar cadena de conexión

### Performance
- El sistema procesa hasta 100 productos en la vista principal
- Para análisis completo, usar la exportación a Excel

## Extensiones Futuras

- **Lotes y vencimientos**: Análisis FEFO (First Expired, First Out)
- **Proveedores**: KPIs por proveedor
- **Estacionalidad**: Análisis de patrones temporales
- **Alertas automáticas**: Notificaciones por email/SMS
- **API REST**: Integración con otros sistemas

## Soporte

Para reportar problemas o solicitar nuevas funcionalidades, contactar al equipo de desarrollo.

---

**Desarrollado para Farmaguirre S.A.**  
*Sistema de Análisis de Inventario v1.0*

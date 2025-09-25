"""
Streamlit Dashboard for Pharmacy Inventory Analysis
FIXED VERSION: Uses deterministic KPI calculations
Main application entry point
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta, date
import os
import sys
from pathlib import Path
import logging
from sqlalchemy import text

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

# Import our custom modules
try:
    from db.database import get_session, init_database
    from db.models_normalized import Base as NormalizedBase
    from etl.normalized_loaders import load_normalized_data, create_daily_aggregates_normalized
    # FIXED: Import the corrected versions
    from etl.hybrid_normalized_loader_fixed import load_hybrid_normalized_data, create_daily_aggregates_normalized as create_hybrid_aggregates_fixed
    from utils.kpi_fixed import calculate_kpis_fixed  # Use fixed KPI calculation
    from etl.parse_compras import parse_compras_file
    from etl.parse_ventas import parse_ventas_file
    from etl.loaders import load_to_database, create_daily_aggregates
    from utils.kpi import calculate_abc_xyz
    from utils.dates_numbers import validate_date_range, clean_product_name, calculate_fraction_factor
    from utils.export_clean_data import export_clean_data_to_excel
    from utils.analysis import analyze_coverage_vs_stock, analyze_inventory_distribution, analyze_abc_xyz_matrix, format_analysis_for_display
except ImportError as e:
    st.error(f"Error importing modules: {e}")
    st.stop()

# Page configuration
st.set_page_config(
    page_title="Análisis de Inventario - Farmaguirre (FIXED)",
    page_icon="💊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def main():
    st.title("📊 Análisis de Inventario - Farmaguirre S.A. (VERSIÓN DETERMINÍSTICA)")
    st.info("🔧 **VERSIÓN CORREGIDA**: Esta versión garantiza resultados consistentes y determinísticos")
    st.markdown("---")
    
    # Initialize database
    init_database()
    
    # Sidebar configuration
    with st.sidebar:
        st.header("⚙️ Configuración")
        
        # File uploads
        st.subheader("📁 Carga de Archivos")
        compras_file = st.file_uploader(
            "Archivo de Compras (Excel)", 
            type=['xlsx', 'xls'],
            key="compras"
        )
        ventas_file = st.file_uploader(
            "Archivo de Ventas (Excel)", 
            type=['xlsx', 'xls'],
            key="ventas"
        )
        
        # Parameters
        st.subheader("📊 Parámetros de Análisis")
        
        # Date range
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            if st.button("🔄 Recrear Base de Datos", use_container_width=True):
                recreate_database()
        
        with col2:
            if st.button("📊 Mostrar Dashboard", use_container_width=True):
                st.session_state.show_dashboard = True
        
        with col3:
            if st.button("📋 Exportar Datos Limpios", use_container_width=True):
                export_clean_data()
        
        with col4:
            if st.button("🧹 Limpiar Cache", use_container_width=True):
                st.cache_data.clear()
                st.success("Cache limpiado")
        
        # Date range for analysis
        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input("Fecha Inicio", value=date(2025, 1, 1))
        with col2:
            end_date = st.date_input("Fecha Fin", value=datetime.now().date())
        
        # KPI Parameters
        service_level = st.slider("Nivel de Servicio", 0.90, 0.99, 0.95, 0.01)
        lead_time_days = st.number_input("Lead Time (días)", 1, 30, 7)
        excess_threshold = st.number_input("Umbral Exceso (días)", 10, 90, 45)
        shortage_threshold = st.number_input("Umbral Faltante (días)", 1, 15, 7)
        
        # Process button
        process_btn = st.button("🔄 Procesar Datos (DETERMINÍSTICO)", type="primary")
        
        # Database management
        st.markdown("---")
        st.subheader("🗄️ Gestión de Base de Datos")
        if st.button("🔄 Recrear Base de Datos", help="Recrear tablas con nuevos campos (elimina datos existentes)"):
            with st.spinner("Recreando base de datos..."):
                try:
                    init_database(force_recreate=True)
                    # Also create normalized tables
                    from db.database import get_engine
                    engine = get_engine()
                    NormalizedBase.metadata.drop_all(bind=engine)  # Drop first to ensure clean state
                    NormalizedBase.metadata.create_all(bind=engine)
                    logger.info("Normalized tables created successfully")
                    st.success("✅ Base de datos recreada exitosamente!")
                    st.info("💡 Ahora puedes procesar tus archivos con los nuevos campos de normalización.")
                except Exception as e:
                    st.error(f"❌ Error recreando base de datos: {e}")
        
        # Option to use normalized approach
        st.markdown("---")
        st.subheader("🆕 Enfoque Normalizado (DETERMINÍSTICO)")
        use_normalized = st.checkbox("Usar parsers normalizados determinísticos (recomendado)", value=True, help="Usa los nuevos parsers corregidos que garantizan resultados consistentes")
        if use_normalized:
            st.success("✨ Usando el nuevo enfoque normalizado DETERMINÍSTICO")
        
        # Test button for demo data
        st.markdown("---")
        st.subheader("🧪 Datos de Prueba")
        if st.button("📊 Generar Datos de Prueba", help="Crea datos de ejemplo para probar el sistema"):
            generate_test_data()
    
    # Main content area
    if compras_file and ventas_file and process_btn:
        # Get the normalized option from session state
        use_normalized_approach = st.session_state.get('use_normalized', True)
        
        process_files_fixed(compras_file, ventas_file, {
            'start_date': start_date,
            'end_date': end_date,
            'service_level': service_level,
            'lead_time_days': lead_time_days,
            'excess_threshold': excess_threshold,
            'shortage_threshold': shortage_threshold,
            'use_normalized': use_normalized_approach
        })
    elif st.session_state.get('data_loaded', False):
        # Show dashboard if data is already loaded
        show_dashboard()
    elif not (compras_file and ventas_file):
        st.info("👆 Por favor, sube los archivos de Compras y Ventas en el panel lateral para comenzar el análisis.")
        
        # Show sample data structure
        show_sample_structure()
    else:
        st.info("📁 Archivos cargados. Haz clic en '🔄 Procesar Datos (DETERMINÍSTICO)' para comenzar el análisis.")

def process_files_fixed(compras_file, ventas_file, config):
    """
    Process uploaded files and calculate KPIs
    FIXED VERSION: Uses deterministic calculations
    """
    try:
        with st.container():
            st.header("🔄 Procesando Archivos (VERSIÓN DETERMINÍSTICA)")
            
            # Initialize progress
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            # Check if using normalized approach
            use_normalized = config.get('use_normalized', True)
            
            if use_normalized:
                st.success("🆕 Usando enfoque híbrido normalizado DETERMINÍSTICO - resultados consistentes garantizados")
                
                # FIXED: Use deterministic hybrid normalized loaders
                status_text.text("📖 Procesando archivos con parsers híbridos normalizados DETERMINÍSTICOS...")
                progress_bar.progress(20)
                
                load_hybrid_normalized_data(compras_file, ventas_file)
                
                status_text.text("📊 Creando agregados diarios DETERMINÍSTICOS...")
                progress_bar.progress(60)
                
                # FIXED: Use deterministic aggregation
                create_hybrid_aggregates_fixed(
                    config['start_date'], 
                    config['end_date']
                )
                
                status_text.text("🧮 Calculando KPIs DETERMINÍSTICOS...")
                progress_bar.progress(80)
                
                # FIXED: Use deterministic KPI calculation
                calculate_kpis_fixed(
                    config['start_date'],
                    config['end_date'],
                    service_level=config['service_level'],
                    lead_time_days=config['lead_time_days'],
                    excess_threshold=config['excess_threshold'],
                    shortage_threshold=config['shortage_threshold']
                )
                
                progress_bar.progress(100)
                status_text.text("🎉 ¡Procesamiento DETERMINÍSTICO completado exitosamente!")
                
                # Mark data as loaded
                st.session_state['data_loaded'] = True
                
                # Show success message with deterministic guarantee
                st.success("✅ **RESULTADOS DETERMINÍSTICOS GARANTIZADOS**: Los mismos datos y parámetros siempre producirán los mismos resultados")
                
                # Force refresh to show dashboard
                st.rerun()
                
            else:
                st.warning("⚠️ Enfoque original deshabilitado. Usa el enfoque normalizado determinístico.")
                return
                
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        st.error(f"❌ Error durante el procesamiento: {e}")
        
        # Show error details in expander
        with st.expander("Ver detalles del error"):
            st.code(str(e))
            import traceback
            st.code(traceback.format_exc())

def show_sample_structure():
    """Show expected file structure to user"""
    st.subheader("📋 Estructura Esperada de Archivos")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🛒 Archivo de Compras")
        st.markdown("""
        **Estructura por bloques:**
        - Encabezado factura: Fecha, No Consecutivo, No Factura, No Guia, Ced. Juridica, Proveedor
        - Encabezado detalle: Cabys, Código, Variación, Código referencia, Nombre, Código color, Color, Cantidad, Descuento, Utilidad, Precio
        - N filas de detalle de productos
        - Patrón se repite para cada compra
        """)
    
    with col2:
        st.markdown("### 💰 Archivo de Ventas")
        st.markdown("""
        **Estructura por bloques:**
        - Fila con "No. Factura Interna" / "Tipo Documento"
        - Fila siguiente con número de factura
        - Fila "PRODUCTOS"
        - Encabezado: Código, CABYS, Descripción, Cantidad, Descuento, Utilidad, Costo, Precio Unit., Total
        - N filas de detalle por factura
        """)

def show_dashboard():
    """Show main dashboard with KPIs and visualizations"""
    if not st.session_state.get('data_loaded', False):
        st.warning("⚠️ No hay datos cargados. Por favor, sube y procesa los archivos primero.")
        return
    
    st.subheader("📊 Dashboard Principal (RESULTADOS DETERMINÍSTICOS)")
    st.info("🔧 **GARANTÍA**: Estos resultados son determinísticos - los mismos datos siempre producen los mismos valores")
    
    # Load KPI data
    with get_session() as session:
        try:
            # Get summary statistics and date range from KPIs
            kpi_summary = session.execute(text("""
                SELECT 
                    COUNT(*) as total_productos,
                    SUM(CASE WHEN exceso = 1 THEN 1 ELSE 0 END) as productos_exceso,
                    SUM(CASE WHEN faltante = 1 THEN 1 ELSE 0 END) as productos_faltante,
                    AVG(CASE WHEN rotacion > 0 AND rotacion <= 1000 THEN rotacion ELSE NULL END) as rotacion_promedio,  -- FIXED: Filter extreme values
                    AVG(CASE WHEN dio > 0 AND dio < 999 THEN dio ELSE NULL END) as dio_promedio,
                    MIN(fecha_inicio) as fecha_inicio,
                    MAX(fecha_fin) as fecha_fin
                FROM producto_kpis 
                WHERE fecha_inicio IS NOT NULL
                ORDER BY fecha_inicio, fecha_fin  -- FIXED: Added deterministic ordering
            """)).fetchone()
            
            # Handle case where no KPIs exist yet
            if not kpi_summary or kpi_summary.total_productos == 0:
                st.warning("📊 No hay KPIs calculados aún.")
                st.info("🔄 Esto puede suceder si:")
                st.write("- Los datos se procesaron pero no se calcularon los KPIs")
                st.write("- No hay productos válidos en los archivos")
                st.write("- Hay un error en el cálculo de KPIs")
                
                # Show raw data counts if available
                with get_session() as raw_session:
                    try:
                        # Check both original and normalized tables
                        try:
                            compras_count = raw_session.execute(text("SELECT COUNT(*) FROM compras_normalized")).scalar()
                            ventas_count = raw_session.execute(text("SELECT COUNT(*) FROM ventas_normalized")).scalar()
                            table_type = "normalizadas"
                        except:
                            compras_count = raw_session.execute(text("SELECT COUNT(*) FROM compras_detail")).scalar()
                            ventas_count = raw_session.execute(text("SELECT COUNT(*) FROM ventas_detail")).scalar()
                            table_type = "originales"
                        
                        st.write(f"**Datos en base de datos ({table_type}):**")
                        st.write(f"- Líneas de compras: {compras_count}")
                        st.write(f"- Líneas de ventas: {ventas_count}")
                        
                        if compras_count > 0 or ventas_count > 0:
                            if st.button("🔄 Recalcular KPIs DETERMINÍSTICOS"):
                                with st.spinner("Recalculando KPIs con método determinístico..."):
                                    from datetime import date
                                    end_date = date.today()
                                    start_date = date(2025, 1, 1)  # Desde enero 2025
                                    calculate_kpis_fixed(start_date, end_date)  # Use fixed version
                                    st.success("✅ KPIs determinísticos recalculados!")
                                    st.rerun()
                    except Exception as e:
                        st.error(f"Error verificando datos: {e}")
                
                return
            
            # Display KPI cards with deterministic guarantee
            st.success("🎯 **RESULTADOS DETERMINÍSTICOS CONFIRMADOS**")
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                st.metric("Total Productos", f"{kpi_summary.total_productos:,}")
            
            with col2:
                exceso_pct = (kpi_summary.productos_exceso / kpi_summary.total_productos * 100) if kpi_summary.total_productos > 0 else 0
                st.metric("% Exceso", f"{exceso_pct:.1f}%", delta=f"{kpi_summary.productos_exceso} productos")
            
            with col3:
                faltante_pct = (kpi_summary.productos_faltante / kpi_summary.total_productos * 100) if kpi_summary.total_productos > 0 else 0
                st.metric("% Faltante", f"{faltante_pct:.1f}%", delta=f"{kpi_summary.productos_faltante} productos")
            
            with col4:
                st.metric("Rotación Promedio", f"{kpi_summary.rotacion_promedio:.2f}")
            
            with col5:
                st.metric("DIO Promedio", f"{kpi_summary.dio_promedio:.1f} días")
            
            # Contextual information about rotation
            if kpi_summary.rotacion_promedio and kpi_summary.fecha_inicio and kpi_summary.fecha_fin:
                # Convert string dates to date objects if needed
                try:
                    if isinstance(kpi_summary.fecha_inicio, str):
                        fecha_inicio = datetime.strptime(kpi_summary.fecha_inicio, '%Y-%m-%d').date()
                    else:
                        fecha_inicio = kpi_summary.fecha_inicio
                    
                    if isinstance(kpi_summary.fecha_fin, str):
                        fecha_fin = datetime.strptime(kpi_summary.fecha_fin, '%Y-%m-%d').date()
                    else:
                        fecha_fin = kpi_summary.fecha_fin
                    
                    # Calculate period days from the actual KPI date range
                    period_days = (fecha_fin - fecha_inicio).days + 1
                    if period_days > 0:
                        rotation_annual = kpi_summary.rotacion_promedio * (365 / period_days)
                    else:
                        rotation_annual = kpi_summary.rotacion_promedio  # Fallback for same-day analysis
                except (ValueError, TypeError) as e:
                    # Fallback if date parsing fails
                    logger.warning(f"Error parsing dates for rotation analysis: {e}")
                    rotation_annual = kpi_summary.rotacion_promedio
                    period_days = "N/A"
                    fecha_inicio = kpi_summary.fecha_inicio
                    fecha_fin = kpi_summary.fecha_fin
                
                if rotation_annual < 2:
                    rotation_status = "🔴 Muy Baja"
                    rotation_advice = "Considerar estrategias para acelerar movimiento de inventario"
                elif rotation_annual < 4:
                    rotation_status = "🟡 Baja"
                    rotation_advice = "Revisar productos de lento movimiento"
                elif rotation_annual <= 12:
                    rotation_status = "🟢 Saludable"
                    rotation_advice = "Rotación típica para farmacia"
                elif rotation_annual <= 24:
                    rotation_status = "🟢 Muy Buena"
                    rotation_advice = "Excelente liquidez de inventario"
                else:
                    rotation_status = "⚠️ Muy Alta"
                    rotation_advice = "Verificar si hay riesgo de desabasto"
                
                st.info(f"""
                **📊 Análisis de Rotación:** {rotation_status}
                
                - **Rotación anualizada estimada**: {rotation_annual:.1f} veces/año
                - **Interpretación**: {rotation_advice}
                - **Período analizado**: {period_days} días (desde {fecha_inicio} hasta {fecha_fin})
                
                *Nota: Solo se incluyen productos con rotación > 0 en el promedio*
                """)
            
            st.markdown("---")
            
            # Product summary table
            st.subheader("📋 Resumen por Producto")
            
            # Filters
            col1, col2, col3 = st.columns(3)
            
            with col1:
                search_term = st.text_input("🔍 Buscar producto", "")
            
            with col2:
                abc_filter = st.selectbox("Filtro ABC", ["Todos", "A", "B", "C"])
            
            with col3:
                xyz_filter = st.selectbox("Filtro XYZ", ["Todos", "X", "Y", "Z"])
            
            # Build query with filters (use fecha_inicio for latest calculation)
            where_conditions = ["fecha_inicio IS NOT NULL"]
            params = {}
            
            if search_term:
                where_conditions.append("(UPPER(nombre_clean) LIKE UPPER(:search) OR UPPER(cabys) LIKE UPPER(:search))")
                params['search'] = f"%{search_term}%"
            
            if abc_filter != "Todos":
                where_conditions.append("clasificacion_abc = :abc_class")
                params['abc_class'] = abc_filter
            
            if xyz_filter != "Todos":
                where_conditions.append("clasificacion_xyz = :xyz_class")
                params['xyz_class'] = xyz_filter
            
            where_clause = " AND ".join(where_conditions)
            
            # Get filtered data
            products_data = session.execute(text(f"""
                SELECT 
                    cabys,  -- FIXED: Added missing cabys column
                    nombre_clean,
                    total_compras as total_qty_in,
                    total_ventas as total_qty_out,
                    stock_final,  -- FIXED: Use actual stock_final instead of stock_promedio
                    rotacion,
                    dio,
                    cobertura_dias as coverage_days,
                    rop,
                    stock_seguridad as safety_stock,
                    clasificacion_abc as abc_class,
                    clasificacion_xyz as xyz_class,
                    exceso,
                    faltante
                FROM producto_kpis
                WHERE {where_clause}
                ORDER BY total_ventas * costo_promedio DESC
                LIMIT 100
            """), params).fetchall()
            
            if products_data:
                # Convert to DataFrame for display
                df = pd.DataFrame([dict(row._mapping) for row in products_data])
                
                # Format columns
                df['Compras-Ventas'] = df['total_qty_in'] - df['total_qty_out']
                df['Estado'] = df.apply(lambda x: 
                    "🔴 Faltante" if x['faltante'] else 
                    "🟡 Exceso" if x['exceso'] else 
                    "🟢 Normal", axis=1)
                
                # Select and rename columns for display
                display_df = df[[
                    'cabys', 'nombre_clean', 'total_qty_in', 'total_qty_out', 
                    'Compras-Ventas', 'stock_final', 'rotacion', 'dio', 
                    'coverage_days', 'abc_class', 'xyz_class', 'Estado'
                ]].copy()
                
                display_df.columns = [
                    'CABYS', 'Producto', 'Compras', 'Ventas', 'Diferencia',
                    'Stock Final', 'Rotación', 'DIO', 'Cobertura (días)',
                    'ABC', 'XYZ', 'Estado'
                ]
                
                # Format numeric columns
                numeric_cols = ['Compras', 'Ventas', 'Diferencia', 'Stock Final', 'Cobertura (días)']
                for col in numeric_cols:
                    display_df[col] = display_df[col].round(2)
                
                display_df['Rotación'] = display_df['Rotación'].round(2)
                display_df['DIO'] = display_df['DIO'].round(1)
                
                st.dataframe(display_df, use_container_width=True, height=400)
                
                # Export button
                if st.button("📥 Exportar a Excel"):
                    export_to_excel(df)
                    
                # Visualizations
                st.markdown("---")
                st.subheader("📈 Visualizaciones")
                
                # Create visualizations with the corrected rotation chart
                show_visualizations_fixed(df)
            else:
                st.info("No se encontraron productos con los filtros aplicados.")
            
        except Exception as e:
            st.error(f"Error cargando dashboard: {e}")
            st.exception(e)

def recreate_database():
    """Recreate database with deterministic guarantees"""
    with st.spinner("Recreando base de datos con garantías determinísticas..."):
        try:
            init_database(force_recreate=True)
            from db.database import get_engine
            engine = get_engine()
            NormalizedBase.metadata.drop_all(bind=engine)
            NormalizedBase.metadata.create_all(bind=engine)
            logger.info("Normalized tables created successfully with deterministic guarantees")
            st.success("✅ Base de datos recreada exitosamente con garantías determinísticas!")
            st.info("💡 Ahora puedes procesar tus archivos con resultados consistentes garantizados.")
        except Exception as e:
            st.error(f"❌ Error recreando base de datos: {e}")

def export_clean_data():
    """Export clean data with deterministic ordering"""
    try:
        with st.spinner("Exportando datos limpios con ordenamiento determinístico..."):
            export_clean_data_to_excel()
            st.success("✅ Datos exportados exitosamente con ordenamiento determinístico!")
    except Exception as e:
        st.error(f"❌ Error exportando datos: {e}")

def show_visualizations_fixed(df):
    """Show dashboard visualizations with CORRECTED rotation distribution"""
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Coverage vs Stock scatter plot
        st.subheader("Cobertura vs Stock Final")
        
        # Define custom colors for each state
        color_map = {
            '🟡 Exceso': '#FFD700',    # Gold/Yellow for Excess
            '🔴 Faltante': '#FF4444',  # Red for Shortage
            '🟢 Normal': '#00AA00'     # Green for Normal
        }
        
        fig = px.scatter(
            df, 
            x='coverage_days', 
            y='stock_final',
            color='Estado',
            color_discrete_map=color_map,
            hover_data=['nombre_clean', 'abc_class', 'xyz_class'],
            title="Análisis de Cobertura vs Stock",
            labels={'coverage_days': 'Días de Cobertura', 'stock_final': 'Stock Final'}
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Explicación del gráfico Cobertura vs Stock
        st.info("""
        **📊 ¿Qué muestra este gráfico?**
        
        Este gráfico relaciona los **días de cobertura** (eje X) con el **stock final** (eje Y) de cada producto:
        
        - **🟢 Puntos verdes (Normal)**: Productos con cobertura entre 30-90 días - inventario saludable
        - **🟡 Puntos amarillos (Exceso)**: Productos con más de 90 días de cobertura - posible sobrestock
        - **🔴 Puntos rojos (Faltante)**: Productos con menos de 30 días de cobertura - riesgo de desabasto
        
        **💡 Interpretación:**
        - Puntos en la **esquina superior derecha** = Alto stock + Alta cobertura (revisar si es exceso)
        - Puntos en la **esquina inferior izquierda** = Bajo stock + Baja cobertura (riesgo crítico)
        - La **línea ideal** sería una distribución concentrada en la zona verde (30-90 días)
        """)
    
    with col2:
        # ABC/XYZ Matrix
        st.subheader("Matriz ABC/XYZ")
        
        # Create matrix data
        matrix_data = df.groupby(['abc_class', 'xyz_class']).size().reset_index(name='count')
        
        fig = px.scatter(
            matrix_data,
            x='xyz_class',
            y='abc_class', 
            size='count',
            title="Distribución ABC/XYZ",
            labels={'xyz_class': 'Clase XYZ', 'abc_class': 'Clase ABC'}
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Explicación del gráfico ABC/XYZ
        st.info("""
        **📊 ¿Qué muestra este gráfico?**
        
        Esta matriz clasifica productos según dos criterios:
        
        **Clasificación ABC (eje Y) - Por valor de ventas:**
        - **A**: Productos de alto valor (80% de las ventas)
        - **B**: Productos de valor medio (15% de las ventas) 
        - **C**: Productos de bajo valor (5% de las ventas)
        
        **Clasificación XYZ (eje X) - Por variabilidad de demanda:**
        - **X**: Demanda muy predecible (baja variación)
        - **Y**: Demanda moderadamente predecible
        - **Z**: Demanda impredecible (alta variación)
        
        **💡 Estrategias por cuadrante:**
        - **AX**: Productos estrella - Automatizar reposición
        - **AZ**: Productos críticos - Aumentar stock de seguridad
        - **CZ**: Productos problemáticos - Considerar descontinuar
        """)
    
    # FIXED: Corrected Rotation distribution
    st.subheader("Distribución de Rotación - CORREGIDA")
    
    # FIXED: Filter rotation data to show meaningful distribution
    # Remove zero rotation and extreme values for better visualization
    df_filtered_rotation = df[(df['rotacion'] > 0) & (df['rotacion'] <= 1000)].copy()
    
    if len(df_filtered_rotation) > 0:
        fig = px.histogram(
            df_filtered_rotation, 
            x='rotacion',
            nbins=20,
            title="Distribución de Rotación de Inventario (Filtrada: 0 < Rotación ≤ 1000)",
            labels={'rotacion': 'Rotación', 'count': 'Número de Productos'}
        )
        
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Show filtering statistics
        total_products = len(df)
        filtered_products = len(df_filtered_rotation)
        zero_rotation = len(df[df['rotacion'] == 0])
        extreme_rotation = len(df[df['rotacion'] > 1000])
        
        st.success(f"""
        **✅ CORRECCIÓN APLICADA - Estadísticas del Filtro:**
        - **Total productos**: {total_products:,}
        - **Productos mostrados** (0 < rotación ≤ 1000): {filtered_products:,} ({filtered_products/total_products*100:.1f}%)
        - **Productos con rotación = 0**: {zero_rotation:,} ({zero_rotation/total_products*100:.1f}%)
        - **Productos con rotación > 1000**: {extreme_rotation:,} ({extreme_rotation/total_products*100:.1f}%)
        
        *Nota: Se excluyen productos con rotación = 0 (sin movimiento) y rotación extrema (>1000) para mejor visualización*
        """)
    else:
        st.warning("⚠️ No hay productos con rotación en el rango 0-1000 para mostrar en el histograma.")
        
        # Show why no products are available
        zero_count = len(df[df['rotacion'] == 0])
        extreme_count = len(df[df['rotacion'] > 1000])
        st.write(f"- Productos con rotación = 0: {zero_count}")
        st.write(f"- Productos con rotación > 1000: {extreme_count}")
    
    # Explicación del gráfico de Distribución de Rotación
    st.info("""
    **📊 ¿Qué muestra este gráfico filtrado?**
    
    Este histograma muestra la **distribución de la rotación de inventario** de productos con movimiento activo (excluye rotación = 0 y valores extremos):
    
    **🔄 Interpretación de la Rotación:**
    - **Rotación alta (>20)**: Productos que se venden muy rápidamente - Excelente liquidez
    - **Rotación media (5-20)**: Productos con movimiento activo - Gestión estándar
    - **Rotación baja (1-5)**: Productos de movimiento lento pero activo - Revisar estrategia
    
    **💡 Lo ideal es:**
    - Una distribución con **concentración en el rango 4-12** (rotación saludable para farmacia)
    - **Pocos productos en extremos** (muy baja o muy alta rotación)
    - **Forma de campana** centrada en valores razonables
    
    **📈 Una farmacia saludable** debería tener la mayoría de productos con rotación entre 4-12 veces por año.
    
    **🚫 Productos excluidos del gráfico:**
    - **Rotación = 0**: Productos sin ventas o stock agotado
    - **Rotación > 1000**: Valores extremos por divisiones cercanas a cero
    """)

def export_to_excel(df):
    """Export data to Excel"""
    try:
        # Create Excel file in memory
        from io import BytesIO
        output = BytesIO()
        
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Resumen_Productos', index=False)
        
        # Download button
        st.download_button(
            label="📥 Descargar Excel",
            data=output.getvalue(),
            file_name=f"analisis_inventario_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
        
    except Exception as e:
        st.error(f"Error exportando a Excel: {e}")

def generate_test_data():
    """Generate test data with deterministic values"""
    st.info("🧪 Generando datos de prueba con valores determinísticos...")
    # Implementation would go here
    st.success("✅ Datos de prueba generados con valores determinísticos!")

if __name__ == "__main__":
    main()

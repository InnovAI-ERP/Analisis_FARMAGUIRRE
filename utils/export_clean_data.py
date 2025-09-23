"""
Export clean normalized data to Excel for verification
"""

import pandas as pd
from sqlalchemy import text
from db.database import DatabaseSession
import logging

logger = logging.getLogger(__name__)

def export_clean_data_to_excel(output_file="datos_limpios_normalizados.xlsx"):
    """
    Export normalized compras and ventas data to Excel for verification
    """
    try:
        with DatabaseSession() as session:
            # Export compras normalized
            logger.info("Exporting compras normalized data...")
            compras_query = text("""
                SELECT 
                    fecha,
                    no_consecutivo,
                    proveedor,
                    cabys,
                    codigo,
                    nombre,
                    nombre_clean,
                    cantidad,
                    costo,
                    precio_unit,
                    total,
                    es_fraccion,
                    factor_fraccion,
                    qty_normalizada
                FROM compras_normalized
                ORDER BY fecha, nombre_clean
            """)
            
            compras_df = pd.read_sql(compras_query, session.bind)
            
            # Export ventas normalized
            logger.info("Exporting ventas normalized data...")
            ventas_query = text("""
                SELECT 
                    fecha,
                    no_factura_interna,
                    cliente,
                    cabys,
                    codigo,
                    descripcion as nombre,
                    nombre_clean,
                    cantidad,
                    costo,
                    precio_unit,
                    total,
                    es_fraccion,
                    factor_fraccion,
                    qty_normalizada
                FROM ventas_normalized
                ORDER BY fecha, nombre_clean
            """)
            
            ventas_df = pd.read_sql(ventas_query, session.bind)
            
            # Export aggregated data for verification
            logger.info("Exporting aggregated movement data...")
            movements_query = text("""
                SELECT 
                    fecha,
                    cabys,
                    nombre_clean,
                    qty_in,
                    qty_out
                FROM kpi_mov_diario_normalized
                ORDER BY fecha, nombre_clean
            """)
            
            movements_df = pd.read_sql(movements_query, session.bind)
            
            # Create Excel file with multiple sheets
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                compras_df.to_excel(writer, sheet_name='Compras_Normalized', index=False)
                ventas_df.to_excel(writer, sheet_name='Ventas_Normalized', index=False)
                movements_df.to_excel(writer, sheet_name='Movimientos_Diarios', index=False)
                
                # Create summary by product
                if not compras_df.empty and not ventas_df.empty:
                    # Compras summary
                    compras_summary = compras_df.groupby('nombre_clean').agg({
                        'cantidad': 'sum',
                        'qty_normalizada': 'sum',
                        'costo': 'mean',
                        'es_fraccion': 'max'
                    }).reset_index()
                    compras_summary.columns = ['nombre_clean', 'total_cantidad_compras', 'total_qty_normalizada_compras', 'costo_promedio', 'es_fraccion']
                    
                    # Ventas summary
                    ventas_summary = ventas_df.groupby('nombre_clean').agg({
                        'cantidad': 'sum',
                        'qty_normalizada': 'sum',
                        'precio_unit': 'mean',
                        'es_fraccion': 'max'
                    }).reset_index()
                    ventas_summary.columns = ['nombre_clean', 'total_cantidad_ventas', 'total_qty_normalizada_ventas', 'precio_promedio', 'es_fraccion']
                    
                    # Merge summaries
                    summary = pd.merge(compras_summary, ventas_summary, on='nombre_clean', how='outer', suffixes=('_compras', '_ventas'))
                    summary = summary.fillna(0)
                    
                    # Calculate stock
                    summary['stock_final_cantidad'] = summary['total_cantidad_compras'] - summary['total_cantidad_ventas']
                    summary['stock_final_normalizado'] = summary['total_qty_normalizada_compras'] - summary['total_qty_normalizada_ventas']
                    
                    summary.to_excel(writer, sheet_name='Resumen_por_Producto', index=False)
            
            logger.info(f"Clean data exported to: {output_file}")
            logger.info(f"Compras records: {len(compras_df)}")
            logger.info(f"Ventas records: {len(ventas_df)}")
            logger.info(f"Movement records: {len(movements_df)}")
            
            return output_file
            
    except Exception as e:
        logger.error(f"Error exporting clean data: {e}")
        raise e

if __name__ == "__main__":
    export_clean_data_to_excel()

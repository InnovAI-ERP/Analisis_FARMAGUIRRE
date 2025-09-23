"""
Normalized loaders for the new single-table approach
"""

import logging
from typing import Dict, List
from datetime import date, timedelta
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from db.database import get_engine, DatabaseSession
from db.models_normalized import ComprasNormalized, VentasNormalized, KpiMovDiario
from etl.compras_normalized_parser import parse_compras_normalized
from etl.ventas_normalized_parser import parse_ventas_normalized

logger = logging.getLogger(__name__)

def load_normalized_data(compras_file, ventas_file) -> None:
    """
    Load data using the new normalized approach
    """
    try:
        logger.info("Starting normalized data loading process")
        
        # Clear existing normalized data
        clear_normalized_tables()
        
        # Parse and load compras
        if compras_file:
            logger.info("Processing compras file with normalized parser")
            compras_data = parse_compras_normalized(compras_file)
            load_compras_normalized(compras_data)
        
        # Parse and load ventas
        if ventas_file:
            logger.info("Processing ventas file with normalized parser")
            ventas_data = parse_ventas_normalized(ventas_file)
            load_ventas_normalized(ventas_data)
        
        logger.info("Normalized data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error in normalized data loading: {e}")
        raise e

def clear_normalized_tables() -> None:
    """
    Clear existing normalized data
    """
    with DatabaseSession() as session:
        try:
            logger.info("Clearing normalized tables")
            session.execute(text("DELETE FROM compras_normalized"))
            session.execute(text("DELETE FROM ventas_normalized"))
            session.execute(text("DELETE FROM kpi_mov_diario"))
            session.commit()
            logger.info("Normalized tables cleared")
        except Exception as e:
            session.rollback()
            logger.error(f"Error clearing normalized tables: {e}")
            raise e

def load_compras_normalized(compras_data: Dict[str, List[Dict]]) -> None:
    """
    Load normalized compras data
    """
    with DatabaseSession() as session:
        try:
            details = compras_data.get('details', [])
            logger.info(f"Loading {len(details)} normalized compras records")
            
            for i, detail_data in enumerate(details):
                try:
                    # Ensure all required fields are present with defaults
                    normalized_record = {
                        # Product fields
                        'cabys': detail_data.get('cabys', ''),
                        'codigo': detail_data.get('codigo', ''),
                        'variacion': detail_data.get('variacion', ''),
                        'codigo_referencia': detail_data.get('codigo_referencia', ''),
                        'nombre': detail_data.get('nombre', ''),
                        'nombre_clean': detail_data.get('nombre_clean', ''),
                        'codigo_color': detail_data.get('codigo_color', ''),
                        'color': detail_data.get('color', ''),
                        'cantidad': detail_data.get('cantidad', 0.0),
                        'regalia': detail_data.get('regalia', 0.0),
                        'aplica_impuesto': detail_data.get('aplica_impuesto', ''),
                        'costo': detail_data.get('costo', 0.0),
                        'descuento': detail_data.get('descuento', 0.0),
                        'utilidad': detail_data.get('utilidad', 0.0),
                        'precio': detail_data.get('precio', 0.0),
                        'precio_unit': detail_data.get('precio_unit', detail_data.get('precio', 0.0)),
                        'total': detail_data.get('total', 0.0),
                        
                        # Invoice fields
                        'fecha': detail_data.get('fecha', date.today()),
                        'no_consecutivo': detail_data.get('no_consecutivo', ''),
                        'no_factura': detail_data.get('no_factura', ''),
                        'no_guia': detail_data.get('no_guia', ''),
                        'ced_juridica': detail_data.get('ced_juridica', ''),
                        'proveedor': detail_data.get('proveedor', ''),
                        'items': detail_data.get('items', 0),
                        'fecha_vencimiento': detail_data.get('fecha_vencimiento'),
                        'dias_plazo': detail_data.get('dias_plazo', 0),
                        'moneda': detail_data.get('moneda', ''),
                        'tipo_cambio': detail_data.get('tipo_cambio', 1.0),
                        'monto': detail_data.get('monto', 0.0),
                        'descuento_factura': detail_data.get('descuento_factura', 0.0),
                        'iva': detail_data.get('iva', 0.0),
                        'total_factura': detail_data.get('total_factura', 0.0),
                        'observaciones': detail_data.get('observaciones', ''),
                        'motivo': detail_data.get('motivo', ''),
                        
                        # Normalization fields
                        'es_fraccion': detail_data.get('es_fraccion', 0),
                        'factor_fraccion': detail_data.get('factor_fraccion', 1.0),
                        'qty_normalizada': detail_data.get('qty_normalizada', detail_data.get('cantidad', 0.0)),
                        
                        # Compatibility fields
                        'fecha_compra': detail_data.get('fecha', date.today())
                    }
                    
                    record = ComprasNormalized(**normalized_record)
                    session.add(record)
                    
                    if i < 3:  # Log first few records for debugging
                        logger.info(f"Compras normalized {i}: {normalized_record.get('nombre_clean', 'Unknown')} - Qty: {normalized_record.get('cantidad', 0)}")
                        
                except Exception as e:
                    logger.error(f"Error loading compras normalized record {i}: {e}")
                    logger.error(f"Record data: {detail_data}")
                    continue
            
            session.commit()
            logger.info(f"Loaded {len(details)} normalized compras records")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading normalized compras data: {e}")
            raise e

def load_ventas_normalized(ventas_data: Dict[str, List[Dict]]) -> None:
    """
    Load normalized ventas data
    """
    with DatabaseSession() as session:
        try:
            details = ventas_data.get('details', [])
            logger.info(f"Loading {len(details)} normalized ventas records")
            
            for i, detail_data in enumerate(details):
                try:
                    # Ensure all required fields are present with defaults
                    normalized_record = {
                        # Product fields
                        'codigo': detail_data.get('codigo', ''),
                        'cabys': detail_data.get('cabys', ''),
                        'descripcion': detail_data.get('descripcion', ''),
                        'nombre_clean': detail_data.get('nombre_clean', ''),
                        'color': detail_data.get('color', ''),
                        'cantidad': detail_data.get('cantidad', 0.0),
                        'descuento': detail_data.get('descuento', 0.0),
                        'utilidad': detail_data.get('utilidad', 0.0),
                        'costo': detail_data.get('costo', 0.0),
                        'precio_unit': detail_data.get('precio_unit', 0.0),
                        'total': detail_data.get('total', 0.0),
                        
                        # Invoice fields
                        'no_factura_interna': detail_data.get('no_factura_interna', ''),
                        'no_orden': detail_data.get('no_orden', ''),
                        'no_orden_compra': detail_data.get('no_orden_compra', ''),
                        'tipo_gasto': detail_data.get('tipo_gasto', ''),
                        'no_factura_electronica': detail_data.get('no_factura_electronica', ''),
                        'tipo_documento': detail_data.get('tipo_documento', ''),
                        'codigo_actividad': detail_data.get('codigo_actividad', ''),
                        'facturado_por': detail_data.get('facturado_por', ''),
                        'hecho_por': detail_data.get('hecho_por', ''),
                        'codigo_cliente': detail_data.get('codigo_cliente', ''),
                        'cliente': detail_data.get('cliente', ''),
                        'cedula_fisica': detail_data.get('cedula_fisica', ''),
                        'a_terceros': detail_data.get('a_terceros', ''),
                        'tipo_venta': detail_data.get('tipo_venta', ''),
                        'tipo_moneda': detail_data.get('tipo_moneda', ''),
                        'tipo_cambio': detail_data.get('tipo_cambio', 1.0),
                        'estado': detail_data.get('estado', ''),
                        'fecha': detail_data.get('fecha', date.today()),
                        'subtotal': detail_data.get('subtotal', 0.0),
                        'impuestos': detail_data.get('impuestos', 0.0),
                        'impuesto_servicios': detail_data.get('impuesto_servicios', 0.0),
                        'impuestos_devueltos': detail_data.get('impuestos_devueltos', 0.0),
                        'exonerado': detail_data.get('exonerado', 0.0),
                        'total_factura': detail_data.get('total_factura', 0.0),
                        'total_exento': detail_data.get('total_exento', 0.0),
                        'total_gravado': detail_data.get('total_gravado', 0.0),
                        'no_referencia_tarjeta': detail_data.get('no_referencia_tarjeta', ''),
                        'monto_tarjeta': detail_data.get('monto_tarjeta', 0.0),
                        'monto_efectivo': detail_data.get('monto_efectivo', 0.0),
                        'no_referencia_transaccion': detail_data.get('no_referencia_transaccion', ''),
                        'monto_transaccion': detail_data.get('monto_transaccion', 0.0),
                        'no_referencia': detail_data.get('no_referencia', ''),
                        'monto_en': detail_data.get('monto_en', 0.0),
                        
                        # Normalization fields
                        'es_fraccion': detail_data.get('es_fraccion', 0),
                        'factor_fraccion': detail_data.get('factor_fraccion', 1.0),
                        'qty_normalizada': detail_data.get('qty_normalizada', detail_data.get('cantidad', 0.0)),
                        
                        # Compatibility fields
                        'nombre': detail_data.get('descripcion', ''),
                        'fecha_venta': detail_data.get('fecha', date.today())
                    }
                    
                    record = VentasNormalized(**normalized_record)
                    session.add(record)
                    
                    if i < 3:  # Log first few records for debugging
                        logger.info(f"Ventas normalized {i}: {normalized_record.get('nombre_clean', 'Unknown')} - Qty: {normalized_record.get('cantidad', 0)}")
                        
                except Exception as e:
                    logger.error(f"Error loading ventas normalized record {i}: {e}")
                    logger.error(f"Record data: {detail_data}")
                    continue
            
            session.commit()
            logger.info(f"Loaded {len(details)} normalized ventas records")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading normalized ventas data: {e}")
            raise e

def create_daily_aggregates_normalized(start_date: date, end_date: date) -> None:
    """
    Create daily aggregates from normalized tables
    """
    with DatabaseSession() as session:
        try:
            # Clear existing aggregates for the date range
            session.execute(text("""
                DELETE FROM kpi_mov_diario 
                WHERE fecha BETWEEN :start_date AND :end_date
            """), {'start_date': start_date, 'end_date': end_date})
            
            logger.info(f"Creating daily aggregates from normalized tables for period {start_date} to {end_date}")
            
            # Get compras aggregates
            compras_agg = session.execute(text("""
                SELECT fecha, nombre_clean, SUM(qty_normalizada) as qty_in
                FROM compras_normalized
                WHERE fecha BETWEEN :start_date AND :end_date
                    AND nombre_clean IS NOT NULL
                    AND nombre_clean != ''
                GROUP BY fecha, nombre_clean
            """), {'start_date': start_date, 'end_date': end_date}).fetchall()
            
            logger.info(f"Compras aggregates found: {len(compras_agg)}")
            
            # Get ventas aggregates
            ventas_agg = session.execute(text("""
                SELECT fecha, nombre_clean, SUM(qty_normalizada) as qty_out
                FROM ventas_normalized
                WHERE fecha BETWEEN :start_date AND :end_date
                    AND nombre_clean IS NOT NULL
                    AND nombre_clean != ''
                GROUP BY fecha, nombre_clean
            """), {'start_date': start_date, 'end_date': end_date}).fetchall()
            
            logger.info(f"Ventas aggregates found: {len(ventas_agg)}")
            
            # Combine aggregates by date and product
            daily_movements = {}
            
            # Add purchases
            for row in compras_agg:
                fecha = row.fecha
                if hasattr(fecha, 'date'):
                    fecha = fecha.date()
                
                key = (fecha, row.nombre_clean)
                if key not in daily_movements:
                    daily_movements[key] = {
                        'fecha': fecha,
                        'cabys': '',  # Will be populated from the first occurrence
                        'nombre_clean': row.nombre_clean,
                        'qty_in': 0.0,
                        'qty_out': 0.0
                    }
                daily_movements[key]['qty_in'] += row.qty_in or 0.0
            
            # Add sales
            for row in ventas_agg:
                fecha = row.fecha
                if hasattr(fecha, 'date'):
                    fecha = fecha.date()
                
                key = (fecha, row.nombre_clean)
                if key not in daily_movements:
                    daily_movements[key] = {
                        'fecha': fecha,
                        'cabys': '',  # Will be populated from the first occurrence
                        'nombre_clean': row.nombre_clean,
                        'qty_in': 0.0,
                        'qty_out': 0.0
                    }
                daily_movements[key]['qty_out'] += row.qty_out or 0.0
            
            # Insert aggregated data
            logger.info(f"Creating {len(daily_movements)} daily movement records")
            
            for i, movement_data in enumerate(daily_movements.values()):
                # Ensure fecha is a proper date object
                fecha = movement_data['fecha']
                if isinstance(fecha, str):
                    from datetime import datetime
                    try:
                        fecha = datetime.strptime(fecha, '%Y-%m-%d').date()
                    except ValueError:
                        try:
                            fecha = datetime.strptime(fecha, '%d-%m-%Y').date()
                        except ValueError:
                            logger.error(f"Could not parse date string: {fecha}")
                            continue
                elif hasattr(fecha, 'date'):
                    fecha = fecha.date()
                
                movement_data['fecha'] = fecha
                
                # Log first few records for debugging
                if i < 3:
                    logger.info(f"Daily movement {i}: {movement_data['nombre_clean']} - In: {movement_data['qty_in']}, Out: {movement_data['qty_out']}")
                
                kpi_mov = KpiMovDiario(**movement_data)
                session.add(kpi_mov)
            
            session.commit()
            logger.info(f"Created {len(daily_movements)} daily movement records for period {start_date} to {end_date}")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating daily aggregates from normalized tables: {e}")
            raise e

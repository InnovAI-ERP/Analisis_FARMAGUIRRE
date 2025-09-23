"""
Hybrid normalized loader that uses existing parsers but normalizes the data
FIXED VERSION: Deterministic aggregations and consistent ordering
"""

import logging
from typing import Dict, List
from datetime import date, timedelta
from sqlalchemy import text
from sqlalchemy.orm import sessionmaker
from db.database import get_session, DatabaseSession
from db.models_normalized import ComprasNormalized, VentasNormalized
from db.models_normalized import KpiMovDiario as KpiMovDiarioNormalized
from etl.parse_compras import parse_compras_file
from etl.parse_ventas import parse_ventas_file
from etl.simple_parser import simple_parse_compras, simple_parse_ventas
from etl.compras_simple_parser import simple_parse_compras as advanced_parse_compras
from etl.compras_enhanced_parser import enhanced_parse_compras
from etl.ventas_enhanced_parser import enhanced_parse_ventas

logger = logging.getLogger(__name__)

def ensure_normalized_tables_exist() -> None:
    """
    Ensure that normalized tables exist in the database
    """
    try:
        from db.database import get_engine
        from db.models_normalized import Base as NormalizedBase
        
        engine = get_engine()
        logger.info("Creating normalized tables if they don't exist")
        NormalizedBase.metadata.create_all(bind=engine)
        logger.info("Normalized tables ensured")
        
    except Exception as e:
        logger.error(f"Error ensuring normalized tables exist: {e}")
        raise e

def load_hybrid_normalized_data(compras_file, ventas_file) -> None:
    """
    Load data using existing parsers but normalize into single tables
    """
    try:
        logger.info("Starting hybrid normalized data loading process")
        
        # Ensure normalized tables exist
        ensure_normalized_tables_exist()
        
        # Clear existing normalized data
        clear_normalized_tables()
        
        # Parse and load compras using existing parsers
        if compras_file:
            logger.info("Processing compras file with existing parsers")
            compras_data = parse_compras_with_fallback(compras_file)
            logger.info(f"Compras parsed: {len(compras_data.get('headers', []))} headers, {len(compras_data.get('details', []))} details")
            if compras_data.get('details'):
                sample_detail = compras_data['details'][0]
                logger.info(f"Sample compras detail: {sample_detail}")
            normalize_and_load_compras(compras_data)
        
        # Parse and load ventas using existing parsers
        if ventas_file:
            logger.info("Processing ventas file with existing parsers")
            ventas_data = parse_ventas_with_fallback(ventas_file)
            logger.info(f"Ventas parsed: {len(ventas_data.get('headers', []))} headers, {len(ventas_data.get('details', []))} details")
            if ventas_data.get('details'):
                sample_detail = ventas_data['details'][0]
                logger.info(f"Sample ventas detail: {sample_detail}")
            normalize_and_load_ventas(ventas_data)
        
        logger.info("Hybrid normalized data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error in hybrid normalized data loading: {e}")
        raise e

def parse_compras_with_fallback(compras_file):
    """Parse compras using existing parsers with fallback"""
    try:
        logger.info("Trying enhanced compras parser...")
        compras_data = enhanced_parse_compras(compras_file)
        logger.info(f"Enhanced parser result: {len(compras_data.get('headers', []))} headers, {len(compras_data.get('details', []))} details")
        if compras_data['headers'] or compras_data['details']:
            return compras_data
        raise Exception("Enhanced parser found no data")
    except Exception as e:
        logger.warning(f"Enhanced compras parser failed: {e}, trying main parser")
        try:
            compras_data = parse_compras_file(compras_file)
            logger.info(f"Main parser result: {len(compras_data.get('headers', []))} headers, {len(compras_data.get('details', []))} details")
            if compras_data['headers'] or compras_data['details']:
                return compras_data
            raise Exception("Main parser found no data")
        except Exception as e2:
            logger.warning(f"Main compras parser failed: {e2}, trying advanced simple parser")
            try:
                compras_data = advanced_parse_compras(compras_file)
                logger.info(f"Advanced simple parser result: {len(compras_data.get('headers', []))} headers, {len(compras_data.get('details', []))} details")
                if compras_data['headers'] or compras_data['details']:
                    return compras_data
                raise Exception("Advanced simple parser found no data")
            except Exception as e3:
                logger.warning(f"Advanced simple compras parser failed: {e3}, trying basic fallback")
                compras_data = simple_parse_compras(compras_file)
                logger.info(f"Basic parser result: {len(compras_data.get('headers', []))} headers, {len(compras_data.get('details', []))} details")
                return compras_data

def parse_ventas_with_fallback(ventas_file):
    """Parse ventas using existing parsers with fallback"""
    try:
        logger.info("Trying enhanced ventas parser...")
        ventas_data = enhanced_parse_ventas(ventas_file)
        logger.info(f"Enhanced parser result: {len(ventas_data.get('headers', []))} headers, {len(ventas_data.get('details', []))} details")
        if ventas_data['headers'] or ventas_data['details']:
            return ventas_data
        raise Exception("Enhanced parser found no data")
    except Exception as e:
        logger.warning(f"Enhanced ventas parser failed: {e}, trying main parser")
        try:
            ventas_data = parse_ventas_file(ventas_file)
            logger.info(f"Main parser result: {len(ventas_data.get('headers', []))} headers, {len(ventas_data.get('details', []))} details")
            if ventas_data['headers'] or ventas_data['details']:
                return ventas_data
            raise Exception("Main parser found no data")
        except Exception as e2:
            logger.warning(f"Main ventas parser failed: {e2}, trying simple parser")
            ventas_data = simple_parse_ventas(ventas_file)
            logger.info(f"Simple parser result: {len(ventas_data.get('headers', []))} headers, {len(ventas_data.get('details', []))} details")
            return ventas_data

def normalize_and_load_compras(compras_data: Dict[str, List[Dict]]) -> None:
    """
    Normalize compras data and load into ComprasNormalized table
    FIXED: Deterministic processing order
    """
    with DatabaseSession() as session:
        try:
            headers = compras_data.get('headers', [])
            details = compras_data.get('details', [])
            logger.info(f"Normalizing {len(headers)} compras headers and {len(details)} details")
            
            # FIXED: Sort headers by no_consecutivo for deterministic processing
            headers_sorted = sorted(headers, key=lambda x: x.get('no_consecutivo', ''))
            
            # Create a mapping of headers by no_consecutivo
            headers_map = {}
            for header in headers_sorted:
                no_consecutivo = header.get('no_consecutivo', '')
                if no_consecutivo:
                    headers_map[no_consecutivo] = header
            
            # FIXED: Sort details for deterministic processing
            details_sorted = sorted(details, key=lambda x: (
                x.get('no_consecutivo', ''), 
                x.get('nombre_clean', ''),
                x.get('cantidad', 0)
            ))
            
            # Process each detail with its corresponding header
            for detail_data in details_sorted:
                no_consecutivo = detail_data.get('no_consecutivo', '')
                header_data = headers_map.get(no_consecutivo, {})
                
                def clean_numeric(value, max_value=1000000):
                    """Clean numeric values to prevent extreme outliers"""
                    if value is None:
                        return 0.0
                    try:
                        num_val = float(value)
                        if abs(num_val) > max_value:
                            logger.warning(f"Extreme value detected and capped: {num_val} -> {max_value}")
                            return max_value if num_val > 0 else -max_value
                        return num_val
                    except (ValueError, TypeError):
                        return 0.0
                
                # Create normalized record with all fields
                normalized_data = {
                    # Product fields
                    'cabys': detail_data.get('cabys', ''),
                    'codigo': detail_data.get('codigo', ''),
                    'variacion': detail_data.get('variacion', ''),
                    'codigo_referencia': detail_data.get('codigo_referencia', ''),
                    'nombre': detail_data.get('nombre', ''),
                    'nombre_clean': detail_data.get('nombre_clean', ''),
                    'codigo_color': detail_data.get('codigo_color', ''),
                    'color': detail_data.get('color', ''),
                    'cantidad': clean_numeric(detail_data.get('cantidad', 0.0)),
                    'regalia': clean_numeric(detail_data.get('regalia', 0.0)),
                    'aplica_impuesto': detail_data.get('aplica_impuesto', ''),
                    'costo': clean_numeric(detail_data.get('costo', 0.0)),
                    'descuento': clean_numeric(detail_data.get('descuento', 0.0)),
                    'utilidad': clean_numeric(detail_data.get('utilidad', 0.0)),
                    'precio': clean_numeric(detail_data.get('precio', 0.0)),
                    'precio_unit': clean_numeric(detail_data.get('precio_unit', detail_data.get('costo', 0.0))),
                    'total': clean_numeric(detail_data.get('total', 0.0)),
                    
                    # Invoice fields (from header)
                    'fecha': header_data.get('fecha', date.today()),
                    'no_consecutivo': no_consecutivo,
                    'no_factura': header_data.get('no_factura', ''),
                    'no_guia': header_data.get('no_guia', ''),
                    'ced_juridica': header_data.get('ced_juridica', ''),
                    'proveedor': header_data.get('proveedor', ''),
                    'items': clean_numeric(header_data.get('items', 0)),
                    'fecha_vencimiento': header_data.get('fecha_vencimiento'),
                    'dias_plazo': clean_numeric(header_data.get('dias_plazo', 0)),
                    'moneda': header_data.get('moneda', 'CRC'),
                    'tipo_cambio': clean_numeric(header_data.get('tipo_cambio', 1.0)),
                    'monto': clean_numeric(header_data.get('monto', 0.0)),
                    'descuento_factura': clean_numeric(header_data.get('descuento_factura', 0.0)),
                    'iva': clean_numeric(header_data.get('iva', 0.0)),
                    'total_factura': clean_numeric(header_data.get('total_factura', 0.0)),
                    'observaciones': '',
                    'motivo': '',
                    
                    # Normalization fields
                    'es_fraccion': detail_data.get('es_fraccion', 0),
                    'factor_fraccion': detail_data.get('factor_fraccion', 1.0),
                    'qty_normalizada': detail_data.get('qty_normalizada', detail_data.get('cantidad', 0.0)),
                    
                    # Compatibility fields
                    'fecha_compra': detail_data.get('fecha_compra', header_data.get('fecha', date.today()))
                }
                
                compra_record = ComprasNormalized(**normalized_data)
                session.add(compra_record)
            
            session.commit()
            logger.info(f"Successfully normalized and loaded {len(details_sorted)} compras records")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error normalizing compras data: {e}")
            raise e

def normalize_and_load_ventas(ventas_data: Dict[str, List[Dict]]) -> None:
    """
    Normalize ventas data and load into VentasNormalized table
    FIXED: Deterministic processing order
    """
    with DatabaseSession() as session:
        try:
            headers = ventas_data.get('headers', [])
            details = ventas_data.get('details', [])
            logger.info(f"Normalizing {len(headers)} ventas headers and {len(details)} details")
            
            # FIXED: Sort headers by no_factura_interna for deterministic processing
            headers_sorted = sorted(headers, key=lambda x: x.get('no_factura_interna', ''))
            
            # Create a mapping of headers by no_factura_interna
            headers_map = {}
            for header in headers_sorted:
                no_factura = header.get('no_factura_interna', '')
                if no_factura:
                    headers_map[no_factura] = header
            
            # FIXED: Sort details for deterministic processing
            details_sorted = sorted(details, key=lambda x: (
                x.get('no_factura_interna', ''), 
                x.get('nombre_clean', ''),
                x.get('cantidad', 0)
            ))
            
            # Process each detail with its corresponding header
            for detail_data in details_sorted:
                no_factura = detail_data.get('no_factura_interna', '')
                header_data = headers_map.get(no_factura, {})
                
                def clean_numeric(value, max_value=1000000):
                    """Clean numeric values to prevent extreme outliers"""
                    if value is None:
                        return 0.0
                    try:
                        num_val = float(value)
                        if abs(num_val) > max_value:
                            logger.warning(f"Extreme value detected and capped: {num_val} -> {max_value}")
                            return max_value if num_val > 0 else -max_value
                        return num_val
                    except (ValueError, TypeError):
                        return 0.0
                
                # Create normalized record with all fields
                normalized_data = {
                    # Product fields
                    'codigo': detail_data.get('codigo', ''),
                    'cabys': detail_data.get('cabys', ''),
                    'descripcion': detail_data.get('descripcion', ''),
                    'nombre_clean': detail_data.get('nombre_clean', ''),
                    'color': detail_data.get('color', ''),
                    'cantidad': clean_numeric(detail_data.get('cantidad', 0.0)),
                    'descuento': clean_numeric(detail_data.get('descuento', 0.0)),
                    'utilidad': clean_numeric(detail_data.get('utilidad', 0.0)),
                    'costo': clean_numeric(detail_data.get('costo', 0.0)),
                    'precio_unit': clean_numeric(detail_data.get('precio_unit', 0.0)),
                    'total': clean_numeric(detail_data.get('total', 0.0)),
                    
                    # Invoice fields (from header)
                    'no_factura_interna': no_factura,
                    'no_orden': header_data.get('no_orden', ''),
                    'no_orden_compra': header_data.get('no_orden_compra', ''),
                    'tipo_gasto': header_data.get('tipo_gasto', ''),
                    'no_factura_electronica': header_data.get('no_factura_electronica', ''),
                    'tipo_documento': header_data.get('tipo_documento', ''),
                    'codigo_actividad': header_data.get('codigo_actividad', ''),
                    'facturado_por': header_data.get('facturado_por', ''),
                    'hecho_por': header_data.get('hecho_por', ''),
                    'codigo_cliente': header_data.get('codigo_cliente', ''),
                    'cliente': header_data.get('cliente', ''),
                    'cedula_fisica': header_data.get('cedula_fisica', ''),
                    'a_terceros': header_data.get('a_terceros', ''),
                    'tipo_venta': header_data.get('tipo_venta', ''),
                    'tipo_moneda': header_data.get('tipo_moneda', 'CRC'),
                    'tipo_cambio': clean_numeric(header_data.get('tipo_cambio', 1.0)),
                    'estado': header_data.get('estado', ''),
                    'fecha': header_data.get('fecha', date.today()),
                    'subtotal': clean_numeric(header_data.get('subtotal', 0.0)),
                    'impuestos': clean_numeric(header_data.get('impuestos', 0.0)),
                    'impuesto_servicios': clean_numeric(header_data.get('impuesto_servicios', 0.0)),
                    'impuestos_devueltos': clean_numeric(header_data.get('impuestos_devueltos', 0.0)),
                    'exonerado': clean_numeric(header_data.get('exonerado', 0.0)),
                    'total_factura': clean_numeric(header_data.get('total_factura', 0.0)),
                    'total_exento': clean_numeric(header_data.get('total_exento', 0.0)),
                    'total_gravado': clean_numeric(header_data.get('total_gravado', 0.0)),
                    'no_referencia_tarjeta': '',
                    'monto_tarjeta': 0.0,
                    'monto_efectivo': 0.0,
                    'no_referencia_transaccion': '',
                    'monto_transaccion': 0.0,
                    'no_referencia': '',
                    'monto_en': 0.0,
                    
                    # Normalization fields
                    'es_fraccion': detail_data.get('es_fraccion', 0),
                    'factor_fraccion': detail_data.get('factor_fraccion', 1.0),
                    'qty_normalizada': detail_data.get('qty_normalizada', detail_data.get('cantidad', 0.0)),
                    
                    # Compatibility fields
                    'nombre': detail_data.get('descripcion', ''),
                    'fecha_venta': detail_data.get('fecha_venta', header_data.get('fecha', date.today()))
                }
                
                venta_record = VentasNormalized(**normalized_data)
                session.add(venta_record)
            
            session.commit()
            logger.info(f"Successfully normalized and loaded {len(details_sorted)} ventas records")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error normalizing ventas data: {e}")
            raise e

def clear_normalized_tables() -> None:
    """
    Clear existing normalized data
    """
    with DatabaseSession() as session:
        try:
            logger.info("Clearing normalized tables")
            
            # Try to clear each table, ignore if table doesn't exist
            tables_to_clear = [
                "compras_normalized",
                "ventas_normalized", 
                "kpi_mov_diario_normalized"
            ]
            
            for table in tables_to_clear:
                try:
                    session.execute(text(f"DELETE FROM {table}"))
                    logger.info(f"Cleared table {table}")
                except Exception as table_error:
                    logger.warning(f"Could not clear table {table}: {table_error}")
            
            session.commit()
            logger.info("Normalized tables clearing completed")
        except Exception as e:
            session.rollback()
            logger.error(f"Error clearing normalized tables: {e}")
            raise e

def create_daily_aggregates_normalized_fixed(start_date: date, end_date: date) -> None:
    """
    Create daily aggregates from normalized tables
    FIXED VERSION: Deterministic aggregations with consistent ordering
    """
    with DatabaseSession() as session:
        try:
            # Clear existing aggregates for the date range
            session.execute(text("""
                DELETE FROM kpi_mov_diario_normalized 
                WHERE fecha BETWEEN :start_date AND :end_date
            """), {'start_date': start_date, 'end_date': end_date})
            
            logger.info(f"Creating DETERMINISTIC daily aggregates from normalized tables for period {start_date} to {end_date}")
            
            # FIXED: Get compras aggregates with deterministic ordering
            compras_agg = session.execute(text("""
                SELECT fecha, nombre_clean, SUM(qty_normalizada) as qty_in
                FROM compras_normalized
                WHERE fecha BETWEEN :start_date AND :end_date
                    AND nombre_clean IS NOT NULL
                    AND nombre_clean != ''
                GROUP BY fecha, nombre_clean
                ORDER BY fecha, nombre_clean  -- FIXED: Added deterministic ordering
            """), {'start_date': start_date, 'end_date': end_date}).fetchall()
            
            logger.info(f"Compras aggregates found: {len(compras_agg)}")
            
            # FIXED: Get ventas aggregates with deterministic ordering
            ventas_agg = session.execute(text("""
                SELECT fecha, nombre_clean, SUM(qty_normalizada) as qty_out
                FROM ventas_normalized
                WHERE fecha BETWEEN :start_date AND :end_date
                    AND nombre_clean IS NOT NULL
                    AND nombre_clean != ''
                GROUP BY fecha, nombre_clean
                ORDER BY fecha, nombre_clean  -- FIXED: Added deterministic ordering
            """), {'start_date': start_date, 'end_date': end_date}).fetchall()
            
            logger.info(f"Ventas aggregates found: {len(ventas_agg)}")
            
            # FIXED: Combine aggregates by date and product with deterministic ordering
            daily_movements = {}
            
            # Add purchases (sorted for consistency)
            for row in sorted(compras_agg, key=lambda x: (x.fecha, x.nombre_clean)):
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
            
            # Add sales (sorted for consistency)
            for row in sorted(ventas_agg, key=lambda x: (x.fecha, x.nombre_clean)):
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
            
            # FIXED: Insert aggregated data in deterministic order
            logger.info(f"Creating {len(daily_movements)} daily movement records")
            
            # Sort by key for deterministic insertion order
            sorted_movements = sorted(daily_movements.items(), key=lambda x: (x[0][0], x[0][1]))
            
            for i, (key, movement_data) in enumerate(sorted_movements):
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
                
                kpi_mov = KpiMovDiarioNormalized(**movement_data)
                session.add(kpi_mov)
            
            session.commit()
            logger.info(f"Created {len(daily_movements)} DETERMINISTIC daily movement records for period {start_date} to {end_date}")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error creating daily aggregates from normalized tables: {e}")
            raise e

# Keep original function for backward compatibility but use fixed version
def create_daily_aggregates_normalized(start_date: date, end_date: date) -> None:
    """
    Create daily aggregates from normalized tables
    This now calls the fixed version for deterministic results
    """
    logger.info("Using FIXED version of create_daily_aggregates_normalized for deterministic results")
    create_daily_aggregates_normalized_fixed(start_date, end_date)

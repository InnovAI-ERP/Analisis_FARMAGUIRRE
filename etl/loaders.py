"""
Data loaders for inserting parsed data into database and creating aggregations
"""

import pandas as pd
from datetime import date, datetime
from typing import Dict, List
import logging
from sqlalchemy import text
from db.database import DatabaseSession
from db.models import (
    ComprasHeader, ComprasDetail, VentasHeader, VentasDetail,
    KpiMovDiario, Productos
)
from utils.dates_numbers import get_product_key

logger = logging.getLogger(__name__)

def load_compras_data(compras_data: Dict[str, List[Dict]]) -> None:
    """
    Load purchase data into database
    
    Args:
        compras_data: Dictionary with 'headers' and 'details' lists
    """
    with DatabaseSession() as session:
        try:
            # Clear existing data (for demo purposes - in production you might want to be more selective)
            session.query(ComprasDetail).delete()
            session.query(ComprasHeader).delete()
            
            # Load headers
            headers = compras_data.get('headers', [])
            for header_data in headers:
                # Ensure fecha is a proper date object
                if 'fecha' in header_data and header_data['fecha']:
                    fecha = header_data['fecha']
                    if isinstance(fecha, str):
                        from datetime import datetime
                        try:
                            header_data['fecha'] = datetime.strptime(fecha, '%Y-%m-%d').date()
                        except ValueError:
                            try:
                                header_data['fecha'] = datetime.strptime(fecha, '%d-%m-%Y').date()
                            except ValueError:
                                logger.error(f"Could not parse date string: {fecha}")
                                continue
                    elif hasattr(fecha, 'date'):
                        header_data['fecha'] = fecha.date()
                
                header = ComprasHeader(**header_data)
                session.add(header)
            
            # Flush to get IDs
            session.flush()
            
            # Load details
            details = compras_data.get('details', [])
            logger.info(f"Loading {len(details)} compras details")
            for i, detail_data in enumerate(details):
                try:
                    # Ensure all required fields are present
                    if 'qty_normalizada' not in detail_data:
                        detail_data['qty_normalizada'] = detail_data.get('cantidad', 0)
                    if 'es_fraccion' not in detail_data:
                        detail_data['es_fraccion'] = 0
                    if 'factor_fraccion' not in detail_data:
                        detail_data['factor_fraccion'] = 1.0
                    
                    detail = ComprasDetail(**detail_data)
                    session.add(detail)
                    
                    if i < 3:  # Log first few records for debugging
                        logger.info(f"Compras detail {i}: {detail_data.get('nombre_clean', 'Unknown')} - Qty: {detail_data.get('cantidad', 0)}")
                        
                except Exception as e:
                    logger.error(f"Error loading compras detail {i}: {e}")
                    logger.error(f"Detail data: {detail_data}")
                    continue
            
            session.commit()
            logger.info(f"Loaded {len(headers)} purchase headers and {len(details)} detail lines")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading purchase data: {e}")
            raise e

def load_ventas_data(ventas_data: Dict[str, List[Dict]]) -> None:
    """
    Load sales data into database
    
    Args:
        ventas_data: Dictionary with 'headers' and 'details' lists
    """
    with DatabaseSession() as session:
        try:
            # Clear existing data
            session.query(VentasDetail).delete()
            session.query(VentasHeader).delete()
            
            # Load headers
            headers = ventas_data.get('headers', [])
            for header_data in headers:
                # Ensure fecha is a proper date object
                if 'fecha' in header_data and header_data['fecha']:
                    fecha = header_data['fecha']
                    if isinstance(fecha, str):
                        from datetime import datetime
                        try:
                            header_data['fecha'] = datetime.strptime(fecha, '%Y-%m-%d').date()
                        except ValueError:
                            try:
                                header_data['fecha'] = datetime.strptime(fecha, '%d-%m-%Y').date()
                            except ValueError:
                                logger.error(f"Could not parse date string: {fecha}")
                                continue
                    elif hasattr(fecha, 'date'):
                        header_data['fecha'] = fecha.date()
                
                header = VentasHeader(**header_data)
                session.add(header)
            
            # Flush to get IDs
            session.flush()
            
            # Load details
            details = ventas_data.get('details', [])
            logger.info(f"Loading {len(details)} ventas details")
            for i, detail_data in enumerate(details):
                try:
                    # Ensure all required fields are present
                    if 'qty_normalizada' not in detail_data:
                        detail_data['qty_normalizada'] = detail_data.get('cantidad', 0)
                    if 'es_fraccion' not in detail_data:
                        detail_data['es_fraccion'] = 0
                    if 'factor_fraccion' not in detail_data:
                        detail_data['factor_fraccion'] = 1.0
                    
                    detail = VentasDetail(**detail_data)
                    session.add(detail)
                    
                    if i < 3:  # Log first few records for debugging
                        logger.info(f"Ventas detail {i}: {detail_data.get('nombre_clean', 'Unknown')} - Qty: {detail_data.get('cantidad', 0)}")
                        
                except Exception as e:
                    logger.error(f"Error loading ventas detail {i}: {e}")
                    logger.error(f"Detail data: {detail_data}")
                    continue
            
            session.commit()
            logger.info(f"Loaded {len(headers)} sales headers and {len(details)} detail lines")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error loading sales data: {e}")
            raise e

def update_productos_catalog() -> None:
    """
    Update the consolidated products catalog from both purchases and sales
    """
    with DatabaseSession() as session:
        try:
            # Clear existing catalog
            session.query(Productos).delete()
            
            # Get unique products from purchases
            compras_products = session.execute(text("""
                SELECT DISTINCT cabys, nombre_clean, codigo as codigo_alt
                FROM compras_detail 
                WHERE cabys IS NOT NULL AND nombre_clean IS NOT NULL
            """)).fetchall()
            
            # Get unique products from sales
            ventas_products = session.execute(text("""
                SELECT DISTINCT cabys, nombre_clean, codigo as codigo_alt
                FROM ventas_detail 
                WHERE cabys IS NOT NULL AND nombre_clean IS NOT NULL
            """)).fetchall()
            
            # Combine and deduplicate
            all_products = {}
            
            for row in compras_products:
                key = get_product_key(row.cabys, row.nombre_clean)
                if key not in all_products:
                    all_products[key] = {
                        'cabys': row.cabys,
                        'nombre_clean': row.nombre_clean,
                        'codigo_alt': row.codigo_alt
                    }
            
            for row in ventas_products:
                key = get_product_key(row.cabys, row.nombre_clean)
                if key not in all_products:
                    all_products[key] = {
                        'cabys': row.cabys,
                        'nombre_clean': row.nombre_clean,
                        'codigo_alt': row.codigo_alt
                    }
            
            # Insert into catalog
            for product_data in all_products.values():
                producto = Productos(**product_data)
                session.add(producto)
            
            session.commit()
            logger.info(f"Updated products catalog with {len(all_products)} unique products")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error updating products catalog: {e}")
            raise e

def create_daily_aggregates(start_date: date, end_date: date) -> None:
    """
    Create daily movement aggregates for the specified date range
    
    Args:
        start_date: Start date for aggregation
        end_date: End date for aggregation
    """
    with DatabaseSession() as session:
        try:
            # Clear existing aggregates for the date range
            session.execute(text("""
                DELETE FROM kpi_mov_diario 
                WHERE fecha BETWEEN :start_date AND :end_date
            """), {'start_date': start_date, 'end_date': end_date})
            
            # Create aggregates from purchases
            logger.info(f"Creating daily aggregates for period {start_date} to {end_date}")
            
            # First, let's check what data we have
            compras_count = session.execute(text("SELECT COUNT(*) FROM compras_detail")).scalar()
            ventas_count = session.execute(text("SELECT COUNT(*) FROM ventas_detail")).scalar()
            logger.info(f"Total records: {compras_count} compras, {ventas_count} ventas")
            
            # Try using the denormalized fecha_compra field first, fallback to header join
            compras_agg = session.execute(text("""
                SELECT 
                    COALESCE(cd.fecha_compra, ch.fecha) as fecha, 
                    cd.nombre_clean, 
                    SUM(cd.qty_normalizada) as qty_in
                FROM compras_detail cd
                LEFT JOIN compras_header ch ON ch.no_consecutivo = cd.no_consecutivo
                WHERE (cd.fecha_compra BETWEEN :start_date AND :end_date 
                       OR ch.fecha BETWEEN :start_date AND :end_date)
                    AND cd.nombre_clean IS NOT NULL
                    AND cd.nombre_clean != ''
                GROUP BY COALESCE(cd.fecha_compra, ch.fecha), cd.nombre_clean
            """), {'start_date': start_date, 'end_date': end_date}).fetchall()
            
            logger.info(f"Compras aggregates found: {len(compras_agg)}")
            
            # Aggregate sales by date and product (using denormalized fecha_venta field)
            ventas_agg = session.execute(text("""
                SELECT 
                    COALESCE(vd.fecha_venta, vh.fecha) as fecha, 
                    vd.nombre_clean, 
                    SUM(vd.qty_normalizada) as qty_out
                FROM ventas_detail vd
                LEFT JOIN ventas_header vh ON vh.no_factura_interna = vd.no_factura_interna
                WHERE (vd.fecha_venta BETWEEN :start_date AND :end_date 
                       OR vh.fecha BETWEEN :start_date AND :end_date)
                    AND vd.nombre_clean IS NOT NULL
                    AND vd.nombre_clean != ''
                GROUP BY COALESCE(vd.fecha_venta, vh.fecha), vd.nombre_clean
            """), {'start_date': start_date, 'end_date': end_date}).fetchall()
            
            logger.info(f"Ventas aggregates found: {len(ventas_agg)}")
            
            # Combine aggregates by date and product
            daily_movements = {}
            
            # Add purchases
            for row in compras_agg:
                # Ensure fecha is a date object, not datetime
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
                # Ensure fecha is a date object, not datetime
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
                # Ensure fecha is a proper date object before creating the model
                fecha = movement_data['fecha']
                if isinstance(fecha, str):
                    # Convert string to date object
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
                
                # Update the movement data with proper date
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
            logger.error(f"Error creating daily aggregates: {e}")
            raise e

def load_to_database(compras_data: Dict[str, List[Dict]], ventas_data: Dict[str, List[Dict]]) -> None:
    """
    Load all data to database and update catalogs
    
    Args:
        compras_data: Purchase data dictionary
        ventas_data: Sales data dictionary
    """
    try:
        logger.info("Loading purchase data...")
        load_compras_data(compras_data)
        
        logger.info("Loading sales data...")
        load_ventas_data(ventas_data)
        
        logger.info("Updating products catalog...")
        update_productos_catalog()
        
        logger.info("Data loading completed successfully")
        
    except Exception as e:
        logger.error(f"Error in data loading process: {e}")
        raise e

def get_date_range_from_data() -> tuple:
    """
    Get the actual date range from loaded data
    
    Returns:
        Tuple of (start_date, end_date) or (None, None) if no data
    """
    with DatabaseSession() as session:
        try:
            # Get date range from purchases
            compras_dates = session.execute(text("""
                SELECT MIN(fecha) as min_date, MAX(fecha) as max_date
                FROM compras_header
            """)).fetchone()
            
            # Get date range from sales
            ventas_dates = session.execute(text("""
                SELECT MIN(fecha) as min_date, MAX(fecha) as max_date
                FROM ventas_header
            """)).fetchone()
            
            # Combine ranges
            min_dates = [d for d in [compras_dates.min_date, ventas_dates.min_date] if d]
            max_dates = [d for d in [compras_dates.max_date, ventas_dates.max_date] if d]
            
            if min_dates and max_dates:
                return min(min_dates), max(max_dates)
            
            return None, None
            
        except Exception as e:
            logger.error(f"Error getting date range from data: {e}")
            return None, None

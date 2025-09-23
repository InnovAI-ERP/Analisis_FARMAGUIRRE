"""
Normalized parser for Ventas (Sales) Excel files
Creates a single normalized table with invoice data + product details
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from datetime import date, datetime
from utils.dates_numbers import parse_date, normalize_number, normalize_text, clean_product_name, is_fraction_product

logger = logging.getLogger(__name__)

def parse_ventas_normalized(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Parse ventas file and create normalized table with invoice + product data
    
    Args:
        file_path_or_buffer: File path or buffer containing the Excel file
        
    Returns:
        Dictionary with 'headers' and 'details' lists (normalized structure)
    """
    try:
        # Read all sheets and try each one
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Ventas normalized parser - Found sheets: {excel_file.sheet_names}")
        
        for sheet_name in excel_file.sheet_names:
            try:
                logger.info(f"Ventas normalized parser - Trying sheet: {sheet_name}")
                df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_name, header=None)
                
                if df.empty:
                    continue
                
                logger.info(f"Ventas normalized parser - Sheet {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
                
                normalized_data = normalize_ventas_data(df, sheet_name)
                
                if normalized_data:
                    logger.info(f"Ventas normalized parser - Success: {len(normalized_data)} normalized records")
                    return {
                        'headers': [],  # We'll create headers from the normalized data
                        'details': normalized_data
                    }
                    
            except Exception as e:
                logger.error(f"Ventas normalized parser - Error with sheet {sheet_name}: {e}")
                continue
        
        logger.warning("Ventas normalized parser - No parseable data found")
        return {'headers': [], 'details': []}
        
    except Exception as e:
        logger.error(f"Ventas normalized parser - General error: {e}")
        return {'headers': [], 'details': []}

def normalize_ventas_data(df: pd.DataFrame, sheet_name: str) -> List[Dict]:
    """
    Normalize ventas data by combining invoice headers with product details
    """
    normalized_records = []
    
    try:
        current_invoice_data = {}
        in_products_section = False
        
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isna().all():
                continue
            
            row_text = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            
            # Debug: Log first 10 rows to see what we're getting
            if idx < 10:
                logger.info(f"Row {idx}: {row_text[:100]}...")
            
            # Check if this is an invoice header section
            if any(keyword in row_text for keyword in ['no. factura interna', 'factura interna', 'no factura', 'factura']):
                logger.info(f"Found invoice header at row {idx}: {row_text[:50]}...")
                current_invoice_data = extract_invoice_data_ventas(df, idx)
                in_products_section = False
                continue
            
            # Check if this is the start of products section
            if any(keyword in row_text for keyword in ['código', 'cabys', 'descripción', 'productos', 'codigo']):
                logger.info(f"Found products section at row {idx}: {row_text[:50]}...")
                in_products_section = True
                continue
            
            # If we're in products section and have invoice data, extract product
            if in_products_section and current_invoice_data:
                product_data = extract_product_data_ventas(row, idx, current_invoice_data)
                if product_data:
                    normalized_records.append(product_data)
        
        logger.info(f"Normalized {len(normalized_records)} ventas records")
        return normalized_records
        
    except Exception as e:
        logger.error(f"Error normalizing ventas data: {e}")
        return []

def extract_invoice_data_ventas(df: pd.DataFrame, start_idx: int) -> Dict:
    """
    Extract invoice header data from ventas file
    """
    invoice_data = {
        'no_factura_interna': '',
        'no_orden': '',
        'no_orden_compra': '',
        'tipo_gasto': '',
        'no_factura_electronica': '',
        'tipo_documento': '',
        'codigo_actividad': '',
        'facturado_por': '',
        'hecho_por': '',
        'codigo_cliente': '',
        'cliente': '',
        'cedula_fisica': '',
        'a_terceros': '',
        'tipo_venta': '',
        'tipo_moneda': '',
        'tipo_cambio': 0.0,
        'estado': '',
        'fecha': date.today(),
        'subtotal': 0.0,
        'impuestos': 0.0,
        'impuesto_servicios': 0.0,
        'impuestos_devueltos': 0.0,
        'exonerado': 0.0,
        'descuento': 0.0,
        'total': 0.0,
        'total_exento': 0.0,
        'total_gravado': 0.0,
        'no_referencia_tarjeta': '',
        'monto_tarjeta': 0.0,
        'monto_efectivo': 0.0,
        'no_referencia_transaccion': '',
        'monto_transaccion': 0.0,
        'no_referencia': '',
        'monto_en': 0.0
    }
    
    try:
        # Look for invoice data in the next several rows
        for i in range(start_idx, min(start_idx + 20, len(df))):
            row = df.iloc[i]
            
            for j, cell in enumerate(row):
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    
                    # Try to identify different fields based on patterns
                    if 'factura' in cell_str.lower() and len(cell_str) > 5:
                        if not invoice_data['no_factura_interna']:
                            # Extract number from factura field
                            import re
                            numbers = re.findall(r'\d+', cell_str)
                            if numbers:
                                invoice_data['no_factura_interna'] = numbers[-1]
                    
                    # Look for dates
                    parsed_date = parse_date(cell, dayfirst=True)
                    if parsed_date and not invoice_data['fecha']:
                        invoice_data['fecha'] = parsed_date.date() if hasattr(parsed_date, 'date') else parsed_date
                    
                    # Look for client information (longer text fields)
                    if len(cell_str) > 10 and not cell_str.replace('.', '').replace(',', '').isdigit():
                        if not invoice_data['cliente']:
                            invoice_data['cliente'] = cell_str
                    
                    # Look for numeric values (totals, etc.)
                    num_val = normalize_number(cell)
                    if num_val is not None and num_val > 0:
                        if not invoice_data['total'] and num_val > 100:  # Likely a total
                            invoice_data['total'] = num_val
        
        # Set defaults if not found
        if not invoice_data['no_factura_interna']:
            invoice_data['no_factura_interna'] = f"VENTA_{start_idx}"
        
        logger.info(f"Extracted invoice data: {invoice_data['no_factura_interna']}")
        return invoice_data
        
    except Exception as e:
        logger.error(f"Error extracting invoice data at row {start_idx}: {e}")
        return invoice_data

def extract_product_data_ventas(row: pd.Series, row_idx: int, invoice_data: Dict) -> Optional[Dict]:
    """
    Extract product data and combine with invoice data
    """
    try:
        # Initialize product fields
        codigo = ""
        cabys = ""
        descripcion = ""
        color = ""
        cantidad = 0.0
        descuento = 0.0
        utilidad = 0.0
        costo = 0.0
        precio_unit = 0.0
        total = 0.0
        
        # Extract data from row
        for i, cell in enumerate(row):
            if pd.notna(cell):
                cell_str = str(cell).strip()
                
                # Try to identify product description (longest text)
                if len(cell_str) > 5 and not cell_str.replace('.', '').replace(',', '').isdigit():
                    if not descripcion or len(cell_str) > len(descripcion):
                        descripcion = cell_str
                
                # Try to identify codes (shorter alphanumeric)
                elif len(cell_str) <= 15 and (cell_str.isdigit() or any(c.isalpha() for c in cell_str)):
                    if not codigo:
                        codigo = cell_str
                    elif not cabys:
                        cabys = cell_str
                
                # Try to identify numeric values
                num_val = normalize_number(cell)
                if num_val is not None:
                    if cantidad == 0.0 and 0 < num_val < 1000:  # Likely quantity
                        cantidad = num_val
                    elif costo == 0.0 and num_val > 0:  # Likely cost
                        costo = num_val
                    elif precio_unit == 0.0 and num_val > costo:  # Likely price
                        precio_unit = num_val
                    elif total == 0.0 and num_val > precio_unit:  # Likely total
                        total = num_val
        
        # Validate we have minimum required data
        if descripcion and cantidad > 0:
            es_fraccion = is_fraction_product(descripcion)
            nombre_clean = clean_product_name(descripcion, remove_frac_prefix=False)
            
            # Create normalized record combining invoice + product data
            normalized_record = {
                # Product fields
                'codigo': codigo,
                'cabys': cabys,
                'descripcion': descripcion,
                'nombre_clean': nombre_clean,
                'color': color,
                'cantidad': cantidad,
                'descuento': descuento,
                'utilidad': utilidad,
                'costo': costo,
                'precio_unit': precio_unit,
                'total': total,
                
                # Invoice fields (denormalized)
                'no_factura_interna': invoice_data['no_factura_interna'],
                'no_orden': invoice_data['no_orden'],
                'no_orden_compra': invoice_data['no_orden_compra'],
                'tipo_gasto': invoice_data['tipo_gasto'],
                'no_factura_electronica': invoice_data['no_factura_electronica'],
                'tipo_documento': invoice_data['tipo_documento'],
                'codigo_actividad': invoice_data['codigo_actividad'],
                'facturado_por': invoice_data['facturado_por'],
                'hecho_por': invoice_data['hecho_por'],
                'codigo_cliente': invoice_data['codigo_cliente'],
                'cliente': invoice_data['cliente'],
                'cedula_fisica': invoice_data['cedula_fisica'],
                'a_terceros': invoice_data['a_terceros'],
                'tipo_venta': invoice_data['tipo_venta'],
                'tipo_moneda': invoice_data['tipo_moneda'],
                'tipo_cambio': invoice_data['tipo_cambio'],
                'estado': invoice_data['estado'],
                'fecha': invoice_data['fecha'],
                'subtotal': invoice_data['subtotal'],
                'impuestos': invoice_data['impuestos'],
                'impuesto_servicios': invoice_data['impuesto_servicios'],
                'impuestos_devueltos': invoice_data['impuestos_devueltos'],
                'exonerado': invoice_data['exonerado'],
                'total_factura': invoice_data['total'],
                'total_exento': invoice_data['total_exento'],
                'total_gravado': invoice_data['total_gravado'],
                'no_referencia_tarjeta': invoice_data['no_referencia_tarjeta'],
                'monto_tarjeta': invoice_data['monto_tarjeta'],
                'monto_efectivo': invoice_data['monto_efectivo'],
                'no_referencia_transaccion': invoice_data['no_referencia_transaccion'],
                'monto_transaccion': invoice_data['monto_transaccion'],
                'no_referencia': invoice_data['no_referencia'],
                'monto_en': invoice_data['monto_en'],
                
                # Normalization fields
                'es_fraccion': 1 if es_fraccion else 0,
                'factor_fraccion': 1.0,
                'qty_normalizada': cantidad
            }
            
            return normalized_record
        
        return None
        
    except Exception as e:
        logger.debug(f"Error extracting product from row {row_idx}: {e}")
        return None

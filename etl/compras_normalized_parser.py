"""
Normalized parser for Compras (Purchases) Excel files
Creates a single normalized table with invoice data + product details
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from datetime import date, datetime
from utils.dates_numbers import parse_date, normalize_number, normalize_text, clean_product_name, is_fraction_product

logger = logging.getLogger(__name__)

def parse_compras_normalized(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Parse compras file and create normalized table with invoice + product data
    
    Args:
        file_path_or_buffer: File path or buffer containing the Excel file
        
    Returns:
        Dictionary with 'headers' and 'details' lists (normalized structure)
    """
    try:
        # Read all sheets and try each one
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Compras normalized parser - Found sheets: {excel_file.sheet_names}")
        
        for sheet_name in excel_file.sheet_names:
            try:
                logger.info(f"Compras normalized parser - Trying sheet: {sheet_name}")
                df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_name, header=None)
                
                if df.empty:
                    continue
                
                logger.info(f"Compras normalized parser - Sheet {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
                
                normalized_data = normalize_compras_data(df, sheet_name)
                
                if normalized_data:
                    logger.info(f"Compras normalized parser - Success: {len(normalized_data)} normalized records")
                    return {
                        'headers': [],  # We'll create headers from the normalized data
                        'details': normalized_data
                    }
                    
            except Exception as e:
                logger.error(f"Compras normalized parser - Error with sheet {sheet_name}: {e}")
                continue
        
        logger.warning("Compras normalized parser - No parseable data found")
        return {'headers': [], 'details': []}
        
    except Exception as e:
        logger.error(f"Compras normalized parser - General error: {e}")
        return {'headers': [], 'details': []}

def normalize_compras_data(df: pd.DataFrame, sheet_name: str) -> List[Dict]:
    """
    Normalize compras data by combining invoice headers with product details
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
            if any(keyword in row_text for keyword in ['fecha', 'no consecutivo', 'proveedor', 'factura', 'consecutivo']):
                logger.info(f"Found invoice header at row {idx}: {row_text[:50]}...")
                current_invoice_data = extract_invoice_data_compras(df, idx)
                in_products_section = False
                continue
            
            # Check if this is the start of products section
            if any(keyword in row_text for keyword in ['cabys', 'código', 'nombre', 'productos', 'cantidad', 'codigo']):
                logger.info(f"Found products section at row {idx}: {row_text[:50]}...")
                in_products_section = True
                continue
            
            # If we're in products section and have invoice data, extract product
            if in_products_section and current_invoice_data:
                product_data = extract_product_data_compras(row, idx, current_invoice_data)
                if product_data:
                    normalized_records.append(product_data)
        
        logger.info(f"Normalized {len(normalized_records)} compras records")
        return normalized_records
        
    except Exception as e:
        logger.error(f"Error normalizing compras data: {e}")
        return []

def extract_invoice_data_compras(df: pd.DataFrame, start_idx: int) -> Dict:
    """
    Extract invoice header data from compras file
    """
    invoice_data = {
        'fecha': date.today(),
        'no_consecutivo': '',
        'no_factura': '',
        'no_guia': '',
        'ced_juridica': '',
        'proveedor': '',
        'items': 0,
        'fecha_vencimiento': None,
        'dias_plazo': 0,
        'moneda': '',
        'tipo_cambio': 1.0,
        'monto': 0.0,
        'descuento': 0.0,
        'iva': 0.0,
        'total': 0.0,
        'observaciones': '',
        'motivo': ''
    }
    
    try:
        # Look for invoice data in the next several rows
        for i in range(start_idx, min(start_idx + 20, len(df))):
            row = df.iloc[i]
            
            for j, cell in enumerate(row):
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    
                    # Try to identify different fields based on patterns
                    if 'consecutivo' in cell_str.lower():
                        # Extract number from consecutivo field
                        import re
                        numbers = re.findall(r'\d+', cell_str)
                        if numbers:
                            invoice_data['no_consecutivo'] = numbers[-1]
                    
                    elif 'factura' in cell_str.lower() and len(cell_str) > 5:
                        # Extract factura number
                        import re
                        numbers = re.findall(r'\d+', cell_str)
                        if numbers:
                            invoice_data['no_factura'] = numbers[-1]
                    
                    elif 'guia' in cell_str.lower():
                        # Extract guia number
                        import re
                        numbers = re.findall(r'\d+', cell_str)
                        if numbers:
                            invoice_data['no_guia'] = numbers[-1]
                    
                    # Look for dates
                    parsed_date = parse_date(cell, dayfirst=True)
                    if parsed_date:
                        if 'vencimiento' in row_text.lower():
                            invoice_data['fecha_vencimiento'] = parsed_date.date() if hasattr(parsed_date, 'date') else parsed_date
                        else:
                            invoice_data['fecha'] = parsed_date.date() if hasattr(parsed_date, 'date') else parsed_date
                    
                    # Look for supplier information (longer text fields)
                    if len(cell_str) > 10 and not cell_str.replace('.', '').replace(',', '').isdigit():
                        if not invoice_data['proveedor'] and 'proveedor' in row_text.lower():
                            invoice_data['proveedor'] = cell_str
                    
                    # Look for numeric values (totals, etc.)
                    num_val = normalize_number(cell)
                    if num_val is not None and num_val > 0:
                        if not invoice_data['total'] and num_val > 100:  # Likely a total
                            invoice_data['total'] = num_val
        
        # Set defaults if not found
        if not invoice_data['no_consecutivo']:
            invoice_data['no_consecutivo'] = f"COMPRA_{start_idx}"
        
        logger.info(f"Extracted invoice data: {invoice_data['no_consecutivo']}")
        return invoice_data
        
    except Exception as e:
        logger.error(f"Error extracting invoice data at row {start_idx}: {e}")
        return invoice_data

def extract_product_data_compras(row: pd.Series, row_idx: int, invoice_data: Dict) -> Optional[Dict]:
    """
    Extract product data and combine with invoice data
    """
    try:
        # Initialize product fields based on the structure you specified
        cabys = ""
        codigo = ""
        variacion = ""
        codigo_referencia = ""
        nombre = ""
        codigo_color = ""
        color = ""
        cantidad = 0.0
        regalia = 0.0
        aplica_impuesto = ""
        costo = 0.0
        descuento = 0.0
        utilidad = 0.0
        precio = 0.0
        total = 0.0
        
        # Extract data from row
        for i, cell in enumerate(row):
            if pd.notna(cell):
                cell_str = str(cell).strip()
                
                # Try to identify product name (longest text)
                if len(cell_str) > 5 and not cell_str.replace('.', '').replace(',', '').isdigit():
                    if not nombre or len(cell_str) > len(nombre):
                        nombre = cell_str
                
                # Try to identify codes (shorter alphanumeric)
                elif len(cell_str) <= 15 and (cell_str.isdigit() or any(c.isalpha() for c in cell_str)):
                    if not cabys:
                        cabys = cell_str
                    elif not codigo:
                        codigo = cell_str
                    elif not codigo_referencia:
                        codigo_referencia = cell_str
                
                # Try to identify numeric values
                num_val = normalize_number(cell)
                if num_val is not None:
                    if cantidad == 0.0 and 0 < num_val < 1000:  # Likely quantity
                        cantidad = num_val
                    elif costo == 0.0 and num_val > 0:  # Likely cost
                        costo = num_val
                    elif precio == 0.0 and num_val > costo:  # Likely price
                        precio = num_val
                    elif total == 0.0 and num_val > precio:  # Likely total
                        total = num_val
        
        # Validate we have minimum required data
        if nombre and cantidad > 0:
            es_fraccion = is_fraction_product(nombre)
            nombre_clean = clean_product_name(nombre, remove_frac_prefix=False)
            
            # Create normalized record combining invoice + product data
            normalized_record = {
                # Product fields
                'cabys': cabys,
                'codigo': codigo,
                'variacion': variacion,
                'codigo_referencia': codigo_referencia,
                'nombre': nombre,
                'nombre_clean': nombre_clean,
                'codigo_color': codigo_color,
                'color': color,
                'cantidad': cantidad,
                'regalia': regalia,
                'aplica_impuesto': aplica_impuesto,
                'costo': costo,
                'descuento': descuento,
                'utilidad': utilidad,
                'precio': precio,
                'precio_unit': precio,  # For compatibility
                'total': total,
                
                # Invoice fields (denormalized)
                'fecha': invoice_data['fecha'],
                'no_consecutivo': invoice_data['no_consecutivo'],
                'no_factura': invoice_data['no_factura'],
                'no_guia': invoice_data['no_guia'],
                'ced_juridica': invoice_data['ced_juridica'],
                'proveedor': invoice_data['proveedor'],
                'items': invoice_data['items'],
                'fecha_vencimiento': invoice_data['fecha_vencimiento'],
                'dias_plazo': invoice_data['dias_plazo'],
                'moneda': invoice_data['moneda'],
                'tipo_cambio': invoice_data['tipo_cambio'],
                'monto': invoice_data['monto'],
                'descuento_factura': invoice_data['descuento'],
                'iva': invoice_data['iva'],
                'total_factura': invoice_data['total'],
                'observaciones': invoice_data['observaciones'],
                'motivo': invoice_data['motivo'],
                
                # Denormalized header fields for compatibility
                'fecha_compra': invoice_data['fecha'],
                
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

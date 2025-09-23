"""
Simple and robust parser for Excel files
This parser is more flexible and handles various Excel formats
"""

import pandas as pd
import numpy as np
from datetime import datetime, date
import logging
from typing import Dict, List, Optional
from utils.dates_numbers import parse_date, normalize_number, normalize_text, clean_product_name, is_fraction_product, calculate_fraction_factor

logger = logging.getLogger(__name__)

def simple_parse_compras(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Simple parser for purchases file - tries to extract any tabular data
    """
    try:
        # Read all sheets
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Found sheets: {excel_file.sheet_names}")
        
        # Try each sheet
        for sheet_name in excel_file.sheet_names:
            try:
                logger.info(f"Trying to parse sheet: {sheet_name}")
                df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_name, header=None)
                
                if df.empty:
                    continue
                
                logger.info(f"Sheet {sheet_name} has {len(df)} rows and {len(df.columns)} columns")
                
                # Look for data that looks like purchases
                headers = []
                details = []
                
                # Simple approach: look for rows with date-like values and consecutive numbers
                for idx, row in df.iterrows():
                    try:
                        # Skip empty rows
                        if row.isna().all():
                            continue
                        
                        # Look for potential invoice data
                        row_values = [str(cell).strip() for cell in row if pd.notna(cell)]
                        
                        if len(row_values) >= 6:  # Minimum columns for invoice
                            # Try to parse as invoice header
                            fecha = parse_date(row.iloc[0] if len(row) > 0 else None, dayfirst=True)
                            
                            if fecha:
                                # This looks like an invoice header
                                header_data = {
                                    'fecha': fecha,
                                    'no_consecutivo': normalize_text(row.iloc[1] if len(row) > 1 else f"AUTO_{idx}"),
                                    'no_factura': normalize_text(row.iloc[2] if len(row) > 2 else ""),
                                    'no_guia': normalize_text(row.iloc[3] if len(row) > 3 else ""),
                                    'ced_juridica': normalize_text(row.iloc[4] if len(row) > 4 else ""),
                                    'proveedor': normalize_text(row.iloc[5] if len(row) > 5 else "")
                                }
                                headers.append(header_data)
                                continue
                        
                        # Try to parse as detail line
                        if len(row_values) >= 8:  # Minimum for detail
                            cantidad = normalize_number(row.iloc[7] if len(row) > 7 else None)
                            precio = normalize_number(row.iloc[10] if len(row) > 10 else None)
                            
                            if cantidad is not None and precio is not None:
                                detail_data = {
                                    'cabys': normalize_text(row.iloc[0] if len(row) > 0 else ""),
                                    'codigo': normalize_text(row.iloc[1] if len(row) > 1 else ""),
                                    'variacion': normalize_text(row.iloc[2] if len(row) > 2 else ""),
                                    'codigo_referencia': normalize_text(row.iloc[3] if len(row) > 3 else ""),
                                    'nombre': normalize_text(row.iloc[4] if len(row) > 4 else ""),
                                    'codigo_color': normalize_text(row.iloc[5] if len(row) > 5 else ""),
                                    'color': normalize_text(row.iloc[6] if len(row) > 6 else ""),
                                    'cantidad': cantidad,
                                    'descuento': normalize_number(row.iloc[8] if len(row) > 8 else 0),
                                    'utilidad': normalize_number(row.iloc[9] if len(row) > 9 else 0),
                                    'precio_unit': precio,
                                    'no_consecutivo': headers[-1]['no_consecutivo'] if headers else f"AUTO_{idx}"
                                }
                                
                                detail_data['nombre_clean'] = clean_product_name(detail_data['nombre'], remove_frac_prefix=False)
                                
                                if detail_data['cabys'] or detail_data['nombre_clean']:
                                    details.append(detail_data)
                    
                    except Exception as e:
                        logger.debug(f"Error parsing row {idx}: {e}")
                        continue
                
                if headers or details:
                    logger.info(f"Found {len(headers)} headers and {len(details)} details in sheet {sheet_name}")
                    return {'headers': headers, 'details': details}
                
            except Exception as e:
                logger.error(f"Error parsing sheet {sheet_name}: {e}")
                continue
        
        # If no data found, return empty
        logger.warning("No parseable data found in any sheet")
        return {'headers': [], 'details': []}
        
    except Exception as e:
        logger.error(f"Error in simple_parse_compras: {e}")
        return {'headers': [], 'details': []}

def simple_parse_ventas(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Simple parser for sales file - tries to extract any tabular data
    """
    try:
        # Read all sheets
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Found sheets: {excel_file.sheet_names}")
        
        # Try each sheet
        for sheet_name in excel_file.sheet_names:
            try:
                logger.info(f"Trying to parse sheet: {sheet_name}")
                df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_name, header=None)
                
                if df.empty:
                    continue
                
                logger.info(f"Sheet {sheet_name} has {len(df)} rows and {len(df.columns)} columns")
                
                headers = []
                details = []
                current_invoice = None
                
                # Look for sales data
                for idx, row in df.iterrows():
                    try:
                        # Skip empty rows
                        if row.isna().all():
                            continue
                        
                        row_values = [str(cell).strip() for cell in row if pd.notna(cell)]
                        
                        # Look for invoice numbers (numeric values that could be invoice IDs)
                        for cell in row:
                            if pd.notna(cell):
                                cell_str = str(cell).strip()
                                if cell_str.isdigit() and len(cell_str) >= 4:  # Looks like invoice number
                                    current_invoice = cell_str
                                    # Create header
                                    header_data = {
                                        'no_factura_interna': current_invoice,
                                        'fecha': date.today(),  # Default date
                                        'tipo_documento': 'CONTADO',
                                        'cliente': '',
                                        'cedula': '',
                                        'vendedor': '',
                                        'caja': ''
                                    }
                                    headers.append(header_data)
                                    break
                        
                        # Try to parse as detail line (look for quantity and price)
                        if len(row_values) >= 5:
                            cantidad = None
                            precio = None
                            costo = None
                            
                            # Try to find numeric values that could be quantity, cost, price
                            for i, cell in enumerate(row):
                                if pd.notna(cell):
                                    num_val = normalize_number(cell)
                                    if num_val is not None and num_val > 0:
                                        if cantidad is None:
                                            cantidad = num_val
                                        elif costo is None:
                                            costo = num_val
                                        elif precio is None:
                                            precio = num_val
                                            break
                            
                            if cantidad is not None and (costo is not None or precio is not None):
                                descripcion = ""
                                cabys = ""
                                codigo = ""
                                
                                # Try to find text that looks like product description
                                for cell in row:
                                    if pd.notna(cell):
                                        cell_str = str(cell).strip()
                                        if len(cell_str) > 3 and not cell_str.isdigit():
                                            if not descripcion:
                                                descripcion = cell_str
                                            elif len(cell_str) < 20 and not cabys:
                                                cabys = cell_str
                                            elif len(cell_str) < 10 and not codigo:
                                                codigo = cell_str
                                
                                if descripcion:
                                    es_fraccion = is_fraction_product(descripcion)
                                    nombre_clean = clean_product_name(descripcion, remove_frac_prefix=True)
                                    
                                    detail_data = {
                                        'no_factura_interna': current_invoice or f"AUTO_{idx}",
                                        'cabys': cabys,
                                        'codigo': codigo,
                                        'descripcion': descripcion,
                                        'nombre_clean': nombre_clean,
                                        'cantidad': cantidad,
                                        'descuento': 0,
                                        'utilidad': 0,
                                        'costo': costo or precio or 0,
                                        'precio_unit': precio or costo or 0,
                                        'total': cantidad * (precio or costo or 0),
                                        'es_fraccion': 1 if es_fraccion else 0,
                                        'factor_fraccion': 1,
                                        'qty_normalizada': cantidad
                                    }
                                    
                                    # Calculate fraction factor if needed
                                    if es_fraccion and costo and precio:
                                        factor = calculate_fraction_factor(costo, 0, precio)
                                        if factor:
                                            detail_data['factor_fraccion'] = factor
                                            detail_data['qty_normalizada'] = cantidad / factor
                                    
                                    details.append(detail_data)
                    
                    except Exception as e:
                        logger.debug(f"Error parsing row {idx}: {e}")
                        continue
                
                if headers or details:
                    logger.info(f"Found {len(headers)} headers and {len(details)} details in sheet {sheet_name}")
                    return {'headers': headers, 'details': details}
                
            except Exception as e:
                logger.error(f"Error parsing sheet {sheet_name}: {e}")
                continue
        
        # If no data found, return empty
        logger.warning("No parseable data found in any sheet")
        return {'headers': [], 'details': []}
        
    except Exception as e:
        logger.error(f"Error in simple_parse_ventas: {e}")
        return {'headers': [], 'details': []}

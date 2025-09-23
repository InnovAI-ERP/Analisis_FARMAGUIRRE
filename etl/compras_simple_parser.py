"""
Simple and robust parser for Compras (Purchases) Excel files
Focuses on extracting data reliably from the actual file format
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from datetime import date, datetime
from utils.dates_numbers import parse_date, normalize_number, normalize_text, clean_product_name, is_fraction_product

logger = logging.getLogger(__name__)

def simple_parse_compras(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Simple parser for purchases files - more robust approach
    
    Args:
        file_path_or_buffer: File path or buffer containing the Excel file
        
    Returns:
        Dictionary with 'headers' and 'details' lists
    """
    try:
        # Read all sheets and try each one
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Simple compras parser - Found sheets: {excel_file.sheet_names}")
        
        for sheet_name in excel_file.sheet_names:
            try:
                logger.info(f"Simple compras parser - Trying sheet: {sheet_name}")
                df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_name, header=None)
                
                if df.empty:
                    continue
                
                logger.info(f"Simple compras parser - Sheet {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
                
                headers, details = parse_compras_simple_structure(df, sheet_name)
                
                if headers or details:
                    logger.info(f"Simple compras parser - Success: {len(headers)} headers, {len(details)} details")
                    return {'headers': headers, 'details': details}
                    
            except Exception as e:
                logger.error(f"Simple compras parser - Error with sheet {sheet_name}: {e}")
                continue
        
        logger.warning("Simple compras parser - No parseable data found")
        return {'headers': [], 'details': []}
        
    except Exception as e:
        logger.error(f"Simple compras parser - General error: {e}")
        return {'headers': [], 'details': []}

def parse_compras_simple_structure(df: pd.DataFrame, sheet_name: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Parse compras with a simple approach - look for data patterns
    """
    headers = []
    details = []
    
    try:
        # Strategy 1: Look for rows that might contain invoice data
        current_invoice = None
        
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isna().all():
                continue
            
            # Look for potential invoice header data
            # Check if row contains date-like values and invoice numbers
            row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            
            # Check if this might be an invoice header
            if any(keyword in row_str for keyword in ['fecha', 'consecutivo', 'factura', 'proveedor']):
                # Try to extract header information
                invoice_data = extract_invoice_header_simple(row, idx)
                if invoice_data:
                    current_invoice = invoice_data
                    headers.append(invoice_data)
                    logger.info(f"Found invoice header at row {idx}: {invoice_data.get('no_consecutivo', 'Unknown')}")
                continue
            
            # Check if this might be a product detail line
            if current_invoice:
                detail_data = extract_product_detail_simple(row, idx, current_invoice)
                if detail_data:
                    details.append(detail_data)
        
        # If no structured approach worked, try to find any tabular data
        if not details:
            logger.info("No structured data found, trying tabular approach")
            headers, details = parse_as_table(df)
        
        # If still no data, try aggressive extraction
        if not details:
            logger.info("No tabular data found, trying aggressive extraction")
            headers, details = aggressive_extract(df)
        
        logger.info(f"Simple parser extracted {len(headers)} headers and {len(details)} details")
        return headers, details
        
    except Exception as e:
        logger.error(f"Error in simple compras parsing: {e}")
        return [], []

def extract_invoice_header_simple(row: pd.Series, row_idx: int) -> Optional[Dict]:
    """
    Try to extract invoice header information from a row
    """
    try:
        # Look for date, consecutive number, invoice number, etc.
        fecha = None
        no_consecutivo = ""
        no_factura = ""
        proveedor = ""
        
        for i, cell in enumerate(row):
            if pd.notna(cell):
                cell_str = str(cell).strip()
                
                # Try to parse as date
                if not fecha:
                    parsed_date = parse_date(cell, dayfirst=True)
                    if parsed_date:
                        fecha = parsed_date.date() if hasattr(parsed_date, 'date') else parsed_date
                
                # Look for numbers that could be invoice numbers
                if cell_str.isdigit() and len(cell_str) >= 4:
                    if not no_consecutivo:
                        no_consecutivo = cell_str
                    elif not no_factura:
                        no_factura = cell_str
                
                # Look for text that could be supplier name
                if len(cell_str) > 10 and not cell_str.isdigit():
                    if not proveedor:
                        proveedor = cell_str
        
        # Create header if we have minimum required data
        if fecha or no_consecutivo:
            return {
                'fecha': fecha or date.today(),
                'no_consecutivo': no_consecutivo or f"AUTO_{row_idx}",
                'no_factura': no_factura,
                'no_guia': "",
                'ced_juridica': "",
                'proveedor': proveedor
            }
        
        return None
        
    except Exception as e:
        logger.debug(f"Error extracting header from row {row_idx}: {e}")
        return None

def extract_product_detail_simple(row: pd.Series, row_idx: int, invoice_data: Dict) -> Optional[Dict]:
    """
    Try to extract product detail from a row
    """
    try:
        # Look for product information
        cabys = ""
        codigo = ""
        nombre = ""
        cantidad = None
        costo = None
        precio_unit = None
        
        # Scan row for different types of data
        for i, cell in enumerate(row):
            if pd.notna(cell):
                cell_str = str(cell).strip()
                
                # Try to identify product description (longer text)
                if len(cell_str) > 5 and not cell_str.replace('.', '').replace(',', '').isdigit():
                    if not nombre or len(cell_str) > len(nombre):
                        nombre = cell_str
                
                # Try to identify CABYS or codes (shorter alphanumeric)
                elif len(cell_str) <= 15 and not cabys:
                    if cell_str.isdigit() or any(c.isalpha() for c in cell_str):
                        cabys = cell_str
                
                # Try to identify numeric values
                num_val = normalize_number(cell)
                if num_val is not None and num_val > 0:
                    if cantidad is None and num_val < 10000:  # Likely quantity
                        cantidad = num_val
                    elif costo is None:  # Likely cost
                        costo = num_val
                    elif precio_unit is None:  # Likely price
                        precio_unit = num_val
        
        # Validate we have minimum required data
        if nombre and cantidad is not None:
            es_fraccion = is_fraction_product(nombre)
            nombre_clean = clean_product_name(nombre, remove_frac_prefix=False)
            
            detail_data = {
                'no_consecutivo': invoice_data['no_consecutivo'],
                'cabys': cabys,
                'codigo': codigo,
                'nombre': nombre,
                'nombre_clean': nombre_clean,
                'variacion': "",
                'codigo_referencia': "",
                'codigo_color': "",
                'color': "",
                'cantidad': cantidad,
                'descuento': 0,
                'utilidad': 0,
                'costo': costo or precio_unit or 0,  # Use costo if available, fallback to precio_unit
                'precio_unit': precio_unit or costo or 0,
                
                # Populated header data for normalization
                'fecha_compra': invoice_data['fecha'],
                'no_factura': invoice_data['no_factura'],
                'no_guia': invoice_data['no_guia'],
                'ced_juridica': invoice_data['ced_juridica'],
                'proveedor': invoice_data['proveedor'],
                
                # Normalization fields
                'es_fraccion': 1 if es_fraccion else 0,
                'factor_fraccion': 1.0,
                'qty_normalizada': cantidad
            }
            
            return detail_data
        
        return None
        
    except Exception as e:
        logger.debug(f"Error extracting product from row {row_idx}: {e}")
        return None

def parse_as_table(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """
    Parse data as a continuous table
    """
    headers = []
    details = []
    
    try:
        # Generate a single header for all data
        header_data = {
            'fecha': date.today(),
            'no_consecutivo': 'TABLE_DATA',
            'no_factura': '',
            'no_guia': '',
            'ced_juridica': '',
            'proveedor': 'IMPORTED_DATA'
        }
        headers.append(header_data)
        
        # Try to extract all product lines
        for idx, row in df.iterrows():
            detail_data = extract_product_detail_simple(row, idx, header_data)
            if detail_data:
                details.append(detail_data)
        
        logger.info(f"Table parsing found {len(details)} detail lines")
        return headers, details
        
    except Exception as e:
        logger.error(f"Error in table parsing: {e}")
        return [], []

def aggressive_extract(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """
    Aggressive extraction - try to find ANY data that looks like products
    """
    headers = []
    details = []
    
    try:
        # Generate a single header for all data
        header_data = {
            'fecha': date.today(),
            'no_consecutivo': 'AGGRESSIVE_EXTRACT',
            'no_factura': '',
            'no_guia': '',
            'ced_juridica': '',
            'proveedor': 'EXTRACTED_DATA'
        }
        headers.append(header_data)
        
        # Look for ANY row that might contain product data
        for idx, row in df.iterrows():
            # Skip completely empty rows
            if row.isna().all():
                continue
            
            # Look for rows with at least some text and some numbers
            text_cells = 0
            number_cells = 0
            longest_text = ""
            
            for cell in row:
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    if cell_str:
                        # Check if it's a number
                        num_val = normalize_number(cell)
                        if num_val is not None:
                            number_cells += 1
                        else:
                            text_cells += 1
                            if len(cell_str) > len(longest_text):
                                longest_text = cell_str
            
            # If we have both text and numbers, this might be a product row
            if text_cells >= 1 and number_cells >= 1 and len(longest_text) > 3:
                detail_data = {
                    'no_consecutivo': header_data['no_consecutivo'],
                    'cabys': '',
                    'codigo': '',
                    'nombre': longest_text,
                    'nombre_clean': clean_product_name(longest_text, remove_frac_prefix=False),
                    'variacion': '',
                    'codigo_referencia': '',
                    'codigo_color': '',
                    'color': '',
                    'cantidad': 1,  # Default quantity
                    'descuento': 0,
                    'utilidad': 0,
                    'costo': 0,  # Cost per unit
                    'precio_unit': 0,
                    
                    # Populated header data for normalization
                    'fecha_compra': header_data['fecha'],
                    'no_factura': header_data['no_factura'],
                    'no_guia': header_data['no_guia'],
                    'ced_juridica': header_data['ced_juridica'],
                    'proveedor': header_data['proveedor'],
                    
                    # Normalization fields
                    'es_fraccion': 1 if is_fraction_product(longest_text) else 0,
                    'factor_fraccion': 1.0,
                    'qty_normalizada': 1
                }
                
                details.append(detail_data)
        
        logger.info(f"Aggressive extraction found {len(details)} potential product lines")
        return headers, details
        
    except Exception as e:
        logger.error(f"Error in aggressive extraction: {e}")
        return [], []

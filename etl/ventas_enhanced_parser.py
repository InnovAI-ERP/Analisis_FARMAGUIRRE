"""
Enhanced parser specifically designed for Farmaguirre sales Excel format
Based on the detailed structure description provided
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
import re
from datetime import datetime, date
from utils.dates_numbers import (
    parse_date, normalize_number, normalize_text, clean_product_name,
    is_fraction_product, calculate_fraction_factor
)

logger = logging.getLogger(__name__)

def enhanced_parse_ventas(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Enhanced parser specifically for Farmaguirre sales format
    
    Expected structure:
    - Fila con títulos de bloque: "No. Factura Interna" / "Tipo Documento"
    - Fila siguiente con No. Factura Interna (ej. 131270)
    - Fila "PRODUCTOS"
    - Encabezado de detalle: Código (col 1), CABYS (col 2), Descripción (col 3), 
      Cantidad (col 5), Descuento (col 6), Utilidad (col 7), Costo (col 8), 
      Precio Unit. (col 9), Total (col 10)
    - N filas de detalle por factura
    """
    
    try:
        # Read all sheets and try each one
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Enhanced parser - Found sheets: {excel_file.sheet_names}")
        
        for sheet_name in excel_file.sheet_names:
            try:
                logger.info(f"Enhanced parser - Trying sheet: {sheet_name}")
                df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_name, header=None)
                
                if df.empty:
                    continue
                
                logger.info(f"Enhanced parser - Sheet {sheet_name}: {len(df)} rows, {len(df.columns)} columns")
                
                headers, details = parse_enhanced_structure(df, sheet_name)
                
                if headers or details:
                    logger.info(f"Enhanced parser - Success: {len(headers)} headers, {len(details)} details")
                    return {'headers': headers, 'details': details}
                    
            except Exception as e:
                logger.error(f"Enhanced parser - Error with sheet {sheet_name}: {e}")
                continue
        
        logger.warning("Enhanced parser - No parseable data found")
        return {'headers': [], 'details': []}
        
    except Exception as e:
        logger.error(f"Enhanced parser - General error: {e}")
        return {'headers': [], 'details': []}

def parse_enhanced_structure(df: pd.DataFrame, sheet_name: str) -> Tuple[List[Dict], List[Dict]]:
    """
    Parse the enhanced structure looking for specific patterns
    """
    headers = []
    details = []
    
    # Step 1: Find invoice blocks by looking for invoice numbers
    invoice_blocks = find_invoice_blocks_enhanced(df)
    logger.info(f"Enhanced parser - Found {len(invoice_blocks)} invoice blocks")
    
    # Step 2: Process each block
    for i, block_info in enumerate(invoice_blocks):
        try:
            invoice_number = block_info['invoice_number']
            start_row = block_info['start_row']
            end_row = block_info.get('end_row', len(df))
            
            logger.info(f"Enhanced parser - Processing invoice {invoice_number} (rows {start_row}-{end_row})")
            
            # Extract date for this invoice
            fecha = extract_date_enhanced(df, start_row, end_row)
            
            # Find products section
            products_row = find_products_section_enhanced(df, start_row, end_row)
            
            # Find detail header and extract details
            detail_lines = extract_details_enhanced(df, products_row, end_row, invoice_number)
            
            if detail_lines:
                # Add invoice number and populate header data in each detail line
                for detail in detail_lines:
                    detail['no_factura_interna'] = invoice_number
                    # Populate header data for normalization
                    detail['fecha_venta'] = fecha
                    detail['tipo_documento'] = 'CONTADO'
                    detail['cliente'] = ''
                    detail['cedula'] = ''
                    detail['vendedor'] = ''
                    detail['caja'] = ''
                
                # Create header
                header_data = {
                    'no_factura_interna': invoice_number,
                    'fecha': fecha,
                    'tipo_documento': 'CONTADO',
                    'cliente': '',
                    'cedula': '',
                    'vendedor': '',
                    'caja': ''
                }
                headers.append(header_data)
                details.extend(detail_lines)
                
                logger.info(f"Enhanced parser - Processed invoice {invoice_number}: {len(detail_lines)} details")
            
        except Exception as e:
            logger.error(f"Enhanced parser - Error processing block {i}: {e}")
            continue
    
    return headers, details

def find_invoice_blocks_enhanced(df: pd.DataFrame) -> List[Dict]:
    """
    Find invoice blocks by looking for invoice numbers in column 1 (index 0)
    """
    blocks = []
    
    try:
        for idx, row in df.iterrows():
            # Look specifically in column 1 (index 0) for invoice numbers
            if len(row) > 0 and pd.notna(row.iloc[0]):
                cell_str = str(row.iloc[0]).strip()
                
                # Check if it looks like an invoice number (6 digits exactly, in column 1)
                if cell_str.isdigit() and len(cell_str) == 6:
                    # Additional validation: check if next few rows contain "PRODUCTOS" and column headers
                    if is_likely_invoice_block(df, idx):
                        blocks.append({
                            'invoice_number': cell_str,
                            'start_row': idx,
                            'row_idx': idx,
                            'col_idx': 0
                        })
                        logger.info(f"Enhanced parser - Found invoice {cell_str} at row {idx+1}")
        
        # Set end_row for each block
        for i, block in enumerate(blocks):
            if i + 1 < len(blocks):
                block['end_row'] = blocks[i + 1]['start_row']
            else:
                block['end_row'] = len(df)
        
        logger.info(f"Enhanced parser - Found {len(blocks)} invoice blocks total")
        return blocks
        
    except Exception as e:
        logger.error(f"Enhanced parser - Error finding invoice blocks: {e}")
        return []

def is_likely_invoice_block(df: pd.DataFrame, row_idx: int) -> bool:
    """
    Check if a row with an invoice number is likely the start of an invoice block
    by looking for "PRODUCTOS" and column headers in the next few rows
    """
    try:
        # Check the next 5 rows for the expected pattern
        search_end = min(len(df), row_idx + 5)
        
        has_productos = False
        has_headers = False
        
        for check_idx in range(row_idx + 1, search_end):
            if check_idx >= len(df):
                break
                
            row = df.iloc[check_idx]
            row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            
            # Look for "PRODUCTOS"
            if 'productos' in row_str:
                has_productos = True
            
            # Look for column headers
            if any(keyword in row_str for keyword in ['código', 'cabys', 'descripción', 'cantidad', 'costo', 'precio']):
                has_headers = True
            
            # If we found both, this is likely an invoice block
            if has_productos and has_headers:
                return True
        
        # If we found at least PRODUCTOS, it's probably an invoice
        return has_productos
        
    except Exception as e:
        logger.debug(f"Enhanced parser - Error checking invoice block: {e}")
        return False

def extract_date_enhanced(df: pd.DataFrame, start_row: int, end_row: int) -> date:
    """
    Extract date from invoice block - look for the actual invoice date in column 18 (index 17)
    """
    try:
        # The date should be in the invoice header row itself (start_row)
        invoice_row = df.iloc[start_row]
        
        # First, check column 18 (index 17) where invoice dates are typically located
        if len(invoice_row) > 17:
            date_cell = invoice_row.iloc[17]
            if pd.notna(date_cell):
                parsed_date = parse_date(date_cell, dayfirst=False)
                if parsed_date and 2020 <= parsed_date.year <= 2030:
                    logger.info(f"Enhanced parser - Found invoice date {parsed_date} in column 18")
                    # Ensure we return a date object, not datetime
                    if hasattr(parsed_date, 'date'):
                        return parsed_date.date()
                    return parsed_date
        
        # If not found in column 18, check other columns in the invoice header row
        for col_idx in range(len(invoice_row)):
            cell = invoice_row.iloc[col_idx]
            if pd.notna(cell):
                parsed_date = parse_date(cell, dayfirst=False)
                if parsed_date and 2020 <= parsed_date.year <= 2030:
                    logger.info(f"Enhanced parser - Found date {parsed_date} in column {col_idx+1}")
                    # Ensure we return a date object, not datetime
                    if hasattr(parsed_date, 'date'):
                        return parsed_date.date()
                    return parsed_date
        
        # If not found in invoice row, search nearby rows
        search_start = max(0, start_row - 2)
        search_end = min(len(df), start_row + 3)
        
        for idx in range(search_start, search_end):
            row = df.iloc[idx]
            
            # Check all cells for date values
            for col_idx, cell in enumerate(row):
                if pd.notna(cell):
                    parsed_date = parse_date(cell, dayfirst=False)
                    if parsed_date and 2020 <= parsed_date.year <= 2030:
                        logger.info(f"Enhanced parser - Found date {parsed_date} at row {idx+1}, col {col_idx+1}")
                        # Ensure we return a date object, not datetime
                        if hasattr(parsed_date, 'date'):
                            return parsed_date.date()
                        return parsed_date
        
        # Fallback: use a date within the expected range instead of today's date
        logger.warning(f"Enhanced parser - No date found for invoice, using fallback date 2025-07-01")
        return date(2025, 7, 1)  # Use start of report period instead of today
        
    except Exception as e:
        logger.error(f"Enhanced parser - Error extracting date: {e}")
        return date(2025, 7, 1)  # Use start of report period instead of today

def find_products_section_enhanced(df: pd.DataFrame, start_row: int, end_row: int) -> Optional[int]:
    """
    Find the "PRODUCTOS" section
    """
    try:
        for idx in range(start_row, min(end_row, len(df))):
            row = df.iloc[idx]
            row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            
            if 'productos' in row_str:
                logger.info(f"Enhanced parser - Found PRODUCTOS section at row {idx}")
                return idx
        
        # If no PRODUCTOS found, return a reasonable starting point
        return start_row + 2
        
    except Exception as e:
        logger.error(f"Enhanced parser - Error finding products section: {e}")
        return start_row + 2

def extract_details_enhanced(df: pd.DataFrame, products_row: Optional[int], end_row: int, invoice_number: str) -> List[Dict]:
    """
    Extract detail lines using the specific column structure
    
    Expected pattern per invoice:
    - Invoice number row
    - "PRODUCTOS" row  
    - Column headers row
    - ONE detail row per invoice
    - Empty row or next invoice
    """
    details = []
    
    try:
        # Start searching from products row + 1 (the row right after "PRODUCTOS")
        start_search = (products_row + 1) if products_row else 0
        
        # Find the detail header first
        header_row = None
        search_end = min(start_search + 5, end_row, len(df))
        
        for idx in range(start_search, search_end):
            row = df.iloc[idx]
            row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
            
            # Look for column headers
            if any(keyword in row_str for keyword in ['código', 'cabys', 'descripción', 'cantidad']):
                header_row = idx
                logger.info(f"Enhanced parser - Found detail header at row {idx+1}")
                break
        
        # Extract ALL detail rows after header (each invoice can have multiple products)
        if header_row is not None:
            detail_start_idx = header_row + 1
            
            # Extract all product rows until we hit an empty row or next invoice
            for detail_row_idx in range(detail_start_idx, min(end_row, len(df))):
                row = df.iloc[detail_row_idx]
                
                # Stop if we hit an empty row (end of this invoice's products)
                if row.isna().all():
                    logger.info(f"Enhanced parser - Hit empty row at {detail_row_idx+1}, stopping product extraction")
                    break
                
                # Stop if we hit the next invoice (6-digit number in column 1)
                if len(row) > 0 and pd.notna(row.iloc[0]):
                    cell_str = str(row.iloc[0]).strip()
                    if cell_str.isdigit() and len(cell_str) == 6:
                        logger.info(f"Enhanced parser - Hit next invoice {cell_str} at row {detail_row_idx+1}, stopping")
                        break
                
                # Check if this row has product data
                # Look for product description in column 4 (index 3)
                if len(row) > 3 and pd.notna(row.iloc[3]):
                    descripcion = str(row.iloc[3]).strip()
                    # If it contains product-like text, process it
                    if descripcion and not descripcion.isdigit() and len(descripcion) > 3:
                        # Extract detail using expected column positions
                        detail_data = extract_detail_from_row_enhanced(row, detail_row_idx, invoice_number)
                        if detail_data:
                            details.append(detail_data)
                            logger.info(f"Enhanced parser - Extracted detail for invoice {invoice_number}: {detail_data.get('descripcion', 'N/A')}")
                        else:
                            logger.debug(f"Enhanced parser - Failed to extract detail data from row {detail_row_idx+1}")
                    else:
                        logger.debug(f"Enhanced parser - Skipping row {detail_row_idx+1} - invalid descripcion: '{descripcion}'")
        
        logger.info(f"Enhanced parser - Extracted {len(details)} detail lines for invoice {invoice_number}")
        return details
        
    except Exception as e:
        logger.error(f"Enhanced parser - Error extracting details: {e}")
        return []

def extract_detail_from_row_enhanced(row: pd.Series, row_idx: int, invoice_number: str) -> Optional[Dict]:
    """
    Extract detail from a single row using expected column positions
    """
    try:
        # Map expected columns (1-indexed to 0-indexed) - CORRECTED based on real file structure
        col_map = {
            'codigo': 1,      # Col 2 (Código)
            'cabys': 2,       # Col 3 (CABYS)
            'descripcion': 3, # Col 4 (Descripción)
            'color': 4,       # Col 5 (Color)
            'cantidad': 5,    # Col 6 (Cantidad)
            'descuento': 6,   # Col 7 (Descuento)
            'utilidad': 7,    # Col 8 (Utilidad)
            'costo': 8,       # Col 9 (Costo) - CORRECTED
            'precio_unit': 9, # Col 10 (Precio Unit.) - CORRECTED
            'total': 10       # Col 11 (Total) - CORRECTED
        }
        
        # Extract values
        codigo = normalize_text(row.iloc[col_map['codigo']] if len(row) > col_map['codigo'] else "")
        cabys = normalize_text(row.iloc[col_map['cabys']] if len(row) > col_map['cabys'] else "")
        descripcion = normalize_text(row.iloc[col_map['descripcion']] if len(row) > col_map['descripcion'] else "")
        color = normalize_text(row.iloc[col_map['color']] if len(row) > col_map['color'] else "")
        
        # Extract numeric values
        cantidad = normalize_number(row.iloc[col_map['cantidad']] if len(row) > col_map['cantidad'] else None)
        descuento = normalize_number(row.iloc[col_map['descuento']] if len(row) > col_map['descuento'] else 0)
        utilidad = normalize_number(row.iloc[col_map['utilidad']] if len(row) > col_map['utilidad'] else 0)
        costo = normalize_number(row.iloc[col_map['costo']] if len(row) > col_map['costo'] else 0)
        precio_unit = normalize_number(row.iloc[col_map['precio_unit']] if len(row) > col_map['precio_unit'] else 0)
        total = normalize_number(row.iloc[col_map['total']] if len(row) > col_map['total'] else 0)
        
        # Validate minimum required data
        if not descripcion or cantidad is None or cantidad <= 0:
            return None
        
        # Process fraction information
        es_fraccion = is_fraction_product(descripcion)
        nombre_clean = clean_product_name(descripcion, remove_frac_prefix=True)
        
        # Calculate fraction factor and normalized quantity
        factor_fraccion = 1
        qty_normalizada = cantidad
        
        if es_fraccion and costo and precio_unit:
            factor = calculate_fraction_factor(costo, utilidad or 0, precio_unit)
            if factor and factor > 0:
                factor_fraccion = factor
                qty_normalizada = cantidad / factor
        
        detail_data = {
            'no_factura_interna': invoice_number,
            'cabys': cabys,
            'codigo': codigo,
            'descripcion': descripcion,
            'nombre_clean': nombre_clean,
            'color': color,
            'cantidad': cantidad,
            'descuento': descuento or 0,
            'utilidad': utilidad or 0,
            'costo': costo or 0,
            'precio_unit': precio_unit or 0,
            'total': total or (cantidad * precio_unit if precio_unit else 0),
            'es_fraccion': 1 if es_fraccion else 0,
            'factor_fraccion': factor_fraccion,
            'qty_normalizada': qty_normalizada
        }
        
        return detail_data
        
    except Exception as e:
        logger.debug(f"Enhanced parser - Error extracting detail from row {row_idx}: {e}")
        return None

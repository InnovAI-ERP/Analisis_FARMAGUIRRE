"""
Enhanced parser for Compras (Purchases) Excel files
Specifically designed for the actual file structure with proper date extraction
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from datetime import date, datetime
from utils.dates_numbers import parse_date, normalize_number, normalize_text, clean_product_name, is_fraction_product

logger = logging.getLogger(__name__)

def enhanced_parse_compras(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Enhanced parser for purchases files - recognizes the actual file structure
    
    Args:
        file_path_or_buffer: File path or buffer containing the Excel file
        
    Returns:
        Dictionary with 'headers' and 'details' lists
    """
    try:
        # Read the Excel file
        df = pd.read_excel(file_path_or_buffer, header=None)
        logger.info(f"Enhanced compras parser - Loaded {len(df)} rows, {len(df.columns)} columns")
        
        if df.empty:
            return {'headers': [], 'details': []}
        
        headers, details = parse_compras_enhanced_structure(df)
        
        logger.info(f"Enhanced compras parser - Success: {len(headers)} headers, {len(details)} details")
        return {'headers': headers, 'details': details}
        
    except Exception as e:
        logger.error(f"Enhanced compras parser - Error: {e}")
        return {'headers': [], 'details': []}

def parse_compras_enhanced_structure(df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
    """
    Parse compras with enhanced structure recognition
    
    The file structure is:
    - Product rows with CABYS, code, name, quantity, cost, etc. (at the beginning)
    - Invoice header rows with date, consecutive number, invoice number, etc. (scattered throughout)
    - Column header rows with field names
    
    Strategy: First collect all products, then match them to invoices based on proximity
    """
    headers = []
    details = []
    
    try:
        # First pass: collect all invoice headers with their row positions
        invoice_headers = []
        
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isna().all():
                continue
            
            # Check if this is an invoice header row
            invoice_data = extract_invoice_header_enhanced(row, idx)
            if invoice_data:
                invoice_data['row_idx'] = idx
                invoice_headers.append(invoice_data)
                headers.append(invoice_data)
                logger.info(f"Enhanced parser - Found invoice header at row {idx+1}: {invoice_data.get('no_consecutivo', 'Unknown')} on {invoice_data.get('fecha', 'Unknown')}")
        
        # Second pass: collect all product rows and assign them to the nearest invoice
        current_invoice = None
        current_invoice_idx = 0
        
        for idx, row in df.iterrows():
            # Skip empty rows
            if row.isna().all():
                continue
            
            # Skip if this is an invoice header row
            if any(h['row_idx'] == idx for h in invoice_headers):
                # Update current invoice context
                current_invoice = next(h for h in invoice_headers if h['row_idx'] == idx)
                current_invoice_idx = invoice_headers.index(current_invoice)
                continue
            
            # Skip column header rows
            if is_column_header_row(row):
                logger.debug(f"Enhanced parser - Skipping column header row at {idx+1}")
                continue
            
            # Try to extract product data
            detail_data = extract_product_detail_enhanced(row, idx, None)  # Don't pass invoice yet
            if detail_data:
                # Find the best invoice for this product based on row position
                best_invoice = find_best_invoice_for_product(idx, invoice_headers)
                if best_invoice:
                    # Add invoice data to the product
                    detail_data.update({
                        'fecha_compra': best_invoice['fecha'],
                        'no_consecutivo': best_invoice['no_consecutivo'],
                        'no_factura': best_invoice['no_factura'],
                        'no_guia': best_invoice['no_guia'],
                        'ced_juridica': best_invoice['ced_juridica'],
                        'proveedor': best_invoice['proveedor']
                    })
                    
                    details.append(detail_data)
                    logger.debug(f"Enhanced parser - Extracted product: {detail_data.get('nombre', 'Unknown')} -> Invoice {best_invoice['no_consecutivo']}")
        
        logger.info(f"Enhanced parser extracted {len(headers)} headers and {len(details)} details")
        return headers, details
        
    except Exception as e:
        logger.error(f"Error in enhanced compras parsing: {e}")
        return [], []

def find_best_invoice_for_product(product_row: int, invoice_headers: List[Dict]) -> Optional[Dict]:
    """
    Find the best invoice for a product based on row proximity
    """
    if not invoice_headers:
        return None
    
    # Find the invoice header that comes before this product row and is closest
    best_invoice = None
    min_distance = float('inf')
    
    for invoice in invoice_headers:
        invoice_row = invoice['row_idx']
        
        # We want invoices that come before or around the product row
        distance = abs(product_row - invoice_row)
        
        if distance < min_distance:
            min_distance = distance
            best_invoice = invoice
    
    return best_invoice

def extract_invoice_header_enhanced(row: pd.Series, row_idx: int) -> Optional[Dict]:
    """
    Extract invoice header information from a row
    
    Based on the file structure, invoice headers have:
    - Column 0: Date (e.g., "01-07-2025")
    - Column 1: Consecutive number (e.g., "1725")
    - Column 2: Invoice number (e.g., "432945")
    - Column 4: Provider ID (e.g., "3101353234")
    - Column 5: Provider name (e.g., "FACEME")
    """
    try:
        # Check if this looks like an invoice header
        # Look for date pattern in column 0
        if len(row) > 0 and pd.notna(row.iloc[0]):
            cell_0 = str(row.iloc[0]).strip()
            
            # Check if column 0 contains a date
            parsed_date = parse_date(cell_0, dayfirst=True)
            if parsed_date and 2020 <= parsed_date.year <= 2030:
                # This looks like an invoice header row
                
                # Extract consecutive number from column 1
                no_consecutivo = ""
                if len(row) > 1 and pd.notna(row.iloc[1]):
                    no_consecutivo = str(row.iloc[1]).strip()
                
                # Extract invoice number from column 2
                no_factura = ""
                if len(row) > 2 and pd.notna(row.iloc[2]):
                    no_factura = str(row.iloc[2]).strip()
                
                # Extract provider info from columns 4-5
                ced_juridica = ""
                proveedor = ""
                if len(row) > 4 and pd.notna(row.iloc[4]):
                    ced_juridica = str(row.iloc[4]).strip()
                if len(row) > 5 and pd.notna(row.iloc[5]):
                    proveedor = str(row.iloc[5]).strip()
                
                logger.info(f"Enhanced parser - Found invoice header: date={parsed_date}, consecutive={no_consecutivo}, invoice={no_factura}")
                
                return {
                    'fecha': parsed_date.date() if hasattr(parsed_date, 'date') else parsed_date,
                    'no_consecutivo': no_consecutivo or f"AUTO_{row_idx}",
                    'no_factura': no_factura,
                    'no_guia': "",
                    'ced_juridica': ced_juridica,
                    'proveedor': proveedor
                }
        
        return None
        
    except Exception as e:
        logger.debug(f"Error extracting header from row {row_idx}: {e}")
        return None

def is_column_header_row(row: pd.Series) -> bool:
    """
    Check if this row contains column headers
    """
    try:
        row_str = ' '.join([str(cell).lower() for cell in row if pd.notna(cell)])
        
        # Look for common column header keywords
        header_keywords = ['cabys', 'código', 'nombre', 'cantidad', 'costo', 'precio', 'variación']
        
        # If the row contains multiple header keywords, it's probably a header row
        keyword_count = sum(1 for keyword in header_keywords if keyword in row_str)
        
        return keyword_count >= 3
        
    except Exception:
        return False

def extract_product_detail_enhanced(row: pd.Series, row_idx: int, invoice_data: Optional[Dict] = None) -> Optional[Dict]:
    """
    Extract product detail from a row
    
    Based on the file structure, product rows have:
    - Column 0: CABYS code
    - Column 1: Product code (optional)
    - Column 4: Product name
    - Column 7: Quantity
    - Column 10: Cost
    - Column 12: Profit margin
    - Column 13: Unit price
    - Column 14: Total
    """
    try:
        # Check if this looks like a product row
        # Must have CABYS code in column 0 and product name in column 4
        if len(row) <= 4:
            return None
        
        # Extract CABYS code from column 0
        cabys = ""
        if pd.notna(row.iloc[0]):
            cabys = str(row.iloc[0]).strip()
            # CABYS codes are typically numeric
            if not cabys.replace('.', '').isdigit():
                return None
        
        # Extract product name from column 4
        nombre = ""
        if pd.notna(row.iloc[4]):
            nombre = str(row.iloc[4]).strip()
            if len(nombre) < 3:  # Product names should be reasonably long
                return None
        
        if not cabys or not nombre:
            return None
        
        # Extract other fields
        codigo = ""
        if len(row) > 1 and pd.notna(row.iloc[1]):
            codigo = str(row.iloc[1]).strip()
        
        # Extract quantity from column 7
        cantidad = 1.0
        if len(row) > 7 and pd.notna(row.iloc[7]):
            try:
                cantidad = normalize_number(row.iloc[7]) or 1.0
            except:
                cantidad = 1.0
        
        # Extract cost from column 10
        costo = 0.0
        if len(row) > 10 and pd.notna(row.iloc[10]):
            try:
                costo = normalize_number(row.iloc[10]) or 0.0
            except:
                costo = 0.0
        
        # Extract profit margin from column 12
        utilidad = 0.0
        if len(row) > 12 and pd.notna(row.iloc[12]):
            try:
                utilidad = normalize_number(row.iloc[12]) or 0.0
            except:
                utilidad = 0.0
        
        # Extract unit price from column 13
        precio_unit = 0.0
        if len(row) > 13 and pd.notna(row.iloc[13]):
            try:
                precio_unit = normalize_number(row.iloc[13]) or 0.0
            except:
                precio_unit = 0.0
        
        # Extract total from column 14
        total = 0.0
        if len(row) > 14 and pd.notna(row.iloc[14]):
            try:
                total = normalize_number(row.iloc[14]) or 0.0
            except:
                total = 0.0
        
        # Clean and normalize product name
        nombre_clean = clean_product_name(nombre)
        es_fraccion = is_fraction_product(nombre)
        
        logger.debug(f"Enhanced parser - Extracted product: {nombre_clean}, qty: {cantidad}, cost: {costo}")
        
        detail_data = {
            'cabys': cabys,
            'codigo': codigo,
            'variacion': "",
            'codigo_referencia': "",
            'nombre': nombre,
            'nombre_clean': nombre_clean,
            'codigo_color': "",
            'color': "",
            'cantidad': cantidad,
            'regalia': 0.0,
            'aplica_impuesto': 'SI',
            'costo': costo,
            'descuento': 0.0,
            'utilidad': utilidad,
            'precio': precio_unit,
            'precio_unit': precio_unit,
            'total': total,
            
            # Invoice data (will be filled later if not provided)
            'fecha_compra': invoice_data['fecha'] if invoice_data else None,
            'no_consecutivo': invoice_data['no_consecutivo'] if invoice_data else '',
            'no_factura': invoice_data['no_factura'] if invoice_data else '',
            'no_guia': invoice_data['no_guia'] if invoice_data else '',
            'ced_juridica': invoice_data['ced_juridica'] if invoice_data else '',
            'proveedor': invoice_data['proveedor'] if invoice_data else '',
            
            # Normalization fields
            'es_fraccion': 1 if es_fraccion else 0,
            'factor_fraccion': 1.0,
            'qty_normalizada': cantidad
        }
        
        return detail_data
        
    except Exception as e:
        logger.debug(f"Error extracting product from row {row_idx}: {e}")
        return None

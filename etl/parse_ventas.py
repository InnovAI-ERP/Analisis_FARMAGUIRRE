"""
Parser for Ventas (Sales) Excel files
Handles block-based structure with invoice headers, date extraction, and fraction handling
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

class VentasParser:
    """Parser for sales files with block detection and fraction handling"""
    
    def __init__(self):
        # Patterns to detect invoice blocks
        self.invoice_patterns = [
            'no. factura interna', 'no factura interna', 'factura interna',
            'tipo documento'
        ]
        
        # Pattern to detect products section
        self.products_pattern = 'productos'
        
        # Expected detail header columns (by position) - Based on actual analysis
        # Structure: [nan, 'Código', 'CABYS', 'Descripción', 'Color', 'Cantidad', 'Descuento', 'Utilidad', 'Costo', 'Precio Unit.', 'Total']
        self.detail_columns = {
            1: 'codigo',      # Column 1: Código
            2: 'cabys',       # Column 2: CABYS  
            3: 'descripcion', # Column 3: Descripción
            4: 'color',       # Column 4: Color
            5: 'cantidad',    # Column 5: Cantidad
            6: 'descuento',   # Column 6: Descuento
            7: 'utilidad',    # Column 7: Utilidad
            8: 'costo',       # Column 8: Costo
            9: 'precio_unit', # Column 9: Precio Unit.
            10: 'total'       # Column 10: Total
        }
    
    def detect_invoice_block(self, row: pd.Series) -> bool:
        """
        Detect if a row contains invoice block start
        
        Args:
            row: Pandas Series representing a row
            
        Returns:
            True if row appears to start an invoice block
        """
        try:
            # Convert row to string and normalize
            row_str = ' '.join([str(cell).lower().strip() for cell in row if pd.notna(cell) and str(cell).strip()])
            
            if not row_str:
                return False
            
            # Check if it contains invoice patterns
            has_pattern = any(pattern in row_str for pattern in self.invoice_patterns)
            
            # Also check for numeric invoice numbers in specific positions
            for i, cell in enumerate(row):
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    # Look for invoice numbers (6+ digits)
                    if cell_str.isdigit() and len(cell_str) >= 6:
                        return True
            
            return has_pattern
        except Exception as e:
            logger.error(f"Error in detect_invoice_block: {e}")
            return False
    
    def detect_products_section(self, row: pd.Series) -> bool:
        """
        Detect if a row contains "PRODUCTOS" marker
        
        Args:
            row: Pandas Series representing a row
            
        Returns:
            True if row contains products marker
        """
        row_str = ' '.join([str(cell).lower().strip() for cell in row if pd.notna(cell)])
        return self.products_pattern in row_str
    
    def extract_invoice_number(self, df: pd.DataFrame, block_start_idx: int) -> Optional[str]:
        """
        Extract invoice number from the block
        
        Args:
            df: DataFrame containing the data
            block_start_idx: Index where the invoice block starts
            
        Returns:
            Invoice number or None if not found
        """
        try:
            # Look in the current row and next few rows for a numeric value
            for i in range(block_start_idx, min(block_start_idx + 8, len(df))):
                row = df.iloc[i]
                for j, cell in enumerate(row):
                    if pd.notna(cell):
                        cell_str = str(cell).strip()
                        # Check if it's a numeric invoice number (at least 4 digits)
                        if re.match(r'^\d{4,}$', cell_str):
                            logger.info(f"Found invoice number: {cell_str} at row {i}, col {j}")
                            return cell_str
            
            # If no specific number found, generate one based on row
            logger.warning(f"No invoice number found at block {block_start_idx}, generating automatic number")
            return f"AUTO_{block_start_idx}"
            
        except Exception as e:
            logger.error(f"Error extracting invoice number at row {block_start_idx}: {e}")
            return f"ERROR_{block_start_idx}"
    
    def extract_date_from_block(self, df: pd.DataFrame, block_start_idx: int, block_end_idx: int) -> Optional[pd.Timestamp]:
        """
        Extract date by scanning around the invoice block for "Fecha" label
        
        Args:
            df: DataFrame containing the data
            block_start_idx: Start of the invoice block
            block_end_idx: End of the invoice block
            
        Returns:
            Parsed date or None if not found
        """
        try:
            # Scan ±10 rows around the block
            scan_start = max(0, block_start_idx - 10)
            scan_end = min(len(df), block_end_idx + 10)
            
            for i in range(scan_start, scan_end):
                row = df.iloc[i]
                
                # Look for "Fecha" label
                for j, cell in enumerate(row):
                    if pd.notna(cell) and 'fecha' in str(cell).lower():
                        # Look for date value in adjacent cells (wider search)
                        for k in range(max(0, j-3), min(len(row), j+4)):
                            if k != j:  # Skip the "Fecha" cell itself
                                date_cell = row.iloc[k]
                                if pd.notna(date_cell):
                                    parsed_date = parse_date(date_cell, dayfirst=True)
                                if parsed_date:
                                    logger.info(f"Found date: {parsed_date} at row {i}, col {k}")
                                    # Ensure we return a date object, not datetime
                                    if hasattr(parsed_date, 'date'):
                                        return parsed_date.date()
                                    return parsed_date
                
                # Also look for any date-like values in the row
                for j, cell in enumerate(row):
                    if pd.notna(cell):
                        parsed_date = parse_date(cell, dayfirst=True)
                        if parsed_date and parsed_date.year >= 2020:  # Reasonable date range
                            logger.info(f"Found date (no label): {parsed_date} at row {i}, col {j}")
                            # Ensure we return a date object, not datetime
                            if hasattr(parsed_date, 'date'):
                                return parsed_date.date()
                            return parsed_date
            
            # If no date found, use today's date as fallback
            logger.warning(f"No date found for block {block_start_idx}, using today's date")
            return date.today()
            
        except Exception as e:
            logger.error(f"Error extracting date from block {block_start_idx}-{block_end_idx}: {e}")
            return date.today()
    
    def find_detail_header(self, df: pd.DataFrame, products_idx: int, block_end_idx: int) -> Optional[int]:
        """
        Find the detail header row after "PRODUCTOS" marker
        
        Args:
            df: DataFrame containing the data
            products_idx: Index of "PRODUCTOS" row
            block_end_idx: End of current block
            
        Returns:
            Index of detail header row or None if not found
        """
        try:
            # If no products_idx provided, search from a reasonable starting point
            start_idx = products_idx + 1 if products_idx is not None else 0
            
            # Look in the next several rows
            for i in range(start_idx, min(start_idx + 10, block_end_idx, len(df))):
                row = df.iloc[i]
                row_str = ' '.join([str(cell).lower().strip() for cell in row if pd.notna(cell)])
                
                # Check if it contains expected column headers
                header_indicators = ['código', 'cabys', 'descripción', 'cantidad', 'precio', 'costo', 'total']
                matches = sum(1 for indicator in header_indicators if indicator in row_str)
                
                if matches >= 2:  # More flexible requirement
                    logger.info(f"Found detail header at row {i} with {matches} matches")
                    return i
                
                # Also check if row has the expected structure (multiple non-empty cells)
                non_empty_cells = sum(1 for cell in row if pd.notna(cell) and str(cell).strip())
                if non_empty_cells >= 5:  # Likely a header row
                    # Check if it looks like column names (not numbers)
                    text_cells = 0
                    for cell in row:
                        if pd.notna(cell):
                            cell_str = str(cell).strip()
                            if cell_str and not cell_str.replace('.', '').replace(',', '').isdigit():
                                text_cells += 1
                    
                    if text_cells >= 3:  # Mostly text, likely headers
                        logger.info(f"Found potential detail header at row {i} (structure-based)")
                        return i
            
            # If no header found, return the row after products or a reasonable default
            fallback_idx = start_idx if start_idx < len(df) else None
            logger.warning(f"No detail header found, using fallback: {fallback_idx}")
            return fallback_idx
            
        except Exception as e:
            logger.error(f"Error finding detail header after row {products_idx}: {e}")
            return None
    
    def extract_detail_lines(self, df: pd.DataFrame, detail_header_idx: int, block_end_idx: int) -> List[Dict]:
        """
        Extract detail lines from the sales block
        
        Args:
            df: DataFrame containing the data
            detail_header_idx: Index of the detail header row
            block_end_idx: End of current block
            
        Returns:
            List of dictionaries with detail line data
        """
        details = []
        
        try:
            # Start from the row after detail header
            start_idx = detail_header_idx + 1
            
            for idx in range(start_idx, block_end_idx):
                row = df.iloc[idx]
                
                # Skip empty rows
                if row.isna().all():
                    continue
                
                # Check if this might be the start of a new block
                if self.detect_invoice_block(row):
                    break
                
                # Extract detail data based on expected column positions
                detail_data = {}
                
                for col_idx, col_name in self.detail_columns.items():
                    if col_idx < len(row):
                        value = row.iloc[col_idx]
                        
                        if col_name in ['cantidad', 'descuento', 'utilidad', 'costo', 'precio_unit', 'total']:
                            detail_data[col_name] = normalize_number(value)
                        else:
                            detail_data[col_name] = normalize_text(value)
                
                # Process description and fraction detection
                descripcion = detail_data.get('descripcion', '')
                detail_data['es_fraccion'] = 1 if is_fraction_product(descripcion) else 0
                detail_data['nombre_clean'] = clean_product_name(descripcion, remove_frac_prefix=True)
                
                # Calculate fraction factor and normalized quantity
                if detail_data['es_fraccion'] == 1:
                    costo = detail_data.get('costo', 0)
                    utilidad = detail_data.get('utilidad', 0)
                    precio_unit = detail_data.get('precio_unit', 0)
                    
                    factor = calculate_fraction_factor(costo, utilidad, precio_unit)
                    detail_data['factor_fraccion'] = factor if factor else 1
                    
                    cantidad = detail_data.get('cantidad', 0) or 0
                    detail_data['qty_normalizada'] = cantidad / detail_data['factor_fraccion'] if detail_data['factor_fraccion'] > 0 else cantidad
                else:
                    detail_data['factor_fraccion'] = 1
                    detail_data['qty_normalizada'] = detail_data.get('cantidad', 0) or 0
                
                # Validate that we have essential data
                if (detail_data.get('cabys') and 
                    detail_data.get('nombre_clean') and 
                    detail_data.get('cantidad') is not None):
                    details.append(detail_data)
                
        except Exception as e:
            logger.error(f"Error extracting detail lines from {detail_header_idx} to {block_end_idx}: {e}")
        
        return details
    
    def parse_sheet(self, df: pd.DataFrame, sheet_name: str = "Contado") -> Tuple[List[Dict], List[Dict]]:
        """
        Parse a single sheet of the sales file
        
        Args:
            df: DataFrame containing the sheet data
            sheet_name: Name of the sheet being parsed
            
        Returns:
            Tuple of (headers_list, details_list)
        """
        headers = []
        details = []
        
        logger.info(f"Parsing sheet: {sheet_name} with {len(df)} rows and {len(df.columns)} columns")
        
        # Strategy 1: Look for structured invoice blocks
        invoice_blocks = []
        
        for idx, row in df.iterrows():
            if self.detect_invoice_block(row):
                invoice_blocks.append(idx)
        
        logger.info(f"Found {len(invoice_blocks)} potential invoice blocks")
        
        if invoice_blocks:
            # Process structured blocks
            for i, block_start in enumerate(invoice_blocks):
                try:
                    # Determine block end
                    block_end = invoice_blocks[i + 1] if i + 1 < len(invoice_blocks) else len(df)
                    
                    logger.info(f"Processing block {i+1}: rows {block_start} to {block_end}")
                    
                    # Extract invoice number
                    invoice_number = self.extract_invoice_number(df, block_start)
                    
                    # Extract date
                    fecha = self.extract_date_from_block(df, block_start, block_end)
                    
                    # Find "PRODUCTOS" section (optional)
                    products_idx = None
                    for idx in range(block_start, min(block_start + 20, block_end)):
                        if self.detect_products_section(df.iloc[idx]):
                            products_idx = idx
                            break
                    
                    # Find detail header (more flexible)
                    detail_header_idx = self.find_detail_header(df, products_idx, block_end)
                    
                    # Extract detail lines
                    if detail_header_idx is not None:
                        detail_lines = self.extract_detail_lines(df, detail_header_idx, block_end)
                    else:
                        # Try to extract details without a clear header
                        detail_lines = self.extract_detail_lines_flexible(df, block_start, block_end)
                    
                    if detail_lines:
                        # Create header record
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
                        
                        # Add invoice number to each detail line
                        for detail in detail_lines:
                            detail['no_factura_interna'] = invoice_number
                        
                        details.extend(detail_lines)
                        
                        logger.info(f"Processed invoice {invoice_number} with {len(detail_lines)} detail lines")
                    else:
                        logger.warning(f"No valid detail lines found for invoice {invoice_number}")
                        
                except Exception as e:
                    logger.error(f"Error processing invoice block starting at row {block_start}: {e}")
                    continue
        
        # Strategy 2: If no structured blocks found, try to parse as continuous data
        if not headers and not details:
            logger.info("No structured blocks found, trying continuous parsing")
            headers, details = self.parse_continuous_data(df)
        
        return headers, details
    
    def extract_detail_lines_flexible(self, df: pd.DataFrame, start_idx: int, end_idx: int) -> List[Dict]:
        """
        Extract detail lines with more flexible approach
        """
        details = []
        
        try:
            for idx in range(start_idx, min(end_idx, len(df))):
                row = df.iloc[idx]
                
                # Skip empty rows
                if row.isna().all():
                    continue
                
                # Look for rows that might contain product data
                # Check if row has enough non-empty cells and some numeric values
                non_empty_cells = [cell for cell in row if pd.notna(cell) and str(cell).strip()]
                
                if len(non_empty_cells) >= 4:  # Minimum for a detail line
                    # Try to extract product information
                    detail_data = self.extract_product_from_row(row, idx)
                    if detail_data:
                        details.append(detail_data)
            
            logger.info(f"Extracted {len(details)} detail lines using flexible approach")
            return details
            
        except Exception as e:
            logger.error(f"Error in flexible detail extraction: {e}")
            return []
    
    def extract_product_from_row(self, row: pd.Series, row_idx: int) -> Optional[Dict]:
        """
        Try to extract product information from a single row
        """
        try:
            # Look for text that could be product description
            descripcion = ""
            cabys = ""
            codigo = ""
            cantidad = None
            costo = None
            precio_unit = None
            
            # Scan row for different types of data
            for i, cell in enumerate(row):
                if pd.notna(cell):
                    cell_str = str(cell).strip()
                    
                    # Try to identify product description (longer text)
                    if len(cell_str) > 5 and not cell_str.replace('.', '').replace(',', '').isdigit():
                        if not descripcion or len(cell_str) > len(descripcion):
                            descripcion = cell_str
                    
                    # Try to identify CABYS (shorter alphanumeric)
                    elif len(cell_str) <= 10 and not cabys:
                        cabys = cell_str
                    
                    # Try to identify numeric values
                    num_val = normalize_number(cell)
                    if num_val is not None and num_val > 0:
                        if cantidad is None and num_val < 1000:  # Likely quantity
                            cantidad = num_val
                        elif costo is None:  # Likely cost
                            costo = num_val
                        elif precio_unit is None:  # Likely price
                            precio_unit = num_val
            
            # Validate we have minimum required data
            if descripcion and cantidad is not None:
                es_fraccion = is_fraction_product(descripcion)
                nombre_clean = clean_product_name(descripcion, remove_frac_prefix=True)
                
                detail_data = {
                    'cabys': cabys,
                    'codigo': codigo,
                    'descripcion': descripcion,
                    'nombre_clean': nombre_clean,
                    'cantidad': cantidad,
                    'descuento': 0,
                    'utilidad': 0,
                    'costo': costo or precio_unit or 0,
                    'precio_unit': precio_unit or costo or 0,
                    'total': cantidad * (precio_unit or costo or 0),
                    'es_fraccion': 1 if es_fraccion else 0,
                    'factor_fraccion': 1,
                    'qty_normalizada': cantidad
                }
                
                # Calculate fraction factor if needed
                if es_fraccion and costo and precio_unit:
                    factor = calculate_fraction_factor(costo, 0, precio_unit)
                    if factor:
                        detail_data['factor_fraccion'] = factor
                        detail_data['qty_normalizada'] = cantidad / factor
                
                return detail_data
            
            return None
            
        except Exception as e:
            logger.debug(f"Error extracting product from row {row_idx}: {e}")
            return None
    
    def parse_continuous_data(self, df: pd.DataFrame) -> Tuple[List[Dict], List[Dict]]:
        """
        Parse data as continuous table without clear block structure
        """
        headers = []
        details = []
        
        try:
            # Generate a single header for all data
            header_data = {
                'no_factura_interna': 'CONTINUOUS_DATA',
                'fecha': datetime.now().date(),
                'tipo_documento': 'CONTADO',
                'cliente': '',
                'cedula': '',
                'vendedor': '',
                'caja': ''
            }
            headers.append(header_data)
            
            # Try to extract all product lines
            for idx, row in df.iterrows():
                detail_data = self.extract_product_from_row(row, idx)
                if detail_data:
                    detail_data['no_factura_interna'] = 'CONTINUOUS_DATA'
                    details.append(detail_data)
            
            logger.info(f"Continuous parsing found {len(details)} detail lines")
            return headers, details
            
        except Exception as e:
            logger.error(f"Error in continuous parsing: {e}")
            return [], []

def parse_ventas_file(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Parse the complete sales file
    
    Args:
        file_path_or_buffer: File path or buffer containing the Excel file
        
    Returns:
        Dictionary with 'headers' and 'details' lists
    """
    parser = VentasParser()
    all_headers = []
    all_details = []
    
    try:
        # Read Excel file
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Found sheets: {excel_file.sheet_names}")
        
        # Look for the main sheet (typically "Contado")
        target_sheets = ["Contado", "Ventas", "Sheet1"]
        sheet_to_parse = None
        
        for sheet in target_sheets:
            if sheet in excel_file.sheet_names:
                sheet_to_parse = sheet
                break
        
        if not sheet_to_parse:
            # Use the first sheet if no standard name found
            sheet_to_parse = excel_file.sheet_names[0]
            logger.warning(f"Using first sheet: {sheet_to_parse}")
        
        logger.info(f"Parsing sheet: {sheet_to_parse}")
        
        # Parse the sheet with error handling
        df = pd.read_excel(file_path_or_buffer, sheet_name=sheet_to_parse, header=None)
        
        if df.empty:
            logger.warning("Excel sheet is empty")
            return {'headers': [], 'details': []}
        
        logger.info(f"Sheet has {len(df)} rows and {len(df.columns)} columns")
        
        headers, details = parser.parse_sheet(df, sheet_to_parse)
        
        all_headers.extend(headers)
        all_details.extend(details)
        
        logger.info(f"Successfully parsed {len(all_headers)} invoices with {len(all_details)} detail lines")
        
        return {
            'headers': all_headers,
            'details': all_details
        }
        
    except Exception as e:
        logger.error(f"Error parsing sales file: {e}")
        # Return empty structure instead of raising to prevent app crash
        return {'headers': [], 'details': []}

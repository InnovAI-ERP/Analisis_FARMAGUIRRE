"""
Parser for Compras (Purchases) Excel files
Handles block-based structure with invoice headers and detail lines
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional
import logging
from utils.dates_numbers import parse_date, normalize_number, normalize_text, clean_product_name, is_fraction_product

logger = logging.getLogger(__name__)

class ComprasParser:
    """Parser for purchase files with block detection"""
    
    def __init__(self):
        # Expected header patterns for invoice header
        self.header_patterns = [
            'fecha', 'no consecutivo', 'no factura', 'no guia', 
            'ced. juridica', 'proveedor'
        ]
        
        # Expected patterns for detail header
        self.detail_patterns = [
            'cabys', 'código', 'variación', 'código referencia', 
            'nombre', 'código color', 'color', 'cantidad', 
            'descuento', 'utilidad', 'precio'
        ]
    
    def detect_invoice_header(self, row: pd.Series) -> bool:
        """
        Detect if a row contains invoice header information
        
        Args:
            row: Pandas Series representing a row
            
        Returns:
            True if row appears to be an invoice header
        """
        try:
            # Convert row to string and normalize
            row_str = ' '.join([str(cell).lower().strip() for cell in row if pd.notna(cell) and str(cell).strip()])
            
            if not row_str:
                return False
            
            # Check if it contains key header patterns
            matches = sum(1 for pattern in self.header_patterns if pattern in row_str)
            
            # Require at least 3 out of 6 patterns to match (more flexible)
            return matches >= 3
        except Exception as e:
            logger.error(f"Error in detect_invoice_header: {e}")
            return False
    
    def detect_detail_header(self, row: pd.Series) -> bool:
        """
        Detect if a row contains detail header information
        
        Args:
            row: Pandas Series representing a row
            
        Returns:
            True if row appears to be a detail header
        """
        try:
            # Convert row to string and normalize
            row_str = ' '.join([str(cell).lower().strip() for cell in row if pd.notna(cell) and str(cell).strip()])
            
            if not row_str:
                return False
            
            # Check if it contains key detail patterns
            matches = sum(1 for pattern in self.detail_patterns if pattern in row_str)
            
            # Require at least 4 out of 11 patterns to match (more flexible)
            return matches >= 4
        except Exception as e:
            logger.error(f"Error in detect_detail_header: {e}")
            return False
    
    def extract_invoice_data(self, df: pd.DataFrame, header_row_idx: int) -> Optional[Dict]:
        """
        Extract invoice header data from the row following the header
        
        Args:
            df: DataFrame containing the data
            header_row_idx: Index of the header row
            
        Returns:
            Dictionary with invoice data or None if extraction fails
        """
        try:
            if header_row_idx + 1 >= len(df):
                return None
            
            data_row = df.iloc[header_row_idx + 1]
            
            # Map data based on expected positions
            # This is a simplified mapping - in practice, you'd want to be more robust
            parsed_fecha = parse_date(data_row.iloc[0] if len(data_row) > 0 else None, dayfirst=True)
            # Ensure fecha is a date object, not datetime
            if parsed_fecha and hasattr(parsed_fecha, 'date'):
                parsed_fecha = parsed_fecha.date()
            
            invoice_data = {
                'fecha': parsed_fecha,
                'no_consecutivo': normalize_text(data_row.iloc[1] if len(data_row) > 1 else None),
                'no_factura': normalize_text(data_row.iloc[2] if len(data_row) > 2 else None),
                'no_guia': normalize_text(data_row.iloc[3] if len(data_row) > 3 else None),
                'ced_juridica': normalize_text(data_row.iloc[4] if len(data_row) > 4 else None),
                'proveedor': normalize_text(data_row.iloc[5] if len(data_row) > 5 else None)
            }
            
            # Validate that we have at least fecha and no_consecutivo
            if invoice_data['fecha'] and invoice_data['no_consecutivo']:
                return invoice_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error extracting invoice data at row {header_row_idx}: {e}")
            return None
    
    def extract_detail_lines(self, df: pd.DataFrame, detail_header_idx: int, next_block_idx: int) -> List[Dict]:
        """
        Extract detail lines between detail header and next block
        
        Args:
            df: DataFrame containing the data
            detail_header_idx: Index of the detail header row
            next_block_idx: Index of the next block (or end of data)
            
        Returns:
            List of dictionaries with detail line data
        """
        details = []
        
        try:
            # Start from the row after detail header
            start_idx = detail_header_idx + 1
            end_idx = min(next_block_idx, len(df))
            
            for idx in range(start_idx, end_idx):
                row = df.iloc[idx]
                
                # Skip empty rows
                if row.isna().all():
                    continue
                
                # Extract detail data based on the actual column structure
                # Based on analysis: [Cabys, Código, Variación, Código referencia, Nombre, Código color, Color, Cantidad, Regalía, Aplica impuesto, Costo, Descuento, Utilidad, Precio, Total]
                detail_data = {
                    'cabys': normalize_text(row.iloc[0] if len(row) > 0 else None),
                    'codigo': normalize_text(row.iloc[1] if len(row) > 1 else None),
                    'variacion': normalize_text(row.iloc[2] if len(row) > 2 else None),
                    'codigo_referencia': normalize_text(row.iloc[3] if len(row) > 3 else None),
                    'nombre': normalize_text(row.iloc[4] if len(row) > 4 else None),  # Nombre is column 4
                    'codigo_color': normalize_text(row.iloc[5] if len(row) > 5 else None),
                    'color': normalize_text(row.iloc[6] if len(row) > 6 else None),
                    'cantidad': normalize_number(row.iloc[7] if len(row) > 7 else None),  # Cantidad is column 7
                    'regalia': normalize_number(row.iloc[8] if len(row) > 8 else None),
                    'aplica_impuesto': normalize_text(row.iloc[9] if len(row) > 9 else None),
                    'costo': normalize_number(row.iloc[10] if len(row) > 10 else None),  # Costo is column 10
                    'descuento': normalize_number(row.iloc[11] if len(row) > 11 else None),  # Descuento is column 11
                    'utilidad': normalize_number(row.iloc[12] if len(row) > 12 else None),  # Utilidad is column 12
                    'precio_unit': normalize_number(row.iloc[13] if len(row) > 13 else None)  # Precio is column 13
                }
                
                # Clean product name
                detail_data['nombre_clean'] = clean_product_name(detail_data['nombre'], remove_frac_prefix=False)
                
                # Validate that we have essential data
                if detail_data['cabys'] and detail_data['nombre_clean'] and detail_data['cantidad'] is not None:
                    details.append(detail_data)
                
        except Exception as e:
            logger.error(f"Error extracting detail lines from {detail_header_idx} to {next_block_idx}: {e}")
        
        return details
    
    def parse_sheet(self, df: pd.DataFrame, sheet_name: str = "Compras Contado") -> Tuple[List[Dict], List[Dict]]:
        """
        Parse a single sheet of the purchases file
        
        Args:
            df: DataFrame containing the sheet data
            sheet_name: Name of the sheet being parsed
            
        Returns:
            Tuple of (headers_list, details_list)
        """
        headers = []
        details = []
        
        logger.info(f"Parsing sheet: {sheet_name} with {len(df)} rows")
        
        # Find all block boundaries
        invoice_header_indices = []
        detail_header_indices = []
        
        for idx, row in df.iterrows():
            if self.detect_invoice_header(row):
                invoice_header_indices.append(idx)
            elif self.detect_detail_header(row):
                detail_header_indices.append(idx)
        
        logger.info(f"Found {len(invoice_header_indices)} invoice headers and {len(detail_header_indices)} detail headers")
        
        # Process each invoice block
        for i, inv_header_idx in enumerate(invoice_header_indices):
            # Extract invoice data
            invoice_data = self.extract_invoice_data(df, inv_header_idx)
            if not invoice_data:
                logger.warning(f"Could not extract invoice data at row {inv_header_idx}")
                continue
            
            # Find corresponding detail header
            detail_header_idx = None
            for det_idx in detail_header_indices:
                if det_idx > inv_header_idx:
                    detail_header_idx = det_idx
                    break
            
            if detail_header_idx is None:
                logger.warning(f"No detail header found for invoice at row {inv_header_idx}")
                continue
            
            # Find next block boundary
            next_block_idx = len(df)
            for next_inv_idx in invoice_header_indices:
                if next_inv_idx > inv_header_idx:
                    next_block_idx = next_inv_idx
                    break
            
            # Extract detail lines
            detail_lines = self.extract_detail_lines(df, detail_header_idx, next_block_idx)
            
            # Add invoice reference and populate header data in each detail line
            for detail in detail_lines:
                detail['no_consecutivo'] = invoice_data['no_consecutivo']
                # Populate header data for normalization
                detail['fecha_compra'] = invoice_data['fecha']
                detail['no_factura'] = invoice_data['no_factura']
                detail['no_guia'] = invoice_data['no_guia']
                detail['ced_juridica'] = invoice_data['ced_juridica']
                detail['proveedor'] = invoice_data['proveedor']
                
                # Calculate normalization fields
                es_fraccion = is_fraction_product(detail.get('nombre', ''))
                detail['es_fraccion'] = 1 if es_fraccion else 0
                detail['factor_fraccion'] = 1.0  # Default, can be calculated later
                detail['qty_normalizada'] = detail.get('cantidad', 0)  # For purchases, usually no fractions
            
            details.extend(detail_lines)
                
            logger.info(f"Processed invoice {invoice_data['no_consecutivo']} with {len(detail_lines)} detail lines")
        else:
            logger.warning(f"No valid detail lines found for invoice {invoice_data['no_consecutivo']}")
        
        return headers, details

def parse_compras_file(file_path_or_buffer) -> Dict[str, List[Dict]]:
    """
    Parse the complete purchases file
    
    Args:
        file_path_or_buffer: File path or buffer containing the Excel file
        
    Returns:
        Dictionary with 'headers' and 'details' lists
    """
    parser = ComprasParser()
    all_headers = []
    all_details = []
    
    try:
        # Read Excel file
        excel_file = pd.ExcelFile(file_path_or_buffer)
        logger.info(f"Found sheets: {excel_file.sheet_names}")
        
        # Look for the main sheet (typically "Compras Contado")
        target_sheets = ["Compras Contado", "Compras", "Sheet1"]
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
        logger.error(f"Error parsing purchases file: {e}")
        # Return empty structure instead of raising to prevent app crash
        return {'headers': [], 'details': []}

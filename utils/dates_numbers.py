"""
Utilities for parsing and normalizing dates, numbers, and text
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime, date
from typing import Union, Optional
import logging

logger = logging.getLogger(__name__)

def parse_date(date_value: Union[str, datetime, date], dayfirst: bool = True) -> Optional[date]:
    """
    Parse date from various formats
    
    Args:
        date_value: Date value to parse
        dayfirst: Whether to interpret the first value as day (dd-mm-yyyy format)
    
    Returns:
        Parsed date or None if parsing fails
    """
    if pd.isna(date_value) or date_value is None:
        return None
    
    if isinstance(date_value, date):
        return date_value
    
    if isinstance(date_value, datetime):
        return date_value.date()
    
    if isinstance(date_value, str):
        # Clean the string
        date_str = str(date_value).strip()
        
        if not date_str:
            return None
        
        try:
            # Try pandas date parser with dayfirst option
            parsed = pd.to_datetime(date_str, dayfirst=dayfirst, errors='coerce')
            if pd.notna(parsed):
                return parsed.date()
        except:
            pass
        
        # Try common date patterns
        patterns = [
            r'(\d{1,2})-(\d{1,2})-(\d{4})',  # dd-mm-yyyy or mm-dd-yyyy
            r'(\d{1,2})/(\d{1,2})/(\d{4})',  # dd/mm/yyyy or mm/dd/yyyy
            r'(\d{4})-(\d{1,2})-(\d{1,2})',  # yyyy-mm-dd
        ]
        
        for pattern in patterns:
            match = re.search(pattern, date_str)
            if match:
                try:
                    if dayfirst and pattern.startswith(r'(\d{1,2})'):
                        # dd-mm-yyyy or dd/mm/yyyy
                        day, month, year = match.groups()
                        return date(int(year), int(month), int(day))
                    elif pattern.startswith(r'(\d{4})'):
                        # yyyy-mm-dd
                        year, month, day = match.groups()
                        return date(int(year), int(month), int(day))
                    else:
                        # mm-dd-yyyy or mm/dd/yyyy
                        month, day, year = match.groups()
                        return date(int(year), int(month), int(day))
                except ValueError:
                    continue
    
    logger.warning(f"Could not parse date: {date_value}")
    return None

def normalize_number(value: Union[str, float, int]) -> Optional[float]:
    """
    Normalize numeric values, handling commas, percentages, and currency symbols
    
    Args:
        value: Value to normalize
    
    Returns:
        Normalized float value or None if parsing fails
    """
    if pd.isna(value) or value is None:
        return None
    
    if isinstance(value, (int, float)):
        return float(value)
    
    if isinstance(value, str):
        # Clean the string
        clean_value = str(value).strip()
        
        if not clean_value:
            return None
        
        # Remove currency symbols and spaces
        clean_value = re.sub(r'[₡$€£¥\s]', '', clean_value)
        
        # Handle percentage
        is_percentage = clean_value.endswith('%')
        if is_percentage:
            clean_value = clean_value[:-1]
        
        # Replace comma with dot for decimal separator
        clean_value = clean_value.replace(',', '.')
        
        # Remove any remaining non-numeric characters except dots and minus
        clean_value = re.sub(r'[^\d.-]', '', clean_value)
        
        try:
            result = float(clean_value)
            # If it was a percentage, keep as percentage value (25% -> 25.0, not 0.25)
            return result
        except ValueError:
            logger.warning(f"Could not parse number: {value}")
            return None
    
    logger.warning(f"Unexpected value type for number: {type(value)}")
    return None

def normalize_text(text: Union[str, None]) -> str:
    """
    Normalize text by trimming, converting to uppercase, and removing extra spaces
    
    Args:
        text: Text to normalize
    
    Returns:
        Normalized text
    """
    if pd.isna(text) or text is None:
        return ""
    
    # Convert to string and clean
    clean_text = str(text).strip().upper()
    
    # Remove extra spaces
    clean_text = re.sub(r'\s+', ' ', clean_text)
    
    return clean_text

def clean_product_name(name: str, remove_frac_prefix: bool = True) -> str:
    """
    Clean and normalize product names
    
    Args:
        name: Product name to clean
        remove_frac_prefix: Whether to remove "FRAC." prefix
        
    Returns:
        Cleaned product name
    """
    if not name or pd.isna(name):
        return ""
    
    # Convert to string and strip
    clean_name = str(name).strip().upper()
    
    # Remove "FRAC." prefix if requested
    if remove_frac_prefix and clean_name.startswith("FRAC."):
        clean_name = clean_name[5:].strip()
    
    # Remove special characters at the end (*, +, -, etc.)
    clean_name = re.sub(r'[*+\-#@!]+$', '', clean_name).strip()
    
    # Remove other common special characters that don't add value
    clean_name = re.sub(r'[^\w\s\./()]', ' ', clean_name)
    
    # Remove extra whitespace
    clean_name = re.sub(r'\s+', ' ', clean_name)
    
    return clean_name

def is_fraction_product(description: str) -> bool:
    """
    Check if a product description indicates a fraction
    
    Args:
        description: Product description
    
    Returns:
        True if product is a fraction
    """
    if pd.isna(description) or description is None:
        return False
    
    return str(description).strip().upper().startswith('FRAC. ')

def calculate_fraction_factor(costo: float, utilidad: float, precio_unit: float) -> Optional[int]:
    """
    Calculate fraction factor for converting fractional sales to complete units
    
    Args:
        costo: Cost per complete unit
        utilidad: Profit margin percentage
        precio_unit: Price per fraction
    
    Returns:
        Fraction factor (integer >= 1) or None if calculation fails
    """
    try:
        # Validate inputs
        if any(pd.isna(x) or x is None for x in [costo, utilidad, precio_unit]):
            return None
        
        if precio_unit <= 0:
            return None
        
        # Calculate factor: (Cost * (1 + Profit%/100)) / PricePerFraction
        factor = (costo * (1 + utilidad / 100)) / precio_unit
        
        # Round to nearest integer and ensure >= 1
        factor_int = max(1, round(factor))
        
        # Log outliers
        if factor_int > 200:
            logger.warning(f"Unusually high fraction factor: {factor_int} (costo={costo}, utilidad={utilidad}, precio_unit={precio_unit})")
        
        return factor_int
        
    except Exception as e:
        logger.error(f"Error calculating fraction factor: {e}")
        return None

def calculate_fraction_factor_from_prices(precio_compra_unitario: float, precio_venta_fraccion: float) -> Optional[int]:
    """
    Calculate fraction factor based on unit prices comparison
    
    This is the correct method for calculating how many fractions make up one complete unit
    by comparing the unit price of purchases vs the unit price of fraction sales.
    
    Args:
        precio_compra_unitario: Unit price from purchases (price per complete unit)
        precio_venta_fraccion: Unit price from fraction sales (price per fraction)
    
    Returns:
        Fraction factor (integer >= 1) or None if calculation fails
        
    Example:
        - Purchased: 10 boxes at ₡1,000 each = ₡10,000 total
        - Sold: 90 fractions at ₡100 each = ₡9,000 total
        - Factor: ₡1,000 ÷ ₡100 = 10 fractions per box
        - Units sold: 90 fractions ÷ 10 = 9 boxes
        - Stock: 10 boxes - 9 boxes = 1 box
    """
    try:
        # Validate inputs
        if any(pd.isna(x) or x is None for x in [precio_compra_unitario, precio_venta_fraccion]):
            return None
        
        if precio_venta_fraccion <= 0 or precio_compra_unitario <= 0:
            return None
        
        # Calculate factor: PricePerCompleteUnit / PricePerFraction
        factor = precio_compra_unitario / precio_venta_fraccion
        
        # Round to nearest integer and ensure >= 1
        factor_int = max(1, round(factor))
        
        # Log calculation for debugging
        logger.info(f"Fraction factor calculation: ₡{precio_compra_unitario} ÷ ₡{precio_venta_fraccion} = {factor} → {factor_int}")
        
        # Log outliers
        if factor_int > 200:
            logger.warning(f"Unusually high fraction factor: {factor_int} (precio_compra={precio_compra_unitario}, precio_fraccion={precio_venta_fraccion})")
        
        return factor_int
        
    except Exception as e:
        logger.error(f"Error calculating fraction factor from prices: {e}")
        return None

def validate_date_range(start_date: date, end_date: date) -> bool:
    """
    Validate that date range is logical
    
    Args:
        start_date: Start date
        end_date: End date
    
    Returns:
        True if date range is valid
    """
    if start_date is None or end_date is None:
        return False
    
    return start_date <= end_date

def get_product_key(cabys: str, nombre_clean: str) -> tuple:
    """
    Generate product key tuple for consistent product identification
    
    Args:
        cabys: CABYS code
        nombre_clean: Cleaned product name
    
    Returns:
        Product key tuple
    """
    return (
        normalize_text(cabys) if cabys else "",
        normalize_text(nombre_clean) if nombre_clean else ""
    )

def safe_divide(numerator: float, denominator: float, default: float = 0.0) -> float:
    """
    Safely divide two numbers, returning default if denominator is zero
    
    Args:
        numerator: Numerator
        denominator: Denominator
        default: Default value if division by zero
    
    Returns:
        Division result or default
    """
    if denominator == 0 or pd.isna(denominator):
        return default
    
    try:
        return numerator / denominator
    except:
        return default

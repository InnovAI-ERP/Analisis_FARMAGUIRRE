"""
KPI calculation utilities for inventory analysis
Includes rotation, DIO, coverage, ROP, safety stock, and ABC/XYZ classification
"""

import pandas as pd
import numpy as np
from datetime import date, timedelta
from typing import Dict, List, Tuple, Optional
import logging
from sqlalchemy import text
from db.database import DatabaseSession
from db.models_normalized import ProductoKpis
from utils.dates_numbers import safe_divide, parse_date, calculate_fraction_factor_from_prices
from scipy import stats

logger = logging.getLogger(__name__)

class KpiCalculator:
    """Calculator for inventory KPIs"""
    
    def __init__(self, service_level: float = 0.95, lead_time_days: int = 7):
        self.service_level = service_level
        self.lead_time_days = lead_time_days
        self.z_scores = {
            0.90: 1.282,
            0.95: 1.645,
            0.99: 2.326,
            0.995: 2.576
        }
    
    def get_z_score(self, service_level: float) -> float:
        """Get Z-score for given service level"""
        # Find closest service level in our table
        closest_level = min(self.z_scores.keys(), key=lambda x: abs(x - service_level))
        return self.z_scores[closest_level]
    
    def calculate_stock_series(self, movements: List[Dict], initial_stock: float = 0.0) -> List[Dict]:
        """
        Calculate daily stock levels from movement data
        
        Args:
            movements: List of daily movements with fecha, qty_in, qty_out
            initial_stock: Initial stock level
            
        Returns:
            List of dictionaries with fecha, stock_level
        """
        stock_series = []
        current_stock = initial_stock
        
        # Sort movements by date
        sorted_movements = sorted(movements, key=lambda x: x['fecha'])
        
        for movement in sorted_movements:
            qty_in = movement.get('qty_in', 0) or 0
            qty_out = movement.get('qty_out', 0) or 0
            
            current_stock += qty_in - qty_out
            
            stock_series.append({
                'fecha': movement['fecha'],
                'stock_level': max(0, current_stock)  # Don't allow negative stock
            })
        
        return stock_series
    
    def calculate_fraction_factor_for_product(self, nombre_clean: str, session) -> int:
        """
        Calculate fraction factor by comparing purchase and sales unit prices for the same product
        
        Args:
            nombre_clean: Clean product name
            session: Database session
            
        Returns:
            Fraction factor (how many fractions make one complete unit)
        """
        try:
            # Get average purchase unit price (complete units)
            compras_query = text("""
                SELECT AVG(precio_unit) as avg_precio_compra
                FROM compras_normalized 
                WHERE nombre_clean = :nombre_clean 
                AND precio_unit > 0
            """)
            compras_result = session.execute(compras_query, {'nombre_clean': nombre_clean}).fetchone()
            precio_compra_unitario = compras_result.avg_precio_compra if compras_result and compras_result.avg_precio_compra else None
            
            # Get average sales unit price (fractions) - only for products with "FRAC" in original description
            ventas_query = text("""
                SELECT AVG(precio_unit) as avg_precio_venta
                FROM ventas_normalized 
                WHERE nombre_clean = :nombre_clean 
                AND precio_unit > 0
                AND (descripcion LIKE 'FRAC.%' OR descripcion LIKE '%FRAC%')
            """)
            ventas_result = session.execute(ventas_query, {'nombre_clean': nombre_clean}).fetchone()
            precio_venta_fraccion = ventas_result.avg_precio_venta if ventas_result and ventas_result.avg_precio_venta else None
            
            # Calculate factor if we have both prices
            if precio_compra_unitario and precio_venta_fraccion:
                factor = calculate_fraction_factor_from_prices(precio_compra_unitario, precio_venta_fraccion)
                if factor:
                    logger.info(f"Calculated fraction factor for {nombre_clean}: {factor} (₡{precio_compra_unitario:.2f} ÷ ₡{precio_venta_fraccion:.2f})")
                    return factor
            
            # Default to 1 if calculation fails
            logger.debug(f"Using default fraction factor (1) for {nombre_clean}")
            return 1
            
        except Exception as e:
            logger.error(f"Error calculating fraction factor for {nombre_clean}: {e}")
            return 1

    def calculate_basic_metrics(self, movements: List[Dict], costs: List[Dict]) -> Dict:
        """
        Calculate basic inventory metrics
        
        Args:
            movements: List of daily movements
            costs: List of cost data
            
        Returns:
            Dictionary with basic metrics
        """
        total_qty_in = sum(m.get('qty_in', 0) or 0 for m in movements)
        total_qty_out = sum(m.get('qty_out', 0) or 0 for m in movements)
        
        # Calculate stock series
        stock_series = self.calculate_stock_series(movements)
        
        # Calculate final stock and average inventory
        if stock_series:
            stock_final = max(0, stock_series[-1]['stock_level'])
            avg_inventory = np.mean([max(0, s['stock_level']) for s in stock_series])
        else:
            # Fallback calculation if no stock series
            total_in = sum(m.get('qty_in', 0) for m in movements)
            total_out = sum(m.get('qty_out', 0) for m in movements)
            stock_final = max(0, total_in - total_out)
            avg_inventory = stock_final / 2  # Simple approximation
        
        # Calculate weighted average cost
        total_cost_value = sum(c.get('cantidad', 0) * c.get('precio_unit', 0) for c in costs)
        total_cost_qty = sum(c.get('cantidad', 0) for c in costs)
        avg_cost = safe_divide(total_cost_value, total_cost_qty, 0.0)
        
        return {
            'total_compras': total_qty_in,  # Map to normalized field name
            'total_ventas': total_qty_out,  # Map to normalized field name
            'stock_promedio': avg_inventory,  # Map to normalized field name
            'costo_promedio': avg_cost,  # Map to normalized field name
            'stock_final_calculado': stock_final  # For internal calculations (not saved to DB)
        }
    
    def calculate_financial_metrics(self, basic_metrics: Dict, period_days: int) -> Dict:
        """
        Calculate financial metrics (COGS, rotation, DIO)
        
        Args:
            basic_metrics: Basic metrics dictionary
            period_days: Number of days in analysis period
            
        Returns:
            Dictionary with financial metrics
        """
        avg_cost = basic_metrics['costo_promedio']
        total_qty_out = basic_metrics['total_ventas']
        inventario_promedio = basic_metrics['stock_promedio']
        
        # Calculate COGS
        cogs = avg_cost * total_qty_out
        
        # Calculate inventory value
        valor_inventario = avg_cost * inventario_promedio
        
        # Calculate rotation (COGS / Average Inventory Value)
        rotacion = safe_divide(cogs, valor_inventario, 0.0)
        
        # Validate rotation for pharmacy context (reasonable range: 0.5 to 50 times per period)
        # For a typical period, this translates to reasonable annual rotation rates
        if rotacion > 50:
            logger.warning(f"Very high rotation detected: {rotacion:.2f} - may indicate data quality issues")
        elif rotacion > 0 and rotacion < 0.1:
            logger.info(f"Very low rotation detected: {rotacion:.2f} - possible slow-moving inventory")
        
        # Calculate DIO (Days Inventory Outstanding)
        daily_cogs = safe_divide(cogs, period_days, 0.0)
        dio = safe_divide(valor_inventario, daily_cogs, 0.0) if daily_cogs > 0 else float('inf')
        
        return {
            'cogs': cogs,
            'valor_inventario': valor_inventario,
            'rotacion': rotacion,
            'dio': dio if dio != float('inf') else 999.0  # Cap at 999 days
        }
    
    def calculate_demand_metrics(self, movements: List[Dict], period_days: int) -> Dict:
        """
        Calculate demand-related metrics
        
        Args:
            movements: List of daily movements
            period_days: Number of days in analysis period
            
        Returns:
            Dictionary with demand metrics
        """
        # Extract daily demand (qty_out)
        daily_demands = [m.get('qty_out', 0) or 0 for m in movements]
        
        total_demand = sum(daily_demands)
        avg_daily_demand = safe_divide(total_demand, period_days, 0.0)
        
        # Calculate standard deviation of daily demand
        if len(daily_demands) > 1:
            std_demand_daily = np.std(daily_demands, ddof=1)
        else:
            std_demand_daily = 0.0
        
        # Calculate coefficient of variation
        cv_demand = safe_divide(std_demand_daily, avg_daily_demand, 0.0)
        
        # Calculate final stock level from movements (compras - ventas acumuladas)
        total_in = sum(m.get('qty_in', 0) for m in movements)
        total_out = sum(m.get('qty_out', 0) for m in movements)
        final_stock = max(0, total_in - total_out)  # Stock no puede ser negativo
        
        coverage_days = safe_divide(final_stock, avg_daily_demand, float('inf'))
        coverage_days = min(coverage_days, 999.0)  # Cap at 999 days
        
        return {
            'avg_daily_demand': avg_daily_demand,
            'std_demand_daily': std_demand_daily,
            'cv_demand': cv_demand,
            'cobertura_dias': coverage_days  # Map to normalized field name
        }
    
    def calculate_reorder_metrics(self, demand_metrics: Dict) -> Dict:
        """
        Calculate reorder point and safety stock
        
        Args:
            demand_metrics: Demand metrics dictionary
            
        Returns:
            Dictionary with reorder metrics
        """
        avg_daily_demand = demand_metrics['avg_daily_demand']
        std_demand_daily = demand_metrics['std_demand_daily']
        
        # Get Z-score for service level
        z_score = self.get_z_score(self.service_level)
        
        # Calculate safety stock
        safety_stock = z_score * std_demand_daily * np.sqrt(self.lead_time_days)
        
        # Calculate reorder point
        rop = avg_daily_demand * self.lead_time_days + safety_stock
        
        return {
            'stock_seguridad': safety_stock,  # Map to normalized field name
            'rop': rop
        }
    
    def classify_abc(self, products_data: List[Dict]) -> Dict[str, str]:
        """
        Classify products using ABC analysis based on sales value
        
        Args:
            products_data: List of product data with sales values
            
        Returns:
            Dictionary mapping product_key to ABC class
        """
        if not products_data:
            return {}
        
        # Calculate sales value for each product
        for product in products_data:
            qty_out = product.get('total_ventas', 0)
            avg_cost = product.get('costo_promedio', 0)
            product['sales_value'] = qty_out * avg_cost
        
        # Sort by sales value descending
        sorted_products = sorted(products_data, key=lambda x: x['sales_value'], reverse=True)
        
        total_value = sum(p['sales_value'] for p in sorted_products)
        
        abc_classification = {}
        cumulative_value = 0
        
        for product in sorted_products:
            cumulative_value += product['sales_value']
            cumulative_percentage = safe_divide(cumulative_value, total_value, 0.0)
            
            product_key = product['nombre_clean']
            
            if cumulative_percentage <= 0.80:
                abc_classification[product_key] = 'A'
            elif cumulative_percentage <= 0.95:
                abc_classification[product_key] = 'B'
            else:
                abc_classification[product_key] = 'C'
        
        return abc_classification
    
    def classify_xyz(self, products_data: List[Dict], cv_thresholds: Tuple[float, float] = (0.5, 1.0)) -> Dict[str, str]:
        """
        Classify products using XYZ analysis based on demand variability
        
        Args:
            products_data: List of product data with CV values
            cv_thresholds: Tuple of (X_threshold, Y_threshold)
            
        Returns:
            Dictionary mapping product_key to XYZ class
        """
        xyz_classification = {}
        x_threshold, y_threshold = cv_thresholds
        
        for product in products_data:
            cv_demand = product.get('cv_demand', 0)
            product_key = product['nombre_clean']
            
            if cv_demand <= x_threshold:
                xyz_classification[product_key] = 'X'
            elif cv_demand <= y_threshold:
                xyz_classification[product_key] = 'Y'
            else:
                xyz_classification[product_key] = 'Z'
        
        return xyz_classification
    
    def calculate_flags(self, metrics: Dict, excess_threshold: int = 45, shortage_threshold: int = 7) -> Dict:
        """
        Calculate operational flags
        
        Args:
            metrics: Product metrics dictionary
            excess_threshold: Days threshold for excess flag
            shortage_threshold: Days threshold for shortage flag
            
        Returns:
            Dictionary with flag values
        """
        coverage_days = metrics.get('cobertura_dias', 0)
        stock_final = metrics.get('stock_final_calculado', 0)
        rop = metrics.get('rop', 0)
        
        exceso = 1 if coverage_days > excess_threshold else 0
        faltante = 1 if (stock_final < rop or coverage_days < shortage_threshold) else 0
        
        return {
            'exceso': exceso,  # Map to normalized field name
            'faltante': faltante  # Map to normalized field name
        }

def calculate_kpis(start_date: date, end_date: date, **kwargs) -> None:
    """
    Calculate KPIs for all products in the specified date range
    
    Args:
        start_date: Start date for analysis
        end_date: End date for analysis
        **kwargs: Additional parameters (service_level, lead_time_days, etc.)
    """
    calculator = KpiCalculator(
        service_level=kwargs.get('service_level', 0.95),
        lead_time_days=kwargs.get('lead_time_days', 7)
    )
    
    period_days = (end_date - start_date).days + 1
    
    with DatabaseSession() as session:
        try:
            # Clear ALL existing KPIs to avoid duplicates
            deleted_count = session.query(ProductoKpis).delete()
            logger.info(f"Cleared {deleted_count} existing KPI records")
            
            # Get unique products from daily movements (using normalized table)
            products = session.execute(text("""
                SELECT DISTINCT nombre_clean
                FROM kpi_mov_diario_normalized
                WHERE fecha BETWEEN :start_date AND :end_date
                    AND (qty_in > 0 OR qty_out > 0)
                    AND nombre_clean IS NOT NULL
                    AND nombre_clean != ''
                ORDER BY nombre_clean
            """), {'start_date': start_date, 'end_date': end_date}).fetchall()
            
            all_products_data = []
            
            for product in products:
                nombre_clean = product.nombre_clean
                
                # Get CABYS from the first occurrence (for reference)
                cabys_result = session.execute(text("""
                    SELECT cabys FROM kpi_mov_diario_normalized
                    WHERE nombre_clean = :nombre_clean
                        AND cabys IS NOT NULL AND cabys != ''
                    LIMIT 1
                """), {'nombre_clean': nombre_clean}).fetchone()
                
                cabys = cabys_result.cabys if cabys_result else ''
                
                # Get movements for this product (using only nombre_clean)
                movements = session.execute(text("""
                    SELECT fecha, qty_in, qty_out
                    FROM kpi_mov_diario_normalized
                    WHERE nombre_clean = :nombre_clean
                        AND fecha BETWEEN :start_date AND :end_date
                    ORDER BY fecha
                """), {
                    'nombre_clean': nombre_clean,
                    'start_date': start_date,
                    'end_date': end_date
                }).fetchall()
                    
                if not movements:
                    continue
                    
                # Get cost data (using normalized table)
                costs = session.execute(text("""
                    SELECT AVG(cn.costo) as avg_cost, AVG(cn.precio_unit) as avg_price
                    FROM compras_normalized cn
                    WHERE cn.nombre_clean = :nombre_clean
                        AND cn.costo > 0
                """), {'nombre_clean': nombre_clean}).fetchone()
                
                # Convert to dictionaries
                movements_dict = [
                    {
                        'fecha': m.fecha,
                        'qty_in': m.qty_in or 0,
                        'qty_out': m.qty_out or 0
                    }
                    for m in movements
                ]
                
                # Handle costs data (single result with averages)
                if costs:
                    costs_dict = [{
                        'cantidad': 1,  # Use 1 as weight for average calculation
                        'precio_unit': costs.avg_cost or 0
                    }]
                else:
                    costs_dict = [{
                        'cantidad': 1,
                        'precio_unit': 0
                    }]

                # Calculate metrics
                basic_metrics = calculator.calculate_basic_metrics(movements_dict, costs_dict)

                # Note: No fraction correction needed here because kpi_mov_diario_normalized 
                # already contains qty_normalizada (quantities converted to complete units)
                # The data is already normalized at the source level

                financial_metrics = calculator.calculate_financial_metrics(basic_metrics, period_days)
                demand_metrics = calculator.calculate_demand_metrics(movements_dict, period_days)
                reorder_metrics = calculator.calculate_reorder_metrics(demand_metrics)
                flags = calculator.calculate_flags(
                    {**basic_metrics, **demand_metrics, **reorder_metrics},
                    kwargs.get('excess_threshold', 45),
                    kwargs.get('shortage_threshold', 7)
                )

                # Combine all metrics
                all_metrics = {
                    'cabys': cabys,
                    'nombre_clean': nombre_clean,
                    'fecha_inicio': start_date,
                    'fecha_fin': end_date,
                    **basic_metrics,
                    **financial_metrics,
                    **demand_metrics,
                    **reorder_metrics,
                    **flags
                }
                
                all_products_data.append(all_metrics)
            
            # Calculate ABC/XYZ classifications
            abc_classification = calculator.classify_abc(all_products_data)
            xyz_classification = calculator.classify_xyz(all_products_data)
            
            # Add classifications to metrics and save to database
            for metrics in all_products_data:
                # Add classifications (map to normalized field names)
                metrics['clasificacion_abc'] = abc_classification.get(metrics['nombre_clean'], 'C')
                metrics['clasificacion_xyz'] = xyz_classification.get(metrics['nombre_clean'], 'Z')
                
                # Filter only valid ProductoKpis fields (exclude calculated fields)
                valid_fields = {
                    'cabys', 'nombre_clean', 'total_compras', 'total_ventas', 
                    'stock_promedio', 'costo_promedio', 'precio_promedio',
                    'rotacion', 'dio', 'rop', 'stock_seguridad', 'cobertura_dias',
                    'exceso', 'faltante', 'clasificacion_abc', 'clasificacion_xyz',
                    'fecha_inicio', 'fecha_fin'
                }
                
                # Exclude internal calculation fields like 'stock_final_calculado'
                clean_metrics = {k: v for k, v in metrics.items() if k in valid_fields}
                
                # Create and save KPI record
                kpi_record = ProductoKpis(**clean_metrics)
                session.add(kpi_record)
            
            session.commit()
            logger.info(f"Successfully calculated and saved KPIs for {len(all_products_data)} products")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Error calculating KPIs: {e}")
            raise e

def calculate_abc_xyz(products_data: List[Dict]) -> Tuple[Dict, Dict]:
    """
    Calculate ABC and XYZ classifications for a list of products
    
    Args:
        products_data: List of product data dictionaries
        
    Returns:
        Tuple of (abc_classification, xyz_classification) dictionaries
    """
    calculator = KpiCalculator()
    abc_classification = calculator.classify_abc(products_data)
    xyz_classification = calculator.classify_xyz(products_data)
    
    return abc_classification, xyz_classification

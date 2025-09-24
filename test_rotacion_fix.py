#!/usr/bin/env python3
"""
Test script to verify the rotation calculation fix
Compares rotation using stock_final vs stock_promedio
"""

from db.database import get_session
from sqlalchemy import text

def test_rotation_fix():
    """Test the rotation calculation fix"""
    
    with get_session() as session:
        print("=== ANÁLISIS DEL PROBLEMA DE ROTACIÓN ===\n")
        
        # Compare rotation calculations
        result = session.execute(text("""
            SELECT 
                nombre_clean,
                total_ventas,
                costo_promedio,
                stock_promedio,
                stock_final,
                (total_ventas * costo_promedio) as cogs,
                (stock_final * costo_promedio) as valor_inventario_final,
                (stock_promedio * costo_promedio) as valor_inventario_promedio,
                CASE 
                    WHEN (stock_final * costo_promedio) > 0 
                    THEN (total_ventas * costo_promedio) / (stock_final * costo_promedio)
                    ELSE 0 
                END as rotacion_con_stock_final,
                CASE 
                    WHEN (stock_promedio * costo_promedio) > 0 
                    THEN (total_ventas * costo_promedio) / (stock_promedio * costo_promedio)
                    ELSE 0 
                END as rotacion_con_stock_promedio,
                rotacion as rotacion_actual_bd
            FROM producto_kpis
            WHERE fecha_inicio IS NOT NULL
            AND total_ventas > 0
            ORDER BY total_ventas DESC
            LIMIT 10
        """)).fetchall()
        
        print("TOP 10 PRODUCTOS CON VENTAS - COMPARACIÓN DE MÉTODOS:")
        print("-" * 100)
        
        for row in result:
            print(f"Producto: {row.nombre_clean}")
            print(f"  Ventas: {row.total_ventas:.1f}")
            print(f"  COGS: ₡{row.cogs:,.0f}")
            print(f"  Stock Promedio: {row.stock_promedio:.1f}")
            print(f"  Stock Final: {row.stock_final:.1f}")
            print(f"  Valor Inv. Promedio: ₡{row.valor_inventario_promedio:,.0f}")
            print(f"  Valor Inv. Final: ₡{row.valor_inventario_final:,.0f}")
            print(f"  Rotación (Stock Final): {row.rotacion_con_stock_final:.2f}")
            print(f"  Rotación (Stock Promedio): {row.rotacion_con_stock_promedio:.2f}")
            print(f"  Rotación BD Actual: {row.rotacion_actual_bd:.2f}")
            print()
        
        # Summary statistics
        summary = session.execute(text("""
            SELECT 
                COUNT(*) as total_productos,
                SUM(CASE WHEN rotacion = 0 THEN 1 ELSE 0 END) as productos_rotacion_cero,
                SUM(CASE WHEN total_ventas > 0 AND rotacion = 0 THEN 1 ELSE 0 END) as con_ventas_sin_rotacion,
                SUM(CASE WHEN total_ventas > 0 AND stock_promedio > 0 THEN 1 ELSE 0 END) as productos_que_deberian_rotar
            FROM producto_kpis 
            WHERE fecha_inicio IS NOT NULL
        """)).fetchone()
        
        print("=== RESUMEN DEL PROBLEMA ACTUAL ===")
        print(f"Total productos: {summary.total_productos}")
        print(f"Productos con rotación = 0: {summary.productos_rotacion_cero}")
        print(f"Productos con ventas pero rotación = 0: {summary.con_ventas_sin_rotacion}")
        print(f"Productos que deberían tener rotación > 0: {summary.productos_que_deberian_rotar}")
        
        problema_pct = (summary.con_ventas_sin_rotacion / summary.productos_que_deberian_rotar) * 100
        print(f"% de productos con problema: {problema_pct:.1f}%")
        
        print("\n=== IMPACTO DE LA CORRECCIÓN PROPUESTA ===")
        
        # Simulate the fix
        productos_corregidos = session.execute(text("""
            SELECT 
                COUNT(*) as productos_que_tendran_rotacion
            FROM producto_kpis 
            WHERE fecha_inicio IS NOT NULL
            AND total_ventas > 0 
            AND stock_promedio > 0
            AND (stock_promedio * costo_promedio) > 0
        """)).fetchone()
        
        print(f"Productos que tendrán rotación > 0 después de la corrección: {productos_corregidos.productos_que_tendran_rotacion}")
        
        mejora = productos_corregidos.productos_que_tendran_rotacion - (summary.total_productos - summary.con_ventas_sin_rotacion)
        print(f"Mejora esperada: +{mejora} productos con rotación válida")

if __name__ == "__main__":
    test_rotation_fix()

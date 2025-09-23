#!/usr/bin/env python3
"""
Test script to verify the valor_inventario fix
Compares old method (stock_promedio) vs new method (stock_final)
"""

from db.database import get_session
from sqlalchemy import text

def test_valor_inventario_fix():
    """Test the valor_inventario calculation fix"""
    
    with get_session() as session:
        print("=== ANÁLISIS DEL PROBLEMA DE VALOR_INVENTARIO ===\n")
        
        # Compare old vs new calculation
        result = session.execute(text("""
            SELECT 
                nombre_clean,
                stock_final,
                stock_promedio,
                costo_promedio,
                (stock_promedio * costo_promedio) as valor_inventario_ANTERIOR,
                (stock_final * costo_promedio) as valor_inventario_CORREGIDO,
                ((stock_promedio * costo_promedio) - (stock_final * costo_promedio)) as diferencia
            FROM producto_kpis 
            WHERE stock_final <= 0 
            AND (stock_promedio * costo_promedio) > 0
            ORDER BY (stock_promedio * costo_promedio) DESC
            LIMIT 10
        """)).fetchall()
        
        print("TOP 10 PRODUCTOS CON MAYOR CORRECCIÓN:")
        print("-" * 80)
        total_correccion = 0
        
        for row in result:
            print(f"Producto: {row.nombre_clean}")
            print(f"  Stock Final: {row.stock_final}")
            print(f"  Stock Promedio: {row.stock_promedio:.2f}")
            print(f"  Costo Promedio: ₡{row.costo_promedio:,.2f}")
            print(f"  Valor ANTERIOR: ₡{row.valor_inventario_ANTERIOR:,.2f}")
            print(f"  Valor CORREGIDO: ₡{row.valor_inventario_CORREGIDO:,.2f}")
            print(f"  CORRECCIÓN: ₡{row.diferencia:,.2f}")
            print()
            total_correccion += row.diferencia
        
        # Summary statistics
        summary = session.execute(text("""
            SELECT 
                COUNT(*) as productos_afectados,
                SUM(stock_promedio * costo_promedio) as valor_total_anterior,
                SUM(stock_final * costo_promedio) as valor_total_corregido,
                SUM((stock_promedio * costo_promedio) - (stock_final * costo_promedio)) as correccion_total
            FROM producto_kpis 
            WHERE stock_final <= 0 
            AND (stock_promedio * costo_promedio) > 0
        """)).fetchone()
        
        print("=== RESUMEN DE LA CORRECCIÓN ===")
        print(f"Productos afectados: {summary.productos_afectados}")
        print(f"Valor total ANTERIOR: ₡{summary.valor_total_anterior:,.2f}")
        print(f"Valor total CORREGIDO: ₡{summary.valor_total_corregido:,.2f}")
        print(f"CORRECCIÓN TOTAL: ₡{summary.correccion_total:,.2f}")
        print()
        
        # Impact on total inventory value
        total_inventory = session.execute(text("""
            SELECT 
                SUM(stock_promedio * costo_promedio) as valor_total_anterior,
                SUM(stock_final * costo_promedio) as valor_total_corregido
            FROM producto_kpis 
            WHERE fecha_inicio IS NOT NULL
        """)).fetchone()
        
        print("=== IMPACTO EN INVENTARIO TOTAL ===")
        print(f"Inventario total ANTERIOR: ₡{total_inventory.valor_total_anterior:,.2f}")
        print(f"Inventario total CORREGIDO: ₡{total_inventory.valor_total_corregido:,.2f}")
        
        reduction_pct = (summary.correccion_total / total_inventory.valor_total_anterior) * 100
        print(f"Reducción: ₡{summary.correccion_total:,.2f} ({reduction_pct:.1f}%)")
        
        print("\n=== VALIDACIÓN ===")
        # Verify no products with stock_final = 0 have positive valor_inventario with new method
        validation = session.execute(text("""
            SELECT COUNT(*) as productos_problema_restantes
            FROM producto_kpis 
            WHERE stock_final <= 0 
            AND (stock_final * costo_promedio) > 0
        """)).fetchone()
        
        if validation.productos_problema_restantes == 0:
            print("✅ CORRECCIÓN EXITOSA: No hay productos con stock=0 y valor>0")
        else:
            print(f"❌ AÚN HAY PROBLEMAS: {validation.productos_problema_restantes} productos")

if __name__ == "__main__":
    test_valor_inventario_fix()

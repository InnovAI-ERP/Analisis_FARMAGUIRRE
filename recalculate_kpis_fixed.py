#!/usr/bin/env python3
"""
Script to recalculate KPIs with the corrected valor_inventario formula
This will apply the fix: valor_inventario = costo_promedio × stock_final
"""

from datetime import date
from utils.kpi_fixed import calculate_kpis_fixed
from db.database import get_session
from sqlalchemy import text
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def recalculate_kpis_with_fix():
    """Recalculate KPIs with the corrected valor_inventario formula"""
    
    print("=== RECALCULANDO KPIs CON FÓRMULA CORREGIDA ===\n")
    
    # Check current state
    with get_session() as session:
        before = session.execute(text("""
            SELECT 
                COUNT(*) as productos_problema,
                SUM(stock_promedio * costo_promedio) as valor_fantasma_anterior
            FROM producto_kpis 
            WHERE stock_final <= 0 
            AND (stock_promedio * costo_promedio) > 0
        """)).fetchone()
        
        print(f"ANTES DE LA CORRECCIÓN:")
        print(f"- Productos con inventario fantasma: {before.productos_problema}")
        print(f"- Valor fantasma: ₡{before.valor_fantasma_anterior:,.2f}")
        print()
    
    # Recalculate KPIs for 2025 (adjust dates as needed)
    start_date = date(2025, 1, 1)
    end_date = date.today()
    
    print(f"Recalculando KPIs desde {start_date} hasta {end_date}...")
    print("Esto puede tomar unos minutos...\n")
    
    try:
        # Use the fixed KPI calculation function
        calculate_kpis_fixed(
            start_date=start_date,
            end_date=end_date,
            service_level=0.95,
            lead_time_days=7,
            excess_threshold=45,
            shortage_threshold=7
        )
        
        print("✅ KPIs recalculados exitosamente con fórmula corregida!\n")
        
        # Check results after recalculation
        with get_session() as session:
            after = session.execute(text("""
                SELECT 
                    COUNT(*) as productos_problema,
                    SUM(stock_final * costo_promedio) as valor_corregido
                FROM producto_kpis 
                WHERE stock_final <= 0 
                AND (stock_final * costo_promedio) > 0
            """)).fetchone()
            
            total_inventory = session.execute(text("""
                SELECT 
                    COUNT(*) as total_productos,
                    SUM(stock_final * costo_promedio) as valor_total_inventario
                FROM producto_kpis 
                WHERE fecha_inicio IS NOT NULL
            """)).fetchone()
            
            print("DESPUÉS DE LA CORRECCIÓN:")
            print(f"- Productos con inventario fantasma: {after.productos_problema}")
            print(f"- Valor fantasma: ₡{after.valor_corregido:,.2f}")
            print(f"- Total productos: {total_inventory.total_productos}")
            print(f"- Valor total inventario: ₡{total_inventory.valor_total_inventario:,.2f}")
            
            if after.productos_problema == 0:
                print("\n✅ CORRECCIÓN EXITOSA: No hay más inventario fantasma")
                print("✅ El Excel de datos limpios ahora mostrará valores correctos")
            else:
                print(f"\n❌ AÚN HAY PROBLEMAS: {after.productos_problema} productos")
                
    except Exception as e:
        logger.error(f"Error recalculando KPIs: {e}")
        print(f"❌ Error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    success = recalculate_kpis_with_fix()
    if success:
        print("\n🎉 ¡Recálculo completado! Ahora puedes exportar datos limpios con valores corregidos.")
    else:
        print("\n❌ Hubo errores en el recálculo. Revisa los logs.")

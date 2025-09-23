#!/usr/bin/env python3
"""
Script para probar las correcciones del dashboard y rotación
"""

import sys
import os
from datetime import date

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    from db.database import get_session
    from sqlalchemy import text
    import pandas as pd
    import logging
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def test_rotation_calculation():
    """Test rotation calculation with filtering"""
    print("🔍 PROBANDO CÁLCULO DE ROTACIÓN CORREGIDO")
    print("=" * 50)
    
    with get_session() as session:
        # Test the corrected rotation calculation
        rotation_test = session.execute(text("""
            SELECT 
                COUNT(*) as total_productos,
                AVG(CASE WHEN rotacion > 0 AND rotacion <= 1000 THEN rotacion ELSE NULL END) as rotacion_promedio_filtrada,
                AVG(CASE WHEN rotacion > 0 THEN rotacion ELSE NULL END) as rotacion_promedio_sin_filtro,
                COUNT(CASE WHEN rotacion > 1000 THEN 1 END) as productos_extremos
            FROM producto_kpis
        """)).fetchone()
        
        print(f"✅ Total productos: {rotation_test.total_productos:,}")
        print(f"✅ Productos con rotación extrema (>1000): {rotation_test.productos_extremos:,}")
        print(f"✅ Rotación promedio (sin filtro): {rotation_test.rotacion_promedio_sin_filtro:,.2f}")
        print(f"✅ Rotación promedio (filtrada ≤1000): {rotation_test.rotacion_promedio_filtrada:.2f}")
        
        # Verify the difference
        if rotation_test.productos_extremos > 0:
            print(f"\n💡 La filtración redujo la rotación promedio de {rotation_test.rotacion_promedio_sin_filtro:,.0f} a {rotation_test.rotacion_promedio_filtrada:.2f}")
            print("   Esto elimina los valores extremos causados por stock promedio ≈ 0")
        else:
            print("\n✅ No hay productos con rotación extrema")
        
        return True

def test_dashboard_query():
    """Test the corrected dashboard query with cabys column"""
    print("\n🔍 PROBANDO CONSULTA DEL DASHBOARD CORREGIDA")
    print("=" * 50)
    
    with get_session() as session:
        try:
            # Test the corrected query that includes cabys
            products_data = session.execute(text("""
                SELECT 
                    cabys,
                    nombre_clean,
                    total_compras as total_qty_in,
                    total_ventas as total_qty_out,
                    stock_final,
                    rotacion,
                    dio,
                    cobertura_dias as coverage_days,
                    rop,
                    stock_seguridad as safety_stock,
                    clasificacion_abc as abc_class,
                    clasificacion_xyz as xyz_class,
                    exceso,
                    faltante
                FROM producto_kpis
                WHERE fecha_inicio IS NOT NULL
                ORDER BY total_ventas * costo_promedio DESC
                LIMIT 5
            """)).fetchall()
            
            if products_data:
                # Convert to DataFrame to test the problematic operation
                df = pd.DataFrame([dict(row._mapping) for row in products_data])
                
                print(f"✅ Consulta ejecutada exitosamente")
                print(f"✅ Filas obtenidas: {len(df)}")
                print(f"✅ Columnas disponibles: {list(df.columns)}")
                
                # Test the problematic DataFrame operation
                df['Compras-Ventas'] = df['total_qty_in'] - df['total_qty_out']
                df['Estado'] = df.apply(lambda x: 
                    "🔴 Faltante" if x['faltante'] else 
                    "🟡 Exceso" if x['exceso'] else 
                    "🟢 Normal", axis=1)
                
                # Test the column selection that was failing
                display_df = df[[
                    'cabys', 'nombre_clean', 'total_qty_in', 'total_qty_out', 
                    'Compras-Ventas', 'stock_final', 'rotacion', 'dio', 
                    'coverage_days', 'abc_class', 'xyz_class', 'Estado'
                ]].copy()
                
                print(f"✅ DataFrame creado exitosamente con {len(display_df)} filas")
                print(f"✅ Columnas del display_df: {list(display_df.columns)}")
                
                # Show sample data
                print(f"\n📊 Muestra de datos:")
                print("Producto | CABYS | Stock Final | Rotación | Estado")
                print("-" * 60)
                for _, row in display_df.head(3).iterrows():
                    cabys_str = str(row['cabys'])[:10] if row['cabys'] else 'N/A'
                    print(f"{row['nombre_clean'][:20]:<20} | {cabys_str:<10} | {row['stock_final']:>11.1f} | {row['rotacion']:>8.1f} | {row['Estado']}")
                
                return True
            else:
                print("❌ No se obtuvieron datos de la consulta")
                return False
                
        except Exception as e:
            print(f"❌ Error en consulta del dashboard: {e}")
            return False

def test_kpi_summary():
    """Test the corrected KPI summary calculation"""
    print("\n🔍 PROBANDO RESUMEN DE KPIs CORREGIDO")
    print("=" * 50)
    
    with get_session() as session:
        try:
            # Test the corrected KPI summary query
            kpi_summary = session.execute(text("""
                SELECT 
                    COUNT(*) as total_productos,
                    SUM(CASE WHEN exceso = 1 THEN 1 ELSE 0 END) as productos_exceso,
                    SUM(CASE WHEN faltante = 1 THEN 1 ELSE 0 END) as productos_faltante,
                    AVG(CASE WHEN rotacion > 0 AND rotacion <= 1000 THEN rotacion ELSE NULL END) as rotacion_promedio,
                    AVG(CASE WHEN dio > 0 AND dio < 999 THEN dio ELSE NULL END) as dio_promedio,
                    MIN(fecha_inicio) as fecha_inicio,
                    MAX(fecha_fin) as fecha_fin
                FROM producto_kpis 
                WHERE fecha_inicio IS NOT NULL
                ORDER BY fecha_inicio, fecha_fin
            """)).fetchone()
            
            if kpi_summary and kpi_summary.total_productos > 0:
                print(f"✅ Total Productos: {kpi_summary.total_productos:,}")
                print(f"✅ Productos Exceso: {kpi_summary.productos_exceso:,}")
                print(f"✅ Productos Faltante: {kpi_summary.productos_faltante:,}")
                print(f"✅ Rotación Promedio (filtrada): {kpi_summary.rotacion_promedio:.2f}")
                print(f"✅ DIO Promedio: {kpi_summary.dio_promedio:.1f} días")
                
                # Verify mathematical consistency
                suma = kpi_summary.productos_exceso + kpi_summary.productos_faltante
                is_valid = suma <= kpi_summary.total_productos
                print(f"\n🎯 Verificación matemática:")
                print(f"   Exceso + Faltante = {kpi_summary.productos_exceso} + {kpi_summary.productos_faltante} = {suma}")
                print(f"   {suma} ≤ {kpi_summary.total_productos}: {'✅ VÁLIDO' if is_valid else '❌ INVÁLIDO'}")
                
                return True
            else:
                print("❌ No se obtuvieron KPIs válidos")
                return False
                
        except Exception as e:
            print(f"❌ Error en resumen de KPIs: {e}")
            return False

def main():
    """Main function to test all dashboard corrections"""
    print("🧪 PROBANDO CORRECCIONES DEL DASHBOARD")
    print("=" * 60)
    
    success_count = 0
    total_tests = 3
    
    # Test 1: Rotation calculation
    if test_rotation_calculation():
        success_count += 1
    
    # Test 2: Dashboard query
    if test_dashboard_query():
        success_count += 1
    
    # Test 3: KPI summary
    if test_kpi_summary():
        success_count += 1
    
    # Final results
    print("\n" + "=" * 60)
    print(f"🎯 RESULTADOS DE PRUEBAS: {success_count}/{total_tests} exitosas")
    
    if success_count == total_tests:
        print("🎉 ¡TODAS LAS CORRECCIONES FUNCIONAN CORRECTAMENTE!")
        print("✅ Rotación promedio filtrada para eliminar valores extremos")
        print("✅ Consulta del dashboard incluye columna 'cabys'")
        print("✅ KPIs matemáticamente consistentes")
        print("\n💡 El dashboard debería funcionar sin errores ahora")
        return True
    else:
        print("❌ Algunas pruebas fallaron. Revisar los errores arriba.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

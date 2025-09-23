#!/usr/bin/env python3
"""
Script de prueba para verificar que los cambios determinísticos funcionan correctamente
Ejecuta múltiples veces el mismo cálculo y verifica que los resultados sean idénticos
"""

import sys
import os
from datetime import date, timedelta
import json
import hashlib
from pathlib import Path

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    from db.database import get_session, init_database
    from utils.kpi_fixed import calculate_kpis_fixed
    from etl.hybrid_normalized_loader_fixed import create_daily_aggregates_normalized_fixed
    from sqlalchemy import text
    import logging
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_kpi_results():
    """Get current KPI results from database"""
    with get_session() as session:
        try:
            # Get summary statistics
            kpi_summary = session.execute(text("""
                SELECT 
                    COUNT(*) as total_productos,
                    SUM(CASE WHEN exceso = 1 THEN 1 ELSE 0 END) as productos_exceso,
                    SUM(CASE WHEN faltante = 1 THEN 1 ELSE 0 END) as productos_faltante,
                    AVG(CASE WHEN rotacion > 0 THEN rotacion ELSE NULL END) as rotacion_promedio,
                    AVG(CASE WHEN dio > 0 AND dio < 999 THEN dio ELSE NULL END) as dio_promedio,
                    MIN(fecha_inicio) as fecha_inicio,
                    MAX(fecha_fin) as fecha_fin
                FROM producto_kpis 
                WHERE fecha_inicio IS NOT NULL
                ORDER BY fecha_inicio, fecha_fin
            """)).fetchone()
            
            if not kpi_summary or kpi_summary.total_productos == 0:
                return None
            
            # Get detailed product data for comparison
            products_data = session.execute(text("""
                SELECT 
                    nombre_clean,
                    total_compras,
                    total_ventas,
                    stock_promedio,
                    rotacion,
                    dio,
                    cobertura_dias,
                    exceso,
                    faltante,
                    clasificacion_abc,
                    clasificacion_xyz
                FROM producto_kpis
                WHERE fecha_inicio IS NOT NULL
                ORDER BY nombre_clean  -- Deterministic order
                LIMIT 10  -- Just first 10 for testing
            """)).fetchall()
            
            return {
                'summary': {
                    'total_productos': kpi_summary.total_productos,
                    'productos_exceso': kpi_summary.productos_exceso,
                    'productos_faltante': kpi_summary.productos_faltante,
                    'rotacion_promedio': float(kpi_summary.rotacion_promedio) if kpi_summary.rotacion_promedio else 0.0,
                    'dio_promedio': float(kpi_summary.dio_promedio) if kpi_summary.dio_promedio else 0.0
                },
                'products': [
                    {
                        'nombre_clean': p.nombre_clean,
                        'total_compras': float(p.total_compras),
                        'total_ventas': float(p.total_ventas),
                        'stock_promedio': float(p.stock_promedio),
                        'rotacion': float(p.rotacion),
                        'dio': float(p.dio),
                        'cobertura_dias': float(p.cobertura_dias),
                        'exceso': p.exceso,
                        'faltante': p.faltante,
                        'clasificacion_abc': p.clasificacion_abc,
                        'clasificacion_xyz': p.clasificacion_xyz
                    }
                    for p in products_data
                ]
            }
        except Exception as e:
            logger.error(f"Error getting KPI results: {e}")
            return None

def calculate_hash(data):
    """Calculate hash of data for comparison"""
    if data is None:
        return None
    json_str = json.dumps(data, sort_keys=True, default=str)
    return hashlib.md5(json_str.encode()).hexdigest()

def test_deterministic_kpis():
    """Test that KPI calculations are deterministic"""
    print("🧪 INICIANDO PRUEBA DE DETERMINISMO")
    print("=" * 50)
    
    # Initialize database
    try:
        init_database()
        print("✅ Base de datos inicializada")
    except Exception as e:
        print(f"❌ Error inicializando base de datos: {e}")
        return False
    
    # Check if we have data to work with
    with get_session() as session:
        try:
            compras_count = session.execute(text("SELECT COUNT(*) FROM compras_normalized")).scalar()
            ventas_count = session.execute(text("SELECT COUNT(*) FROM ventas_normalized")).scalar()
            
            if compras_count == 0 or ventas_count == 0:
                print("⚠️ No hay datos en las tablas normalizadas")
                print("   Por favor, carga datos primero usando la aplicación Streamlit")
                return False
                
            print(f"📊 Datos disponibles: {compras_count} compras, {ventas_count} ventas")
            
        except Exception as e:
            print(f"❌ Error verificando datos: {e}")
            return False
    
    # Test parameters
    start_date = date(2025, 1, 1)
    end_date = date.today()
    test_runs = 3
    
    print(f"📅 Período de prueba: {start_date} a {end_date}")
    print(f"🔄 Número de ejecuciones: {test_runs}")
    print()
    
    results = []
    hashes = []
    
    for run in range(1, test_runs + 1):
        print(f"🔄 Ejecutando prueba {run}/{test_runs}...")
        
        try:
            # Recreate daily aggregates
            create_daily_aggregates_normalized_fixed(start_date, end_date)
            
            # Calculate KPIs
            calculate_kpis_fixed(
                start_date, 
                end_date,
                service_level=0.95,
                lead_time_days=7,
                excess_threshold=45,
                shortage_threshold=7
            )
            
            # Get results
            result = get_kpi_results()
            if result is None:
                print(f"❌ No se pudieron obtener resultados en la ejecución {run}")
                return False
            
            results.append(result)
            hash_value = calculate_hash(result)
            hashes.append(hash_value)
            
            print(f"   ✅ Ejecución {run} completada")
            print(f"   📊 Total productos: {result['summary']['total_productos']}")
            print(f"   🔢 Hash: {hash_value[:16]}...")
            print()
            
        except Exception as e:
            print(f"❌ Error en ejecución {run}: {e}")
            return False
    
    # Compare results
    print("🔍 VERIFICANDO DETERMINISMO")
    print("=" * 50)
    
    # Check if all hashes are identical
    unique_hashes = set(hashes)
    if len(unique_hashes) == 1:
        print("✅ ¡ÉXITO! Todos los resultados son idénticos")
        print(f"🔢 Hash único: {hashes[0]}")
        
        # Show detailed comparison
        base_result = results[0]
        print(f"📊 Resumen de resultados consistentes:")
        print(f"   • Total productos: {base_result['summary']['total_productos']}")
        print(f"   • Productos exceso: {base_result['summary']['productos_exceso']}")
        print(f"   • Productos faltante: {base_result['summary']['productos_faltante']}")
        print(f"   • Rotación promedio: {base_result['summary']['rotacion_promedio']:.6f}")
        print(f"   • DIO promedio: {base_result['summary']['dio_promedio']:.6f}")
        
        return True
        
    else:
        print("❌ ¡FALLO! Los resultados no son determinísticos")
        print(f"🔢 Se encontraron {len(unique_hashes)} hashes diferentes:")
        for i, hash_val in enumerate(hashes):
            print(f"   Ejecución {i+1}: {hash_val}")
        
        # Show differences in summary
        print("\n📊 Diferencias en resumen:")
        for i, result in enumerate(results):
            print(f"   Ejecución {i+1}:")
            print(f"     Total productos: {result['summary']['total_productos']}")
            print(f"     Rotación promedio: {result['summary']['rotacion_promedio']:.6f}")
            print(f"     DIO promedio: {result['summary']['dio_promedio']:.6f}")
        
        return False

if __name__ == "__main__":
    success = test_deterministic_kpis()
    
    print("\n" + "=" * 50)
    if success:
        print("🎉 ¡PRUEBA EXITOSA! El sistema es completamente determinístico")
        print("✅ Los mismos datos siempre producen los mismos resultados")
    else:
        print("❌ PRUEBA FALLIDA: El sistema aún tiene problemas de determinismo")
        print("🔧 Se requieren más correcciones")
    
    sys.exit(0 if success else 1)

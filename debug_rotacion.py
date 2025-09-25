#!/usr/bin/env python3
"""
Script para analizar la distribución de rotación y identificar el problema
"""

import sys
import os
import pandas as pd
from sqlalchemy import text

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from db.database import get_session

def analyze_rotation_distribution():
    """Analyze the rotation distribution in the database"""
    
    print("🔍 Analizando distribución de rotación en la base de datos...")
    
    with get_session() as session:
        # Get rotation distribution statistics
        query = text("""
            SELECT 
                COUNT(*) as total_productos,
                COUNT(CASE WHEN rotacion = 0 THEN 1 END) as rotacion_cero,
                COUNT(CASE WHEN rotacion > 0 AND rotacion <= 1 THEN 1 END) as rotacion_muy_baja,
                COUNT(CASE WHEN rotacion > 1 AND rotacion <= 5 THEN 1 END) as rotacion_baja,
                COUNT(CASE WHEN rotacion > 5 AND rotacion <= 20 THEN 1 END) as rotacion_media,
                COUNT(CASE WHEN rotacion > 20 AND rotacion <= 100 THEN 1 END) as rotacion_alta,
                COUNT(CASE WHEN rotacion > 100 AND rotacion <= 1000 THEN 1 END) as rotacion_muy_alta,
                COUNT(CASE WHEN rotacion > 1000 THEN 1 END) as rotacion_extrema,
                COUNT(CASE WHEN rotacion IS NULL THEN 1 END) as rotacion_null,
                AVG(rotacion) as rotacion_promedio_sin_filtro,
                AVG(CASE WHEN rotacion > 0 AND rotacion <= 1000 THEN rotacion END) as rotacion_promedio_filtrada,
                MIN(rotacion) as rotacion_min,
                MAX(rotacion) as rotacion_max,
                -- stddev not available in SQLite
                0 as rotacion_stddev
            FROM producto_kpis 
            WHERE fecha_inicio IS NOT NULL
        """)
        
        result = session.execute(query).fetchone()
        
        if result:
            print(f"\n📊 ESTADÍSTICAS DE ROTACIÓN:")
            print(f"Total productos: {result.total_productos:,}")
            print(f"Rotación = 0: {result.rotacion_cero:,} ({result.rotacion_cero/result.total_productos*100:.1f}%)")
            print(f"Rotación muy baja (0-1]: {result.rotacion_muy_baja:,} ({result.rotacion_muy_baja/result.total_productos*100:.1f}%)")
            print(f"Rotación baja (1-5]: {result.rotacion_baja:,} ({result.rotacion_baja/result.total_productos*100:.1f}%)")
            print(f"Rotación media (5-20]: {result.rotacion_media:,} ({result.rotacion_media/result.total_productos*100:.1f}%)")
            print(f"Rotación alta (20-100]: {result.rotacion_alta:,} ({result.rotacion_alta/result.total_productos*100:.1f}%)")
            print(f"Rotación muy alta (100-1000]: {result.rotacion_muy_alta:,} ({result.rotacion_muy_alta/result.total_productos*100:.1f}%)")
            print(f"Rotación extrema (>1000): {result.rotacion_extrema:,} ({result.rotacion_extrema/result.total_productos*100:.1f}%)")
            print(f"Rotación NULL: {result.rotacion_null:,} ({result.rotacion_null/result.total_productos*100:.1f}%)")
            
            print(f"\n📈 PROMEDIOS:")
            print(f"Rotación promedio (sin filtro): {result.rotacion_promedio_sin_filtro:.2f}")
            print(f"Rotación promedio (filtrada ≤1000): {result.rotacion_promedio_filtrada:.2f}")
            print(f"Rotación mínima: {result.rotacion_min:.2f}")
            print(f"Rotación máxima: {result.rotacion_max:.2f}")
            print(f"Desviación estándar: {result.rotacion_stddev:.2f}")
            
            # Get some examples of each category
            print(f"\n🔍 EJEMPLOS POR CATEGORÍA:")
            
            # Products with zero rotation
            zero_rotation = session.execute(text("""
                SELECT nombre_clean, total_compras, total_ventas, stock_final, rotacion
                FROM producto_kpis 
                WHERE rotacion = 0 AND fecha_inicio IS NOT NULL
                ORDER BY total_compras DESC
                LIMIT 5
            """)).fetchall()
            
            if zero_rotation:
                print(f"\n❌ PRODUCTOS CON ROTACIÓN 0 (Top 5 por compras):")
                for row in zero_rotation:
                    print(f"  - {row.nombre_clean}: Compras={row.total_compras}, Ventas={row.total_ventas}, Stock={row.stock_final}")
            
            # Products with normal rotation
            normal_rotation = session.execute(text("""
                SELECT nombre_clean, total_compras, total_ventas, stock_final, rotacion
                FROM producto_kpis 
                WHERE rotacion > 1 AND rotacion <= 20 AND fecha_inicio IS NOT NULL
                ORDER BY rotacion DESC
                LIMIT 5
            """)).fetchall()
            
            if normal_rotation:
                print(f"\n✅ PRODUCTOS CON ROTACIÓN NORMAL (1-20):")
                for row in normal_rotation:
                    print(f"  - {row.nombre_clean}: Rotación={row.rotacion:.2f}, Compras={row.total_compras}, Ventas={row.total_ventas}")
                    
            # Products with extreme rotation
            extreme_rotation = session.execute(text("""
                SELECT nombre_clean, total_compras, total_ventas, stock_final, rotacion, costo_promedio
                FROM producto_kpis 
                WHERE rotacion > 100 AND fecha_inicio IS NOT NULL
                ORDER BY rotacion DESC
                LIMIT 5
            """)).fetchall()
            
            if extreme_rotation:
                print(f"\n⚠️ PRODUCTOS CON ROTACIÓN EXTREMA (>100):")
                for row in extreme_rotation:
                    valor_promedio = row.stock_final * row.costo_promedio if row.costo_promedio else 0
                    print(f"  - {row.nombre_clean}: Rotación={row.rotacion:.2f}, Stock={row.stock_final}, Valor={valor_promedio:.2f}")
                    
            # Check the cause of zero rotation
            print(f"\n🔍 ANÁLISIS DE ROTACIÓN CERO:")
            zero_analysis = session.execute(text("""
                SELECT 
                    COUNT(CASE WHEN total_ventas = 0 THEN 1 END) as sin_ventas,
                    COUNT(CASE WHEN total_ventas > 0 AND stock_final = 0 THEN 1 END) as ventas_pero_sin_stock,
                    COUNT(CASE WHEN total_ventas > 0 AND stock_final > 0 AND costo_promedio = 0 THEN 1 END) as sin_costo,
                    COUNT(CASE WHEN total_ventas > 0 AND stock_final > 0 AND costo_promedio > 0 THEN 1 END) as otros_casos
                FROM producto_kpis 
                WHERE rotacion = 0 AND fecha_inicio IS NOT NULL
            """)).fetchone()
            
            if zero_analysis:
                print(f"  - Sin ventas: {zero_analysis.sin_ventas:,}")
                print(f"  - Con ventas pero sin stock: {zero_analysis.ventas_pero_sin_stock:,}")
                print(f"  - Sin costo promedio: {zero_analysis.sin_costo:,}")
                print(f"  - Otros casos: {zero_analysis.otros_casos:,}")
                
        else:
            print("❌ No se encontraron datos de KPIs en la base de datos")

if __name__ == "__main__":
    analyze_rotation_distribution()

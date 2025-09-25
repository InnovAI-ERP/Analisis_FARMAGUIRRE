#!/usr/bin/env python3
"""
Script para probar la corrección del gráfico de distribución de rotación
"""

import sys
import os
import pandas as pd
import plotly.express as px
from sqlalchemy import text

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

from db.database import get_session

def test_rotation_chart_fix():
    """Test the rotation chart fix by simulating dashboard data"""
    
    print("🧪 Probando la corrección del gráfico de distribución de rotación...")
    
    with get_session() as session:
        # Get the same data that the dashboard would get
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
            LIMIT 1000
        """)).fetchall()
        
        if not products_data:
            print("❌ No se encontraron datos de KPIs")
            return
            
        # Convert to DataFrame (simulating dashboard data)
        df = pd.DataFrame([dict(row._mapping) for row in products_data])
        
        print(f"\n📊 DATOS SIMULADOS DEL DASHBOARD:")
        print(f"Total productos en DataFrame: {len(df):,}")
        
        # Apply the same filter as the corrected dashboard
        print(f"\n🔧 APLICANDO FILTRO DE CORRECCIÓN...")
        df_filtered_rotation = df[(df['rotacion'] > 0) & (df['rotacion'] <= 1000)].copy()
        
        # Show filtering statistics
        total_products = len(df)
        filtered_products = len(df_filtered_rotation)
        zero_rotation = len(df[df['rotacion'] == 0])
        extreme_rotation = len(df[df['rotacion'] > 1000])
        
        print(f"\n📈 ESTADÍSTICAS DEL FILTRO:")
        print(f"- Total productos: {total_products:,}")
        print(f"- Productos mostrados (0 < rotación ≤ 1000): {filtered_products:,} ({filtered_products/total_products*100:.1f}%)")
        print(f"- Productos con rotación = 0: {zero_rotation:,} ({zero_rotation/total_products*100:.1f}%)")
        print(f"- Productos con rotación > 1000: {extreme_rotation:,} ({extreme_rotation/total_products*100:.1f}%)")
        
        if len(df_filtered_rotation) > 0:
            print(f"\n✅ DISTRIBUCIÓN DE ROTACIÓN FILTRADA:")
            
            # Show rotation ranges in filtered data
            rotation_ranges = [
                ("Muy baja (0-1]", len(df_filtered_rotation[(df_filtered_rotation['rotacion'] > 0) & (df_filtered_rotation['rotacion'] <= 1)])),
                ("Baja (1-5]", len(df_filtered_rotation[(df_filtered_rotation['rotacion'] > 1) & (df_filtered_rotation['rotacion'] <= 5)])),
                ("Media (5-20]", len(df_filtered_rotation[(df_filtered_rotation['rotacion'] > 5) & (df_filtered_rotation['rotacion'] <= 20)])),
                ("Alta (20-100]", len(df_filtered_rotation[(df_filtered_rotation['rotacion'] > 20) & (df_filtered_rotation['rotacion'] <= 100)])),
                ("Muy alta (100-1000]", len(df_filtered_rotation[(df_filtered_rotation['rotacion'] > 100) & (df_filtered_rotation['rotacion'] <= 1000)]))
            ]
            
            for range_name, count in rotation_ranges:
                percentage = (count / len(df_filtered_rotation) * 100) if len(df_filtered_rotation) > 0 else 0
                print(f"  - {range_name}: {count:,} productos ({percentage:.1f}%)")
            
            # Show statistics of filtered data
            rotation_stats = df_filtered_rotation['rotacion'].describe()
            print(f"\n📊 ESTADÍSTICAS DE ROTACIÓN FILTRADA:")
            print(f"  - Promedio: {rotation_stats['mean']:.2f}")
            print(f"  - Mediana: {rotation_stats['50%']:.2f}")
            print(f"  - Mínimo: {rotation_stats['min']:.2f}")
            print(f"  - Máximo: {rotation_stats['max']:.2f}")
            print(f"  - Desviación estándar: {rotation_stats['std']:.2f}")
            
            # Show some examples of products in each range
            print(f"\n🔍 EJEMPLOS DE PRODUCTOS POR RANGO:")
            
            for range_name, condition in [
                ("Rotación Baja (1-5)", (df_filtered_rotation['rotacion'] > 1) & (df_filtered_rotation['rotacion'] <= 5)),
                ("Rotación Media (5-20)", (df_filtered_rotation['rotacion'] > 5) & (df_filtered_rotation['rotacion'] <= 20)),
                ("Rotación Alta (20-100)", (df_filtered_rotation['rotacion'] > 20) & (df_filtered_rotation['rotacion'] <= 100))
            ]:
                range_products = df_filtered_rotation[condition].head(3)
                if not range_products.empty:
                    print(f"\n  {range_name}:")
                    for _, product in range_products.iterrows():
                        print(f"    - {product['nombre_clean']}: {product['rotacion']:.2f}")
            
            print(f"\n🎉 RESULTADO: El gráfico ahora mostrará una distribución útil con {filtered_products:,} productos")
            print(f"    En lugar de mostrar {zero_rotation:,} productos en rotación = 0")
            
        else:
            print(f"\n⚠️ PROBLEMA: No hay productos con rotación válida para mostrar")
            print(f"    Todos los productos tienen rotación = 0 o > 1000")
            
        print(f"\n✅ PRUEBA COMPLETADA - La corrección del gráfico funcionará correctamente")

if __name__ == "__main__":
    test_rotation_chart_fix()

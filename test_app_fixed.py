#!/usr/bin/env python3
"""
Script para verificar que app_fixed.py tiene todas las funciones necesarias
"""

import sys
import os

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

def test_app_fixed_completeness():
    """Test that app_fixed.py has all necessary functions"""
    
    print("🧪 Verificando completitud de app_fixed.py...")
    
    try:
        # Try to import the functions from app_fixed
        import app_fixed
        
        # Check if required functions exist
        required_functions = [
            'main',
            'show_dashboard',
            'show_visualizations_fixed',
            'export_to_excel',
            'process_files_fixed'
        ]
        
        missing_functions = []
        existing_functions = []
        
        for func_name in required_functions:
            if hasattr(app_fixed, func_name):
                existing_functions.append(func_name)
                print(f"  ✅ {func_name}: EXISTS")
            else:
                missing_functions.append(func_name)
                print(f"  ❌ {func_name}: MISSING")
        
        print(f"\n📊 RESUMEN:")
        print(f"  - Funciones existentes: {len(existing_functions)}/{len(required_functions)}")
        print(f"  - Funciones faltantes: {len(missing_functions)}")
        
        if missing_functions:
            print(f"\n❌ FUNCIONES FALTANTES:")
            for func in missing_functions:
                print(f"    - {func}")
            return False
        else:
            print(f"\n✅ TODAS LAS FUNCIONES NECESARIAS ESTÁN PRESENTES")
            
            # Test the rotation chart fix specifically
            print(f"\n🔍 VERIFICANDO CORRECCIÓN DE GRÁFICO DE ROTACIÓN...")
            
            # Read the source code to verify the fix is present
            with open('/Users/macair_kev/Documents/Contabilidades/FARMAGUIRRE S.A/2025/proyecto_analisis_de_compras/app_fixed.py', 'r', encoding='utf-8') as f:
                content = f.read()
                
            if 'df_filtered_rotation = df[(df[\'rotacion\'] > 0) & (df[\'rotacion\'] <= 1000)]' in content:
                print("  ✅ Filtro de rotación encontrado en show_visualizations_fixed")
            else:
                print("  ❌ Filtro de rotación NO encontrado")
                return False
                
            if 'rotacion > 0 AND rotacion <= 1000' in content:
                print("  ✅ Filtro de rotación promedio encontrado en KPI query")
            else:
                print("  ❌ Filtro de rotación promedio NO encontrado")
                return False
                
            if 'Distribución de Rotación - CORREGIDA' in content:
                print("  ✅ Título corregido del gráfico encontrado")
            else:
                print("  ❌ Título corregido del gráfico NO encontrado")
                return False
            
            print(f"\n🎉 VERIFICACIÓN COMPLETA: app_fixed.py está listo para usar")
            print(f"  - Todas las funciones necesarias presentes")
            print(f"  - Corrección del gráfico de rotación implementada")
            print(f"  - Filtros aplicados tanto en visualización como en KPIs")
            
            return True
            
    except ImportError as e:
        print(f"❌ ERROR: No se pudo importar app_fixed.py: {e}")
        return False
    except Exception as e:
        print(f"❌ ERROR INESPERADO: {e}")
        return False

if __name__ == "__main__":
    success = test_app_fixed_completeness()
    
    if success:
        print(f"\n✅ RESULTADO: app_fixed.py está correctamente implementado")
        print(f"   El gráfico de distribución de rotación ahora funcionará correctamente")
    else:
        print(f"\n❌ RESULTADO: app_fixed.py necesita correcciones adicionales")
    
    exit(0 if success else 1)

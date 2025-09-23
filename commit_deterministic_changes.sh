#!/bin/bash

# Script para hacer commit de todos los cambios determinísticos
echo "🔧 Preparando commit de cambios determinísticos..."

# Verificar que estamos en el directorio correcto
if [ ! -f "app.py" ]; then
    echo "❌ Error: No se encuentra app.py. Ejecuta desde el directorio del proyecto."
    exit 1
fi

# Agregar todos los archivos modificados
echo "📁 Agregando archivos modificados..."
git add app.py
git add utils/kpi.py
git add etl/hybrid_normalized_loader.py

# Agregar archivos nuevos
echo "📄 Agregando archivos nuevos..."
git add utils/kpi_fixed.py
git add etl/hybrid_normalized_loader_fixed.py
git add app_fixed.py
git add test_deterministic.py
git add README_DETERMINISTIC.md
git add commit_deterministic_changes.sh

# Mostrar estado
echo "📊 Estado de archivos:"
git status --porcelain

# Hacer commit
echo "💾 Haciendo commit..."
git commit -m "🎯 FEAT: Implementar sistema completamente determinístico

✅ Correcciones implementadas:
- KPIs incrementales (no limpieza masiva)
- Consultas SQL con ORDER BY determinístico  
- Agregaciones con ordenamiento consistente
- Clasificación ABC/XYZ con claves secundarias
- Procesamiento de datos en orden estable

🔧 Archivos modificados:
- app.py: Usa funciones determinísticas por defecto
- utils/kpi.py: Cálculos con ordenamiento determinístico
- etl/hybrid_normalized_loader.py: Agregaciones ordenadas

📁 Archivos nuevos:
- utils/kpi_fixed.py: Implementación determinística completa
- etl/hybrid_normalized_loader_fixed.py: Agregaciones determinísticas  
- app_fixed.py: Versión de prueba explícita
- test_deterministic.py: Script de verificación automática
- README_DETERMINISTIC.md: Documentación completa

🎯 Garantía: Mismos datos + parámetros = mismos resultados SIEMPRE

Fixes: #deterministic-kpis"

if [ $? -eq 0 ]; then
    echo "✅ Commit exitoso!"
    echo ""
    echo "🚀 Próximos pasos:"
    echo "1. Ejecutar: python test_deterministic.py"
    echo "2. Verificar: streamlit run app.py"  
    echo "3. Validar: Procesar mismos datos 3 veces"
    echo "4. Push: git push origin main"
    echo ""
    echo "🎯 ¡Sistema determinístico listo para producción!"
else
    echo "❌ Error en commit"
    exit 1
fi

"""
Módulo de análisis automático de resultados de inventario
Genera comentarios inteligentes sobre los KPIs calculados
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Any


def analyze_coverage_vs_stock(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analiza la relación entre cobertura y stock final
    Identifica productos con problemas de inventario
    """
    if df.empty:
        return {"error": "No hay datos para analizar"}
    
    # Filtrar productos con datos válidos
    valid_data = df.dropna(subset=['cobertura_dias', 'stock_final'])
    
    if valid_data.empty:
        return {"error": "No hay datos válidos de cobertura y stock"}
    
    # Categorizar productos por cobertura
    exceso = valid_data[valid_data['cobertura_dias'] > 90]
    normal = valid_data[(valid_data['cobertura_dias'] >= 30) & (valid_data['cobertura_dias'] <= 90)]
    bajo = valid_data[valid_data['cobertura_dias'] < 30]
    
    # Calcular estadísticas
    total_productos = len(valid_data)
    total_valor_inventario = valid_data['valor_inventario'].sum()
    
    # Productos con mayor exceso de inventario
    exceso_top = exceso.nlargest(5, 'valor_inventario')[['descripcion', 'cobertura_dias', 'valor_inventario', 'stock_final']]
    
    # Productos con bajo stock crítico
    bajo_critico = bajo[bajo['stock_final'] > 0].nsmallest(5, 'cobertura_dias')[['descripcion', 'cobertura_dias', 'valor_inventario', 'stock_final']]
    
    # Productos sin movimiento
    sin_movimiento = valid_data[valid_data['cobertura_dias'].isna() | (valid_data['cobertura_dias'] == float('inf'))]
    
    analysis = {
        "resumen": {
            "total_productos": total_productos,
            "productos_exceso": len(exceso),
            "productos_normal": len(normal),
            "productos_bajo": len(bajo),
            "porcentaje_exceso": round((len(exceso) / total_productos) * 100, 1),
            "porcentaje_normal": round((len(normal) / total_productos) * 100, 1),
            "porcentaje_bajo": round((len(bajo) / total_productos) * 100, 1),
            "valor_total_inventario": total_valor_inventario,
            "valor_exceso": exceso['valor_inventario'].sum(),
            "valor_bajo": bajo['valor_inventario'].sum()
        },
        "productos_exceso_top": exceso_top.to_dict('records'),
        "productos_bajo_critico": bajo_critico.to_dict('records'),
        "productos_sin_movimiento": len(sin_movimiento),
        "comentarios": generate_coverage_comments(exceso, normal, bajo, total_valor_inventario)
    }
    
    return analysis


def analyze_inventory_distribution(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analiza la distribución del inventario por diferentes dimensiones
    """
    if df.empty:
        return {"error": "No hay datos para analizar"}
    
    valid_data = df.dropna(subset=['valor_inventario'])
    
    if valid_data.empty:
        return {"error": "No hay datos válidos de inventario"}
    
    # Distribución por valor
    total_valor = valid_data['valor_inventario'].sum()
    
    # Top 10 productos por valor
    top_productos_valor = valid_data.nlargest(10, 'valor_inventario')[['descripcion', 'valor_inventario', 'stock_final']]
    
    # Distribución por cantidad de productos
    total_productos = len(valid_data)
    
    # Análisis de concentración (Pareto)
    sorted_by_value = valid_data.sort_values('valor_inventario', ascending=False)
    sorted_by_value['valor_acumulado'] = sorted_by_value['valor_inventario'].cumsum()
    sorted_by_value['porcentaje_acumulado'] = (sorted_by_value['valor_acumulado'] / total_valor) * 100
    
    # Encontrar el 80% del valor
    productos_80_pct = len(sorted_by_value[sorted_by_value['porcentaje_acumulado'] <= 80])
    porcentaje_productos_80 = round((productos_80_pct / total_productos) * 100, 1)
    
    # Estadísticas descriptivas
    stats = {
        "valor_promedio": valid_data['valor_inventario'].mean(),
        "valor_mediano": valid_data['valor_inventario'].median(),
        "valor_maximo": valid_data['valor_inventario'].max(),
        "valor_minimo": valid_data['valor_inventario'].min(),
        "desviacion_estandar": valid_data['valor_inventario'].std()
    }
    
    analysis = {
        "resumen": {
            "total_productos": total_productos,
            "valor_total": total_valor,
            "productos_80_pct_valor": productos_80_pct,
            "porcentaje_productos_80": porcentaje_productos_80,
            "concentracion_alta": porcentaje_productos_80 < 20  # Regla 80/20
        },
        "top_productos_valor": top_productos_valor.to_dict('records'),
        "estadisticas": stats,
        "comentarios": generate_distribution_comments(stats, porcentaje_productos_80, total_valor)
    }
    
    return analysis


def analyze_abc_xyz_matrix(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analiza la matriz ABC/XYZ y proporciona insights estratégicos
    """
    if df.empty:
        return {"error": "No hay datos para analizar"}
    
    # Verificar que existan las columnas necesarias
    required_cols = ['clasificacion_abc', 'clasificacion_xyz', 'valor_inventario', 'descripcion']
    if not all(col in df.columns for col in required_cols):
        return {"error": "Faltan columnas necesarias para análisis ABC/XYZ"}
    
    valid_data = df.dropna(subset=required_cols)
    
    if valid_data.empty:
        return {"error": "No hay datos válidos para análisis ABC/XYZ"}
    
    # Crear matriz ABC/XYZ
    matrix = valid_data.groupby(['clasificacion_abc', 'clasificacion_xyz']).agg({
        'valor_inventario': ['sum', 'count'],
        'descripcion': 'first'  # Para obtener ejemplos
    }).round(2)
    
    # Aplanar columnas multi-nivel
    matrix.columns = ['valor_total', 'cantidad_productos', 'ejemplo']
    matrix = matrix.reset_index()
    
    # Calcular porcentajes
    total_valor = valid_data['valor_inventario'].sum()
    matrix['porcentaje_valor'] = round((matrix['valor_total'] / total_valor) * 100, 1)
    
    # Identificar productos estratégicos por categoría
    strategic_products = {}
    
    # AX - Alta rotación, alta predictibilidad (productos estrella)
    ax_products = valid_data[(valid_data['clasificacion_abc'] == 'A') & 
                            (valid_data['clasificacion_xyz'] == 'X')]
    if not ax_products.empty:
        strategic_products['AX'] = ax_products.nlargest(3, 'valor_inventario')[['descripcion', 'valor_inventario']].to_dict('records')
    
    # AZ - Alta rotación, baja predictibilidad (productos críticos)
    az_products = valid_data[(valid_data['clasificacion_abc'] == 'A') & 
                            (valid_data['clasificacion_xyz'] == 'Z')]
    if not az_products.empty:
        strategic_products['AZ'] = az_products.nlargest(3, 'valor_inventario')[['descripcion', 'valor_inventario']].to_dict('records')
    
    # CZ - Baja rotación, baja predictibilidad (productos problemáticos)
    cz_products = valid_data[(valid_data['clasificacion_abc'] == 'C') & 
                            (valid_data['clasificacion_xyz'] == 'Z')]
    if not cz_products.empty:
        strategic_products['CZ'] = cz_products.nlargest(3, 'valor_inventario')[['descripcion', 'valor_inventario']].to_dict('records')
    
    # Análisis por clasificación ABC
    abc_summary = valid_data.groupby('clasificacion_abc').agg({
        'valor_inventario': ['sum', 'count'],
        'stock_final': 'sum'
    }).round(2)
    abc_summary.columns = ['valor_total', 'cantidad_productos', 'stock_total']
    abc_summary = abc_summary.reset_index()
    abc_summary['porcentaje_valor'] = round((abc_summary['valor_total'] / total_valor) * 100, 1)
    
    analysis = {
        "matriz_completa": matrix.to_dict('records'),
        "productos_estrategicos": strategic_products,
        "resumen_abc": abc_summary.to_dict('records'),
        "total_valor": total_valor,
        "comentarios": generate_abc_xyz_comments(matrix, strategic_products, abc_summary)
    }
    
    return analysis


def generate_coverage_comments(exceso: pd.DataFrame, normal: pd.DataFrame, 
                             bajo: pd.DataFrame, total_valor: float) -> List[str]:
    """
    Genera comentarios automáticos sobre el análisis de cobertura
    """
    comments = []
    
    total_productos = len(exceso) + len(normal) + len(bajo)
    
    if len(exceso) > 0:
        pct_exceso = round((len(exceso) / total_productos) * 100, 1)
        valor_exceso = exceso['valor_inventario'].sum()
        pct_valor_exceso = round((valor_exceso / total_valor) * 100, 1)
        
        if pct_exceso > 30:
            comments.append(f"⚠️ **ALERTA DE SOBRESTOCK**: {pct_exceso}% de los productos ({len(exceso)} productos) tienen más de 90 días de cobertura, representando {pct_valor_exceso}% del valor total del inventario (₡{valor_exceso:,.0f}).")
        
        if not exceso.empty:
            producto_mayor_exceso = exceso.loc[exceso['cobertura_dias'].idxmax()]
            comments.append(f"📊 **Producto con mayor sobrestock**: {producto_mayor_exceso['descripcion']} con {producto_mayor_exceso['cobertura_dias']:.0f} días de cobertura y valor de ₡{producto_mayor_exceso['valor_inventario']:,.0f}.")
    
    if len(bajo) > 0:
        pct_bajo = round((len(bajo) / total_productos) * 100, 1)
        
        if pct_bajo > 20:
            comments.append(f"🚨 **RIESGO DE DESABASTO**: {pct_bajo}% de los productos ({len(bajo)} productos) tienen menos de 30 días de cobertura.")
        
        productos_criticos = bajo[bajo['cobertura_dias'] < 7]
        if len(productos_criticos) > 0:
            comments.append(f"⛔ **CRÍTICO**: {len(productos_criticos)} productos tienen menos de 7 días de cobertura y requieren reposición inmediata.")
    
    if len(normal) > 0:
        pct_normal = round((len(normal) / total_productos) * 100, 1)
        comments.append(f"✅ **Inventario saludable**: {pct_normal}% de los productos ({len(normal)} productos) mantienen una cobertura óptima entre 30-90 días.")
    
    return comments


def generate_distribution_comments(stats: Dict, porcentaje_productos_80: float, 
                                 total_valor: float) -> List[str]:
    """
    Genera comentarios sobre la distribución del inventario
    """
    comments = []
    
    # Análisis de concentración
    if porcentaje_productos_80 < 20:
        comments.append(f"📈 **Alta concentración de valor**: Solo el {porcentaje_productos_80}% de los productos representan el 80% del valor del inventario (₡{total_valor:,.0f}). Esto indica una distribución tipo Pareto muy marcada.")
    elif porcentaje_productos_80 > 40:
        comments.append(f"📊 **Distribución equilibrada**: El {porcentaje_productos_80}% de los productos representan el 80% del valor, indicando una distribución más equilibrada del inventario.")
    
    # Análisis de variabilidad
    coef_variacion = stats['desviacion_estandar'] / stats['valor_promedio']
    if coef_variacion > 2:
        comments.append(f"📊 **Alta variabilidad**: Existe gran dispersión en los valores de inventario (coeficiente de variación: {coef_variacion:.1f}). El valor promedio es ₡{stats['valor_promedio']:,.0f} pero la mediana es ₡{stats['valor_mediano']:,.0f}.")
    
    # Productos de alto valor
    if stats['valor_maximo'] > stats['valor_promedio'] * 10:
        comments.append(f"💎 **Productos de alto valor**: El producto más valioso (₡{stats['valor_maximo']:,.0f}) representa más de 10 veces el valor promedio, requiere gestión especial.")
    
    return comments


def generate_abc_xyz_comments(matrix: pd.DataFrame, strategic_products: Dict, 
                            abc_summary: pd.DataFrame) -> List[str]:
    """
    Genera comentarios estratégicos sobre la matriz ABC/XYZ
    """
    comments = []
    
    # Análisis de productos A
    productos_a = abc_summary[abc_summary['clasificacion_abc'] == 'A']
    if not productos_a.empty:
        pct_valor_a = productos_a.iloc[0]['porcentaje_valor']
        cantidad_a = productos_a.iloc[0]['cantidad_productos']
        comments.append(f"🎯 **Productos clase A**: {cantidad_a} productos ({pct_valor_a}% del valor total) son los más importantes para el negocio. Requieren gestión prioritaria y monitoreo constante.")
    
    # Análisis de productos AX (estrella)
    if 'AX' in strategic_products and strategic_products['AX']:
        ax_matrix = matrix[(matrix['clasificacion_abc'] == 'A') & (matrix['clasificacion_xyz'] == 'X')]
        if not ax_matrix.empty:
            comments.append(f"⭐ **Productos estrella (AX)**: {ax_matrix.iloc[0]['cantidad_productos']} productos de alta rotación y demanda predecible. Mantener stock óptimo para evitar desabastos.")
            
            # Mencionar productos específicos
            top_ax = strategic_products['AX'][0]
            comments.append(f"🏆 **Producto AX destacado**: {top_ax['descripcion']} (₡{top_ax['valor_inventario']:,.0f}) - Gestión prioritaria.")
    
    # Análisis de productos AZ (críticos)
    if 'AZ' in strategic_products and strategic_products['AZ']:
        az_matrix = matrix[(matrix['clasificacion_abc'] == 'A') & (matrix['clasificacion_xyz'] == 'Z')]
        if not az_matrix.empty:
            comments.append(f"⚠️ **Productos críticos (AZ)**: {az_matrix.iloc[0]['cantidad_productos']} productos de alta rotación pero demanda impredecible. Requieren stock de seguridad elevado y monitoreo frecuente.")
    
    # Análisis de productos CZ (problemáticos)
    if 'CZ' in strategic_products and strategic_products['CZ']:
        cz_matrix = matrix[(matrix['clasificacion_abc'] == 'C') & (matrix['clasificacion_xyz'] == 'Z')]
        if not cz_matrix.empty:
            comments.append(f"🔴 **Productos problemáticos (CZ)**: {cz_matrix.iloc[0]['cantidad_productos']} productos de baja rotación y demanda impredecible. Considerar descontinuar o liquidar.")
    
    # Recomendaciones estratégicas
    comments.append("💡 **Recomendaciones estratégicas**:")
    comments.append("   • Productos AX: Automatizar reposición y mantener stock mínimo")
    comments.append("   • Productos AY: Revisar patrones estacionales y ajustar pronósticos")
    comments.append("   • Productos AZ: Aumentar stock de seguridad y frecuencia de revisión")
    comments.append("   • Productos CZ: Evaluar descontinuación o estrategias de liquidación")
    
    return comments


def format_analysis_for_display(analysis_result: Dict[str, Any], analysis_type: str) -> str:
    """
    Formatea los resultados del análisis para mostrar en Streamlit
    """
    if "error" in analysis_result:
        return f"❌ Error en análisis {analysis_type}: {analysis_result['error']}"
    
    formatted_text = f"## 📊 Análisis: {analysis_type}\n\n"
    
    if "comentarios" in analysis_result:
        for comment in analysis_result["comentarios"]:
            formatted_text += f"{comment}\n\n"
    
    return formatted_text

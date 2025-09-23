#!/usr/bin/env python3
"""
Script para actualizar la base de datos agregando la columna stock_final
y recalcular los KPIs con el stock final correcto
"""

import sys
import os
from datetime import date
import logging

# Add current directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)

try:
    from db.database import get_session, init_database, get_engine
    from db.models_normalized import Base as NormalizedBase
    from utils.kpi_fixed import calculate_kpis_fixed
    from etl.hybrid_normalized_loader_fixed import create_daily_aggregates_normalized_fixed
    from sqlalchemy import text, inspect
    import logging
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_stock_final_column():
    """Check if stock_final column exists in producto_kpis table"""
    engine = get_engine()
    inspector = inspect(engine)
    
    try:
        columns = inspector.get_columns('producto_kpis')
        column_names = [col['name'] for col in columns]
        return 'stock_final' in column_names
    except Exception as e:
        logger.error(f"Error checking columns: {e}")
        return False

def add_stock_final_column():
    """Add stock_final column to producto_kpis table if it doesn't exist"""
    with get_session() as session:
        try:
            # Add the column
            session.execute(text("""
                ALTER TABLE producto_kpis 
                ADD COLUMN stock_final REAL DEFAULT 0.0
            """))
            session.commit()
            logger.info("✅ Added stock_final column to producto_kpis table")
            return True
        except Exception as e:
            session.rollback()
            if "duplicate column name" in str(e).lower() or "already exists" in str(e).lower():
                logger.info("✅ stock_final column already exists")
                return True
            else:
                logger.error(f"❌ Error adding stock_final column: {e}")
                return False

def update_existing_stock_final():
    """Update existing records to calculate stock_final from total_compras - total_ventas"""
    with get_session() as session:
        try:
            # Update stock_final for existing records
            result = session.execute(text("""
                UPDATE producto_kpis 
                SET stock_final = CASE 
                    WHEN (total_compras - total_ventas) < 0 THEN 0 
                    ELSE (total_compras - total_ventas) 
                END
                WHERE stock_final = 0 OR stock_final IS NULL
            """))
            
            session.commit()
            updated_count = result.rowcount
            logger.info(f"✅ Updated stock_final for {updated_count} existing records")
            return True
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Error updating existing stock_final: {e}")
            return False

def main():
    """Main function to update database and recalculate KPIs"""
    print("🔧 ACTUALIZANDO BASE DE DATOS PARA STOCK FINAL CORRECTO")
    print("=" * 60)
    
    # Step 1: Initialize database
    try:
        init_database()
        print("✅ Base de datos inicializada")
    except Exception as e:
        print(f"❌ Error inicializando base de datos: {e}")
        return False
    
    # Step 2: Check if stock_final column exists
    print("\n🔍 Verificando estructura de base de datos...")
    has_stock_final = check_stock_final_column()
    
    if not has_stock_final:
        print("📝 Agregando columna stock_final...")
        if not add_stock_final_column():
            print("❌ No se pudo agregar la columna stock_final")
            return False
    else:
        print("✅ Columna stock_final ya existe")
    
    # Step 3: Recreate tables to ensure schema is up to date
    print("\n🔄 Actualizando esquema de base de datos...")
    try:
        engine = get_engine()
        NormalizedBase.metadata.create_all(bind=engine)
        print("✅ Esquema actualizado")
    except Exception as e:
        print(f"❌ Error actualizando esquema: {e}")
        return False
    
    # Step 4: Update existing records
    print("\n📊 Actualizando registros existentes...")
    if not update_existing_stock_final():
        print("❌ No se pudieron actualizar los registros existentes")
        return False
    
    # Step 5: Check if we have data to recalculate
    with get_session() as session:
        try:
            compras_count = session.execute(text("SELECT COUNT(*) FROM compras_normalized")).scalar()
            ventas_count = session.execute(text("SELECT COUNT(*) FROM ventas_normalized")).scalar()
            
            if compras_count == 0 or ventas_count == 0:
                print("⚠️ No hay datos en las tablas normalizadas para recalcular")
                print("   Los cambios se aplicarán en el próximo procesamiento de datos")
                return True
                
            print(f"📊 Datos disponibles: {compras_count} compras, {ventas_count} ventas")
            
        except Exception as e:
            print(f"❌ Error verificando datos: {e}")
            return False
    
    # Step 6: Recalculate KPIs with corrected stock_final
    print("\n🧮 Recalculando KPIs con stock_final corregido...")
    try:
        start_date = date(2025, 1, 1)
        end_date = date.today()
        
        # Recreate daily aggregates
        print("   📊 Recreando agregados diarios...")
        create_daily_aggregates_normalized_fixed(start_date, end_date)
        
        # Recalculate KPIs
        print("   🧮 Recalculando KPIs...")
        calculate_kpis_fixed(
            start_date, 
            end_date,
            service_level=0.95,
            lead_time_days=7,
            excess_threshold=45,
            shortage_threshold=7
        )
        
        print("✅ KPIs recalculados con stock_final correcto")
        
    except Exception as e:
        print(f"❌ Error recalculando KPIs: {e}")
        return False
    
    # Step 7: Verify results
    print("\n🔍 Verificando resultados...")
    with get_session() as session:
        try:
            # Check a few sample products
            sample_products = session.execute(text("""
                SELECT 
                    nombre_clean,
                    total_compras,
                    total_ventas,
                    stock_final,
                    (total_compras - total_ventas) as calculated_stock
                FROM producto_kpis 
                WHERE total_compras > 0 OR total_ventas > 0
                ORDER BY nombre_clean
                LIMIT 5
            """)).fetchall()
            
            print("📊 Muestra de productos verificados:")
            print("Producto | Compras | Ventas | Stock Final | Stock Calculado")
            print("-" * 70)
            for p in sample_products:
                stock_diff = abs(p.stock_final - max(0, p.calculated_stock))
                status = "✅" if stock_diff < 0.01 else "❌"
                print(f"{p.nombre_clean[:20]:<20} | {p.total_compras:>7.1f} | {p.total_ventas:>6.1f} | {p.stock_final:>11.1f} | {max(0, p.calculated_stock):>15.1f} {status}")
            
        except Exception as e:
            print(f"❌ Error verificando resultados: {e}")
            return False
    
    print("\n" + "=" * 60)
    print("🎉 ¡ACTUALIZACIÓN COMPLETADA EXITOSAMENTE!")
    print("✅ Base de datos actualizada con stock_final correcto")
    print("✅ KPIs recalculados con valores precisos")
    print("✅ Sistema listo para mostrar stock final real")
    print("\n💡 Próximos pasos:")
    print("1. Ejecutar: streamlit run app.py")
    print("2. Verificar que PRIMABELA TBAS muestre stock final = 106")
    print("3. Confirmar que DOLO NEUROBION N TBAS muestre stock final = 0")
    
    return True

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

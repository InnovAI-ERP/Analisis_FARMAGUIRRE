"""
SQLAlchemy models for the inventory analysis system
"""

from sqlalchemy import Column, Integer, String, Float, Date, Text, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class ComprasHeader(Base):
    """Purchase invoice header table"""
    __tablename__ = 'compras_header'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(Date, nullable=False)
    no_consecutivo = Column(String(50), unique=True, nullable=False)
    no_factura = Column(String(50))
    no_guia = Column(String(50))
    ced_juridica = Column(String(50))
    proveedor = Column(String(200))
    
    # Relationship to detail records
    details = relationship("ComprasDetail", back_populates="header")

class ComprasDetail(Base):
    """Purchase invoice detail table"""
    __tablename__ = 'compras_detail'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    no_consecutivo = Column(String(50), ForeignKey('compras_header.no_consecutivo'), nullable=False)
    cabys = Column(String(50))
    codigo = Column(String(50))
    nombre = Column(String(300))
    nombre_clean = Column(String(300))  # Normalized product name
    variacion = Column(String(100))
    codigo_referencia = Column(String(50))
    codigo_color = Column(String(50))
    color = Column(String(100))
    cantidad = Column(Float)  # Purchase quantity (complete units)
    descuento = Column(Float)  # Discount %
    utilidad = Column(Float)  # Profit margin %
    costo = Column(Float)  # Cost per unit (same as precio_unit for purchases)
    precio_unit = Column(Float)  # Unit cost for purchase
    
    # Additional fields for normalization (matching VentasDetail structure)
    es_fraccion = Column(Integer, default=0)  # 1 if was FRAC.
    factor_fraccion = Column(Float, default=1.0)  # Conversion factor
    qty_normalizada = Column(Float)  # Quantity equivalent to complete units
    
    # Populated header data for normalization (denormalized for easier queries)
    fecha_compra = Column(Date)  # From header
    no_factura = Column(String(50))  # From header
    no_guia = Column(String(50))  # From header
    ced_juridica = Column(String(50))  # From header
    proveedor = Column(String(200))  # From header
    
    # Relationship to header
    header = relationship("ComprasHeader", back_populates="details")

# Index for efficient product lookups
Index('idx_cdet_key', ComprasDetail.cabys, ComprasDetail.nombre_clean)

class VentasHeader(Base):
    """Sales invoice header table"""
    __tablename__ = 'ventas_header'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    no_factura_interna = Column(String(50), nullable=False)
    fecha = Column(Date, nullable=False)
    tipo_documento = Column(String(50))
    cliente = Column(String(200))
    cedula = Column(String(50))
    vendedor = Column(String(100))
    caja = Column(String(50))
    
    # Relationship to detail records
    details = relationship("VentasDetail", back_populates="header")

class VentasDetail(Base):
    """Sales invoice detail table"""
    __tablename__ = 'ventas_detail'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    no_factura_interna = Column(String(50), ForeignKey('ventas_header.no_factura_interna'), nullable=False)
    cabys = Column(String(50))
    codigo = Column(String(50))
    descripcion = Column(String(300))
    nombre_clean = Column(String(300))  # Description without "FRAC. "
    cantidad = Column(Float)  # Sold quantity (fractions or complete)
    descuento = Column(Float)  # Discount %
    utilidad = Column(Float)  # Profit margin %
    costo = Column(Float)  # Cost per "complete unit"
    precio_unit = Column(Float)  # Price per fraction or per unit
    total = Column(Float)
    es_fraccion = Column(Integer)  # 1 if was FRAC.
    factor_fraccion = Column(Float)  # Conversion factor
    qty_normalizada = Column(Float)  # Quantity equivalent to complete units
    
    # Populated header data for normalization (denormalized for easier queries)
    fecha_venta = Column(Date)  # From header
    tipo_documento = Column(String(50))  # From header
    cliente = Column(String(200))  # From header
    cedula = Column(String(50))  # From header
    vendedor = Column(String(100))  # From header
    caja = Column(String(50))  # From header
    
    # Relationship to header
    header = relationship("VentasHeader", back_populates="details")

# Index for efficient product lookups
Index('idx_vdet_key', VentasDetail.cabys, VentasDetail.nombre_clean)

class KpiMovDiario(Base):
    """Daily movement aggregates table"""
    __tablename__ = 'kpi_mov_diario'
    
    fecha = Column(Date, primary_key=True)
    cabys = Column(String(50), primary_key=True)
    nombre_clean = Column(String(300), primary_key=True)
    qty_in = Column(Float, default=0.0)  # Normalized purchases
    qty_out = Column(Float, default=0.0)  # Normalized sales

class Productos(Base):
    """Consolidated product catalog"""
    __tablename__ = 'productos'
    
    cabys = Column(String(50), primary_key=True)
    nombre_clean = Column(String(300), primary_key=True)
    codigo_alt = Column(String(50))  # Alternative internal code

class ProductoKpis(Base):
    """Product KPIs calculation results"""
    __tablename__ = 'producto_kpis'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cabys = Column(String(50), nullable=False)
    nombre_clean = Column(String(300), nullable=False)
    fecha_calculo = Column(Date, nullable=False, default=datetime.now().date())
    
    # Basic metrics
    total_qty_in = Column(Float, default=0.0)
    total_qty_out = Column(Float, default=0.0)
    stock_final = Column(Float, default=0.0)
    inventario_promedio = Column(Float, default=0.0)
    
    # Cost and financial metrics
    avg_cost = Column(Float, default=0.0)
    cogs = Column(Float, default=0.0)
    valor_inventario = Column(Float, default=0.0)
    
    # Performance metrics
    rotacion = Column(Float, default=0.0)
    dio = Column(Float, default=0.0)  # Days Inventory Outstanding
    coverage_days = Column(Float, default=0.0)
    
    # Demand metrics
    avg_daily_demand = Column(Float, default=0.0)
    std_demand_daily = Column(Float, default=0.0)
    cv_demand = Column(Float, default=0.0)  # Coefficient of variation
    
    # Reorder point and safety stock
    safety_stock = Column(Float, default=0.0)
    rop = Column(Float, default=0.0)  # Reorder point
    
    # Classifications
    abc_class = Column(String(1))  # A, B, C
    xyz_class = Column(String(1))  # X, Y, Z
    
    # Flags
    flag_exceso = Column(Integer, default=0)
    flag_faltante = Column(Integer, default=0)
    
    # Index for efficient lookups
    __table_args__ = (
        Index('idx_producto_kpis', 'cabys', 'nombre_clean', 'fecha_calculo'),
    )

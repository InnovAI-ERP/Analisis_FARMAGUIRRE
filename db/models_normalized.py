"""
Normalized database models for Farmaguirre inventory analysis
Single table approach with all invoice + product data denormalized
"""

from sqlalchemy import Column, Integer, String, Float, Date, ForeignKey, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class ComprasNormalized(Base):
    """Normalized purchases table with all invoice + product data"""
    __tablename__ = 'compras_normalized'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Product fields
    cabys = Column(String(50))
    codigo = Column(String(50))
    variacion = Column(String(100))
    codigo_referencia = Column(String(50))
    nombre = Column(String(300))  # Original product name
    nombre_clean = Column(String(300))  # Normalized product name
    codigo_color = Column(String(50))
    color = Column(String(100))
    cantidad = Column(Float)
    regalia = Column(Float)
    aplica_impuesto = Column(String(10))
    costo = Column(Float)
    descuento = Column(Float)
    utilidad = Column(Float)
    precio = Column(Float)
    precio_unit = Column(Float)  # Alias for precio
    total = Column(Float)
    
    # Invoice fields (denormalized)
    fecha = Column(Date)
    no_consecutivo = Column(String(50))
    no_factura = Column(String(50))
    no_guia = Column(String(50))
    ced_juridica = Column(String(50))
    proveedor = Column(String(200))
    items = Column(Integer)
    fecha_vencimiento = Column(Date)
    dias_plazo = Column(Integer)
    moneda = Column(String(50))
    tipo_cambio = Column(Float, default=1.0)
    monto = Column(Float)
    descuento_factura = Column(Float)
    iva = Column(Float)
    total_factura = Column(Float)
    observaciones = Column(String(500))
    motivo = Column(String(200))
    
    # Normalization fields
    es_fraccion = Column(Integer, default=0)
    factor_fraccion = Column(Float, default=1.0)
    qty_normalizada = Column(Float)
    
    # Compatibility fields
    fecha_compra = Column(Date)  # Alias for fecha

class VentasNormalized(Base):
    """Normalized sales table with all invoice + product data"""
    __tablename__ = 'ventas_normalized'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Product fields
    codigo = Column(String(50))
    cabys = Column(String(50))
    descripcion = Column(String(300))  # Original product name
    nombre_clean = Column(String(300))  # Normalized product name
    color = Column(String(100))
    cantidad = Column(Float)
    descuento = Column(Float)
    utilidad = Column(Float)
    costo = Column(Float)
    precio_unit = Column(Float)
    total = Column(Float)
    
    # Invoice fields (denormalized)
    no_factura_interna = Column(String(50))
    no_orden = Column(String(50))
    no_orden_compra = Column(String(50))
    tipo_gasto = Column(String(100))
    no_factura_electronica = Column(String(50))
    tipo_documento = Column(String(50))
    codigo_actividad = Column(String(50))
    facturado_por = Column(String(200))
    hecho_por = Column(String(200))
    codigo_cliente = Column(String(50))
    cliente = Column(String(200))
    cedula_fisica = Column(String(50))
    a_terceros = Column(String(50))
    tipo_venta = Column(String(50))
    tipo_moneda = Column(String(50))
    tipo_cambio = Column(Float, default=1.0)
    estado = Column(String(50))
    fecha = Column(Date)
    subtotal = Column(Float)
    impuestos = Column(Float)
    impuesto_servicios = Column(Float)
    impuestos_devueltos = Column(Float)
    exonerado = Column(Float)
    total_factura = Column(Float)
    total_exento = Column(Float)
    total_gravado = Column(Float)
    no_referencia_tarjeta = Column(String(50))
    monto_tarjeta = Column(Float)
    monto_efectivo = Column(Float)
    no_referencia_transaccion = Column(String(50))
    monto_transaccion = Column(Float)
    no_referencia = Column(String(50))
    monto_en = Column(Float)
    
    # Normalization fields
    es_fraccion = Column(Integer, default=0)
    factor_fraccion = Column(Float, default=1.0)
    qty_normalizada = Column(Float)
    
    # Compatibility fields
    nombre = Column(String(300))  # Alias for descripcion
    fecha_venta = Column(Date)  # Alias for fecha

# Keep existing models for compatibility
class ComprasHeader(Base):
    """Purchase invoice header table"""
    __tablename__ = 'compras_header'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    no_consecutivo = Column(String(50), nullable=False, unique=True)
    fecha = Column(Date, nullable=False)
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
    
    # Compatibility fields
    nombre = Column(String(300))  # Alias for descripcion
    
    # Relationship to header
    header = relationship("VentasHeader", back_populates="details")

class ProductoCatalog(Base):
    """Product catalog with normalized names"""
    __tablename__ = 'producto_catalog'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cabys = Column(String(50))
    nombre_clean = Column(String(300), nullable=False, unique=True)
    nombre_original = Column(String(300))
    es_fraccion = Column(Integer, default=0)
    factor_fraccion = Column(Float, default=1.0)

class KpiMovDiario(Base):
    """Daily movement aggregates for KPI calculation"""
    __tablename__ = 'kpi_mov_diario_normalized'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha = Column(Date, nullable=False)
    cabys = Column(String(50))
    nombre_clean = Column(String(300), nullable=False)
    qty_in = Column(Float, default=0.0)  # Purchases
    qty_out = Column(Float, default=0.0)  # Sales

class ProductoKpis(Base):
    """Product KPIs and metrics"""
    __tablename__ = 'producto_kpis'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    cabys = Column(String(50))
    nombre_clean = Column(String(300), nullable=False)
    
    # Basic metrics
    total_compras = Column(Float, default=0.0)
    total_ventas = Column(Float, default=0.0)
    stock_promedio = Column(Float, default=0.0)
    stock_final = Column(Float, default=0.0)  # FIXED: Added stock_final field
    costo_promedio = Column(Float, default=0.0)
    precio_promedio = Column(Float, default=0.0)
    
    # Rotation metrics
    rotacion = Column(Float, default=0.0)
    dio = Column(Float, default=0.0)  # Days Inventory Outstanding
    
    # Reorder metrics
    rop = Column(Float, default=0.0)  # Reorder Point
    stock_seguridad = Column(Float, default=0.0)
    
    # Coverage analysis
    cobertura_dias = Column(Float, default=0.0)
    exceso = Column(Integer, default=0)  # 1 if excess inventory
    faltante = Column(Integer, default=0)  # 1 if shortage
    
    # ABC/XYZ Classification
    clasificacion_abc = Column(String(1))  # A, B, C
    clasificacion_xyz = Column(String(1))  # X, Y, Z
    
    # Calculation period
    fecha_inicio = Column(Date)
    fecha_fin = Column(Date)

class KpiSummary(Base):
    """Summary KPIs for dashboard"""
    __tablename__ = 'kpi_summary'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    fecha_calculo = Column(Date, nullable=False)
    total_productos = Column(Integer, default=0)
    valor_inventario = Column(Float, default=0.0)
    rotacion_promedio = Column(Float, default=0.0)
    dio_promedio = Column(Float, default=0.0)
    productos_exceso = Column(Integer, default=0)
    productos_faltante = Column(Integer, default=0)
    
    # ABC Distribution
    productos_a = Column(Integer, default=0)
    productos_b = Column(Integer, default=0)
    productos_c = Column(Integer, default=0)
    
    # XYZ Distribution
    productos_x = Column(Integer, default=0)
    productos_y = Column(Integer, default=0)
    productos_z = Column(Integer, default=0)

# Indexes for efficient queries
Index('idx_compras_norm_clean', ComprasNormalized.nombre_clean)
Index('idx_compras_norm_fecha', ComprasNormalized.fecha)
Index('idx_ventas_norm_clean', VentasNormalized.nombre_clean)
Index('idx_ventas_norm_fecha', VentasNormalized.fecha)
Index('idx_cdet_key', ComprasDetail.cabys, ComprasDetail.nombre_clean)
Index('idx_vdet_key', VentasDetail.cabys, VentasDetail.nombre_clean)
Index('idx_kpi_mov_fecha_prod', KpiMovDiario.fecha, KpiMovDiario.nombre_clean)
Index('idx_producto_kpis_clean', ProductoKpis.nombre_clean)

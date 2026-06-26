import pandas as pd
import re
from utils.logger import get_logger
from connectors.odoo import get_odoo_client
from extractors.odoo.base import fetch_odoo_data
# 🚀 REUTILIZAMOS TUS UTILS CENTRALIZADOS
from transform.utils import (
    extract_many2one_id,
    extract_many2one_name
)

# Configuración del Logger para trazabilidad en consola
logger = get_logger("transform_facturas_sheets")

# =====================================================================
# FUNCIÓN PRINCIPAL DE TRANSFORMACIÓN
# =====================================================================

def transform_invoices_sheets(invoices_raw):
    """
    Procesa las cabeceras de facturas crudas de Odoo, calcula impuestos,
    asigna signos contables según el tipo de documento (Facturas, Boletas,
    Notas de Crédito/Débito) y devuelve un DataFrame listo para Google Sheets.
    """
    if not invoices_raw:
        logger.warning("No se recibieron datos crudos de Odoo para transformar.")
        return pd.DataFrame()

    # Copia explícita para evitar advertencias de SettingWithCopyWarning en Pandas
    df = pd.DataFrame(invoices_raw).copy()
        
    # =====================================================================
    # 1. FILTRADO INICIAL Y DESEMPAQUETADO MANY2ONE
    # =====================================================================
    # Mantenemos solo los documentos validados/publicados ('posted')
    if "state" in df.columns:
        df = df.rename(columns={"state": "estado"})
    if "estado" in df.columns:
        df = df[df["estado"] == "posted"]

    # FILTRO DE CONTROL DE OPERACIÓN NACIONAL:
    # Solo nos interesan flujos de venta/crédito de clientes nacionales
    if "move_type" in df.columns:
        df = df[df["move_type"].isin(["out_invoice", "out_refund"])]

    # Desempaquetamos relaciones Many2one usando tus funciones estandarizadas
    df["tipo_documento_str"] = df["l10n_latam_document_type_id"].apply(extract_many2one_name)
    
    # ESCUDO TOTAL: Barremos Importaciones, Exportaciones y Comercio Exterior (Códigos SII 110, 111, 112, etc.)
    if not df.empty and "tipo_documento_str" in df.columns:
        palabras_clave_excluir = [
            "IMPORTACIÓN", "IMPORTACION", 
            "EXPORTACIÓN", "EXPORTACION", 
            "EXTRANJERO", "DIN", "110", "111", "112"
        ]
        patron_excluir = "|".join(palabras_clave_excluir)
        
        # Filtramos dejando fuera cualquier fila que contenga estas palabras en el tipo de documento
        df = df[~df["tipo_documento_str"].str.upper().str.contains(patron_excluir, na=False)]

    # Si después de limpiar el comercio exterior el DataFrame quedó vacío, cortamos de inmediato
    if df.empty:
        logger.warning("Luego de filtrar importaciones y exportaciones, no quedaron registros para procesar.")
        return pd.DataFrame()
        
    # Limpieza de registros nulos o duplicados por ID de base de datos
    if "id" in df.columns:
        df = df.dropna(subset=["id"]).drop_duplicates(subset=["id"])

    # Continuamos con el resto de desempaquetados...
    df["Empresa"] = df["partner_id"].apply(extract_many2one_name)
    df["Vendedor"] = df["invoice_user_id"].apply(extract_many2one_name)
    df["condicion_pago_str"] = df["invoice_payment_term_id"].apply(extract_many2one_name)
    df["partner_db_id"] = df["partner_id"].apply(extract_many2one_id)

    # Limpieza de registros nulos o duplicados por ID de base de datos
    if "id" in df.columns:
        df = df.dropna(subset=["id"]).drop_duplicates(subset=["id"])

    # Desempaquetamos relaciones Many2one usando tus funciones estandarizadas
    df["Empresa"] = df["partner_id"].apply(extract_many2one_name)
    df["Vendedor"] = df["invoice_user_id"].apply(extract_many2one_name)
    df["tipo_documento_str"] = df["l10n_latam_document_type_id"].apply(extract_many2one_name)
    df["condicion_pago_str"] = df["invoice_payment_term_id"].apply(extract_many2one_name)
    df["partner_db_id"] = df["partner_id"].apply(extract_many2one_id)

    # =====================================================================
    # 2. SUB-CONSULTA DINÁMICA DE RUTs (RES.PARTNER)
    # =====================================================================
    partner_ids_unicos = df["partner_db_id"].dropna().unique().tolist()
    ruts_mapping = {}

    if partner_ids_unicos:
        try:
            logger.info(f"Buscando RUTs para {len(partner_ids_unicos)} partners únicos en res.partner...")
            client = get_odoo_client()
            partners_data = fetch_odoo_data(
                client=client,
                model_name="res.partner",
                fields=["id", "vat"],
                domain=[("id", "in", partner_ids_unicos)]
            )
            if partners_data:
                for p in partners_data:
                    ruts_mapping[p['id']] = p.get('vat') if p.get('vat') else ""
        except Exception as e:
            logger.error(f"No se pudieron mapear los RUTs desde res.partner: {e}")

    # Mapeamos el RUT al DataFrame principal
    df["RUT Nº"] = df["partner_db_id"].map(ruts_mapping).fillna("")

    # =====================================================================
    # 3. ASIGNACIÓN DE VALORES REALES Y SIGNOS CONTABLES (Odoo Native)
    # =====================================================================
    # Forzamos conversión a tipo numérico de las columnas financieras oficiales de Odoo
    for col in ["amount_tax", "amount_untaxed", "amount_total"]:
        df[col] = pd.to_numeric(df.get(col, 0.0), errors="coerce").fillna(0.0)

    # Creamos las columnas definitivas para Google Sheets usando la data oficial de Odoo
    df["Importe sin impuestos con signo"] = df["amount_untaxed"]
    df["Impuestos con signo"] = df["amount_tax"]
    df["Total con signo"] = df["amount_total"]

    if "move_type" in df.columns:
        # Regla de Negocio: Las Notas de Crédito ('out_refund') RESTAN al reporte general.
        # Facturas, Boletas y Notas de Débito SUMAN.
        es_nota_credito = df["move_type"] == "out_refund"
        
        columnas_reporte_financiero = [
            "Importe sin impuestos con signo", 
            "Impuestos con signo", 
            "Total con signo"
        ]
        
        # Multiplicamos por -1 TODO el bloque financiero si es Nota de Crédito
        for col in columnas_reporte_financiero:
            df.loc[es_nota_credito, col] = df.loc[es_nota_credito, col] * -1

    # =====================================================================
    # 4. VALIDACIÓN DE CUADRATURA HORIZONTAL (Neto + IVA == Total)
    # =====================================================================
    # En Chile, por regla de negocio estricta: Neto + IVA debe ser igual al Total de la fila.
    # Si Odoo tiene desfases de 1 peso por redondeos internos, lo ajustamos en el Neto de esa fila.
    df["suma_control"] = df["Importe sin impuestos con signo"] + df["Impuestos con signo"]
    df["descalce_fila"] = df["Total con signo"] - df["suma_control"]

    # Si hay descalce (ej: 1 peso de diferencia debido a redondeos), se lo sumamos al neto para forzar el match
    df["Importe sin impuestos con signo"] = df["Importe sin impuestos con signo"] + df["descalce_fila"]

    logger.info("Cuadratura horizontal completada usando montos nativos de Odoo.")

    # =====================================================================
    # 5. MAQUEO DE NOMBRES DE DOCUMENTOS Y LIMPIEZA FINAL
    # =====================================================================
    if "invoice_origin" in df.columns:
        df["invoice_origin"] = df["invoice_origin"].apply(lambda x: "" if x is False or x is None else str(x))

    df = df.rename(columns={
        "name": "Número",
        "invoice_date": "Fecha de Factura/Recibo",
        "invoice_date_due": "Fecha de vencimiento",
        "invoice_origin": "Origen",
        "condicion_pago_str": "Plazos de pago"
    })

    def clasificar_tipo_documento(row):
        num = str(row.get("Número", "")).upper()
        m_type = row.get("move_type", "")
        tipo_str = str(row.get("tipo_documento_str", "")).upper()
        
        # 1. Notas de Crédito
        if m_type == "out_refund" or "N/C" in num or "REF" in num:
            return "Nota de Crédito"
            
        # 2. Notas / Boletas de Débito
        elif m_type == "out_charge" or "DEB" in num or "N/D" in num:
            return "Boleta de Débito" if "BOL" in num or "BOLETA" in tipo_str else "Nota de Débito"
            
        # 3. Facturas Electrónicas (Buscamos prefijos específicos como FAC)
        elif "FAC" in num or "FE" in num or "FACTURA" in tipo_str:
            return "Factura Electrónica"
            
        # 4. Todo lo demás que sea venta directa out_invoice es Boleta Electrónica
        else:
            return "Boleta Electrónica"

    df["Tipo de Documento/Nombre"] = df.apply(clasificar_tipo_documento, axis=1)

    if "Tipo de Documento/Nombre" in df.columns:
        df["Tipo de Documento/Nombre"] = df["Tipo de Documento/Nombre"].apply(
            lambda x: re.sub(r'^\(\d+\)\s*', '', str(x)) if x else ""
        )

    text_fields = ["Número", "Tipo de Documento/Nombre", "RUT Nº", "Empresa", "Vendedor", "Origen", "Plazos de pago"]
    for field in text_fields:
        if field in df.columns:
            df[field] = df[field].apply(lambda x: "" if x is False or x is None else str(x)).str.strip()

    date_cols = ["Fecha de Factura/Recibo", "Fecha de vencimiento"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.strftime('%Y-%m-%d').fillna("")

    columnas_reporte = [
        "Número", "Tipo de Documento/Nombre", "RUT Nº", "Empresa", 
        "Fecha de Factura/Recibo", "Fecha de vencimiento", "Vendedor", "Origen", 
        "Importe sin impuestos con signo", "Impuestos con signo", "Total con signo", "Plazos de pago"
    ]
    df = df.reindex(columns=columnas_reporte)

    return df
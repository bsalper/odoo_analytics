import pandas as pd
import numpy as np
from utils.logger import get_logger
from connectors.odoo import get_odoo_client
from extractors.odoo.base import fetch_odoo_data
from transform.utils import (
    extract_many2one_id,
    extract_many2one_name,
    extraer_sku_y_nombre
)

logger = get_logger("transform_facturas_detalle")

def transform_facturas_detalle(detail_raw, cabeceras_raw=None):
    """
    Transforma las líneas de detalle de Odoo para Google Sheets.
    Versión con doble verificación para forzar la aparición de Notas de Débito
    y corrección de Totales Brutos (con IVA) para cuadrar con Cabeceras.
    """
    if not detail_raw:
        logger.warning("No se recibieron líneas de detalle crudas para transformar.")
        return pd.DataFrame()

    df_lineas = pd.DataFrame(detail_raw).copy()

    # 1. Desempaquetado estándar
    df_lineas["invoice_id"] = df_lineas["move_id"].apply(extract_many2one_id)
    df_lineas["Número"] = df_lineas["move_id"].apply(extract_many2one_name)
    df_lineas["Empresa"] = df_lineas["partner_id"].apply(extract_many2one_name) if "partner_id" in df_lineas.columns else ""
    df_lineas["partner_db_id"] = df_lineas["partner_id"].apply(extract_many2one_id) if "partner_id" in df_lineas.columns else None

    # Separación de SKU y Producto
    if "product_id" in df_lineas.columns:
        producto_completo = df_lineas["product_id"].apply(extract_many2one_name)
        df_lineas["Código"] = producto_completo.apply(lambda x: extraer_sku_y_nombre(x)[0])
        df_lineas["Producto"] = producto_completo.apply(lambda x: extraer_sku_y_nombre(x)[1])
    else:
        df_lineas["Código"] = ""
        df_lineas["Producto"] = ""

    # =====================================================================
    # 2. CRUCE MEJORADO CON CABECERAS (Forzando tipo de dato numérico)
    # =====================================================================
    df_lineas["Vendedor"] = ""
    df_lineas["Origen"] = ""
    df_lineas["tipo_documento_cabecera"] = None

    if cabeceras_raw:
        df_cabeceras = pd.DataFrame(cabeceras_raw).copy()
        if not df_cabeceras.empty and "id" in df_cabeceras.columns:
            df_cabeceras["tipo_documento_str"] = df_cabeceras["l10n_latam_document_type_id"].apply(extract_many2one_name)
            df_cabeceras["Vendedor"] = df_cabeceras["invoice_user_id"].apply(extract_many2one_name) if "invoice_user_id" in df_cabeceras.columns else ""
            df_cabeceras["Origen"] = df_cabeceras["invoice_origin"].apply(lambda x: str(x) if x and x is not False else "") if "invoice_origin" in df_cabeceras.columns else ""
            
            def clasificar_tipo_documento(row):
                num = str(row.get("name", "")).upper()
                m_type = row.get("move_type", "")
                tipo_str = str(row.get("tipo_documento_str", "")).upper()
                
                if m_type == "out_refund" or "N/C" in num or "REF" in num or "CREDIT" in tipo_str:
                    return "Nota de Crédito"
                elif "N/D" in num or "DEB" in num or "AMD" in num or "DEBITO" in tipo_str or "DÉBITO" in tipo_str or "56" in tipo_str:
                    return "Nota de Débito"
                elif "FAC" in num or "FE" in num or "FACTURA" in tipo_str or "33" in tipo_str:
                    return "Factura Electrónica"
                else:
                    return "Boleta Electrónica"

            df_cabeceras["tipo_documento_cabecera"] = df_cabeceras.apply(clasificar_tipo_documento, axis=1)
            
            # Forzamos INT numérico en ambos dataframes para asegurar un Merge perfecto
            df_cabeceras["invoice_id"] = pd.to_numeric(df_cabeceras["id"], errors="coerce").fillna(0).astype(int)
            df_lineas["invoice_id"] = pd.to_numeric(df_lineas["invoice_id"], errors="coerce").fillna(0).astype(int)

            logger.info(f"Tipos calculados en Cabecera para cruzar al Detalle: {df_cabeceras['tipo_documento_cabecera'].value_counts().to_dict()}")

            df_cabeceras_mapeo = df_cabeceras[["invoice_id", "Vendedor", "Origen", "tipo_documento_cabecera"]]
            df_lineas = df_lineas.drop(columns=["Vendedor", "Origen"], errors="ignore")
            
            df_lineas = pd.merge(df_lineas, df_cabeceras_mapeo, on="invoice_id", how="left")

    if "Vendedor" not in df_lineas.columns: df_lineas["Vendedor"] = ""
    if "Origen" not in df_lineas.columns: df_lineas["Origen"] = ""

    # =====================================================================
    # 3. CONSULTA DE RUTs
    # =====================================================================
    if "partner_db_id" in df_lineas.columns:
        partner_ids_unicos = df_lineas["partner_db_id"].dropna().unique().tolist()
        ruts_mapping = {}
        if partner_ids_unicos:
            try:
                client = get_odoo_client()
                partners_data = fetch_odoo_data(
                    client=client, model_name="res.partner", fields=["id", "vat"],
                    domain=[("id", "in", partner_ids_unicos)]
                )
                if partners_data:
                    for p in partners_data:
                        ruts_mapping[p['id']] = p.get('vat') if p.get('vat') else ""
            except Exception as e:
                logger.error(f"Error consultando RUTs: {e}")
        df_lineas["RUT Nº"] = df_lineas["partner_db_id"].map(ruts_mapping).fillna("")
    else:
        df_lineas["RUT Nº"] = ""

    # =====================================================================
    # 4. ASIGNACIÓN ASESINA DEL TIPO DE DOCUMENTO (Con Fallback ultra-seguro)
    # =====================================================================
    def determinar_tipo_final(row):
        # Prioridad 1: Si heredó bien de la cabecera tras el merge correcto
        cab_type = row.get("tipo_documento_cabecera")
        if cab_type and pd.notna(cab_type) and str(cab_type).strip() != "":
            return cab_type
            
        # Prioridad 2: Fallback por texto del folio de la línea por si falló el cruce
        num_str = str(row.get("Número", "")).upper()
        
        if "N/C" in num_str or "REF" in num_str:
            return "Nota de Crédito"
        elif "N/D" in num_str or "DEB" in num_str or "AMD" in num_str:
            return "Nota de Débito"
        elif "FAC" in num_str or "FE" in num_str:
            return "Factura Electrónica"
        else:
            return "Boleta Electrónica"

    df_lineas["Tipo de Documento/Nombre"] = df_lineas.apply(determinar_tipo_final, axis=1)
    
    logger.info(f"Conteo FINAL de filas por documento en DETALLE a subir: {df_lineas['Tipo de Documento/Nombre'].value_counts().to_dict()}")

    # =====================================================================
    # 5. PROCESAMIENTO NUMÉRICO (Corrección de Total Bruto con IVA)
    # =====================================================================
    df_lineas["Cantidad"] = pd.to_numeric(df_lineas["quantity"], errors="coerce").fillna(0.0).abs()
    df_lineas["Precio Unit"] = pd.to_numeric(df_lineas["price_unit"], errors="coerce").fillna(0.0).abs()
    df_lineas["Descuento"] = pd.to_numeric(df_lineas["discount"], errors="coerce").fillna(0.0).abs()
    
    # Intentamos extraer price_total (Bruto con IVA) en lugar del Neto
    if "price_total" in df_lineas.columns:
        df_lineas["Total"] = pd.to_numeric(df_lineas["price_total"], errors="coerce").fillna(0.0).abs()
    else:
        logger.warning("El campo 'price_total' no está en detail_raw. Aplicando IVA matemático provisional.")
        # Fallback por si la API no extrae price_total: multiplicamos subtotal neto * 1.19
        df_lineas["Total"] = (pd.to_numeric(df_lineas["price_subtotal"], errors="coerce").fillna(0.0).abs() * 1.19).round(0)

    df_lineas["Precio Sub"] = (df_lineas["Cantidad"] * df_lineas["Precio Unit"]).round(0)
    df_lineas["Coste Unit"] = pd.to_numeric(df_lineas.get("cost_price", 0.0), errors="coerce").fillna(0.0).abs()

    # Inversión de signo únicamente para Notas de Crédito
    es_nota_credito = df_lineas["Tipo de Documento/Nombre"] == "Nota de Crédito"
    for col in ["Cantidad", "Precio Sub", "Total"]:
        df_lineas.loc[es_nota_credito, col] = df_lineas.loc[es_nota_credito, col] * -1

    # =====================================================================
    # 6. FORMATEO Y REINDEXACIÓN
    # =====================================================================
    if "date" in df_lineas.columns:
        df_lineas["Fecha de Factura/Recibo"] = pd.to_datetime(df_lineas["date"], errors="coerce").dt.strftime('%Y-%m-%d').fillna("")
    else:
        df_lineas["Fecha de Factura/Recibo"] = ""

    text_fields = ["Número", "Tipo de Documento/Nombre", "RUT Nº", "Empresa", "Vendedor", "Origen", "Código", "Producto"]
    for field in text_fields:
        if field in df_lineas.columns:
            df_lineas[field] = df_lineas[field].apply(lambda x: "" if x is False or x is None else str(x)).str.strip()

    columnas_foto = [
        "Número", "Tipo de Documento/Nombre", "RUT Nº", "Empresa", "Fecha de Factura/Recibo",
        "Vendedor", "Origen", "Código", "Producto", "Cantidad", "Descuento", 
        "Precio Unit", "Precio Sub", "Total", "Coste Unit"
    ]
    df_lineas = df_lineas.reindex(columns=columnas_foto)
    
    return df_lineas
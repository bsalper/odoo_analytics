CLIENT_FIELDS = [
    'id', 'name', 'company_type', 'type', 'vat', 'user_id',
    'visit_day', 'street', 'city', 'email', 'phone',
    'create_date', 'property_payment_term_id', 'credit_limit',
    'property_product_pricelist', 'partner_latitude',
    'partner_longitude', 'category_id'
]

ORDER_FIELDS = [
    'id', 'name', 'create_date', 'partner_id', 'user_id',
    'amount_untaxed', 'amount_tax', 'amount_total',
    'main_exception_id', 'note', 'date_order', 'state',
    'invoice_status', 'partner_shipping_id'
]

ORDER_LINE_FIELDS = [
    'id', 'order_id', 'create_date', 'product_id',
    'product_uom_qty', 'price_unit', 'price_subtotal'
]

PRODUCT_FIELDS = [
    'id', 'default_code', 'name', 'uom_id',
    'list_price', 'standard_price', 'create_date',
    'taxes_id', 'categ_id', 'sale_ok'
]

TAX_FIELDS = [
    'id',
    'name',
    'amount'
]

INVOICE_FIELDS = [
    "id", "name", "state", "l10n_latam_document_number", "invoice_date_due",
    "l10n_latam_document_type_id", "invoice_date", "create_date",
    "partner_id", "invoice_user_id", "amount_untaxed", "amount_tax", "default_code",
    "amount_total", "amount_residual", "invoice_origin", "invoice_payment_term_id",
    "payment_state", "partner_shipping_id", "preferred_payment_method_line_id"
]

INVOICE_LINE_FIELDS = [
    "id", "move_id", "product_id", "quantity", "discount",
    "product_uom_id", "price_unit", "price_subtotal", "price_total", 
    "tax_ids", "account_id", "name"
]
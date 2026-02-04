from connectors.odoo import get_odoo_client
from extractors.odoo.invoices import get_invoice_lines_raw
import pandas as pd

def run():
    odoo = get_odoo_client()

    invoice_lines = get_invoice_lines_raw(odoo)

    df = pd.DataFrame(invoice_lines)

    print("Total invoice lines:", len(df))
    print("=" * 60)

    df = df.sort_values("id", ascending=False)

    sample = df[["id", "move_id", "tax_ids", "price_unit", "price_subtotal"]].head(100)

    print(sample)

    print("=" * 60)
    print("Resumen tax_ids (cantidad de impuestos por línea):")
    print(
        sample["tax_ids"]
        .apply(lambda x: len(x) if isinstance(x, list) else 0)
        .value_counts()
    )

if __name__ == "__main__":
    run()

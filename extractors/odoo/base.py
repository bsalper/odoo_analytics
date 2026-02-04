#
from utils.logger import get_logger

logger = get_logger("odoo_extractor")

def fetch_odoo_data(client, model_name, fields, limit=None, domain=None, batch_size=1000):
    domain = domain or []
    all_data = []
    offset = 0

    logger.info(f"Extrayendo modelo Odoo: {model_name}")

    try:
        while True:
            data = client.search_read(
                model_name,
                domain=domain,
                fields=fields,
                limit=batch_size,
                offset=offset
            )

            if not data:
                break

            all_data.extend(data)
            offset += batch_size

            if limit and len(all_data) >= limit:
                break
            
        logger.info(f"{model_name}: {len(all_data)} registros obtenidos")
        return all_data[:limit] if limit else all_data

    except Exception as e:
        logger.error(f"Error extrayendo {model_name}: {e}")
        return []
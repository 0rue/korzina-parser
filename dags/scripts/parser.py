import requests
from datetime import datetime
from clickhouse_driver import Client
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def parse_and_insert():
    base_api_url = 'https://api.korzinavdom.kz/client/showcases'
    page = 0
    size = 100  
    all_items = []

    while True:
        url = f"{base_api_url}?page={page}&size={size}"
        logger.info(f"Запрос к API: {url}")
        response = requests.get(url)

        if response.status_code != 200:
            logger.warning(f"Ошибка запроса: статус {response.status_code}")
            break

        data = response.json()
        products = data.get("data", {}).get("page", {}).get("content", [])

        if not products:
            logger.info("Продукты не найдены, завершение.")
            break

        for product in products:
            if not isinstance(product, dict):
                logger.warning(f"Пропущен продукт: {product}")
                continue

            try:

                item = {
                    'product_name': product.get('productName'),
                    'article': str(product.get('productNumber', 'нет артикула')),
                    'price': int(product.get('productPrice', {}).get('current', 0)),
                    'product_url': f"https://korzinavdom.kz/product/{product.get('productNumber')}",
                    'parsed_at': datetime.now()
                }
                all_items.append(item)
            except Exception as e:
                logger.error(f"Ошибка при обработке продукта: {e}")

        logger.info(f"Страница {page} обработана, товаров: {len(products)}")
        page += 1

    logger.info("Подключение к ClickHouse...")

    client = Client(host='clickhouse')
    client.execute('''
        CREATE TABLE IF NOT EXISTS korzina_products (
            product_name String,
            article String,
            price Int32,
            product_url String,
            parsed_at DateTime
        ) ENGINE = MergeTree() ORDER BY parsed_at
    ''')

    data_to_insert = [
        (
            item['product_name'], item['article'], item['price'],
            item['product_url'], item['parsed_at']
        )
        for item in all_items if item['product_name'] and item['price'] is not None
    ]

    if data_to_insert:
        client.execute('''
            INSERT INTO korzina_products 
            (product_name, article, price, product_url, parsed_at)
            VALUES
        ''', data_to_insert)
        logger.info(f"Успешно вставлено {len(data_to_insert)} товаров.")
    else:
        logger.warning("Нет данных для вставки.")
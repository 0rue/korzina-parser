import requests
from bs4 import BeautifulSoup
from datetime import datetime
from clickhouse_driver import Client
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

logger = LoggingMixin().log

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def parse_and_insert():
    base_url = 'https://korzinavdom.kz/catalog'
    page = 1
    all_items = []

    while True:
        url = f"{base_url}?PAGEN_1={page}"
        logger.info(f"Запрашиваем страницу: {url}")

        response = requests.get(url)
        if response.status_code != 200:
            logger.warning(f"Страница {url} вернула статус {response.status_code}. Завершаем.")
            break

        soup = BeautifulSoup(response.text, 'html.parser')
        products = soup.select('app-product-card')

        if not products:
            logger.info("Продукты не найдены, завершение парсинга.")
            break

        for link in products:
            product_url = 'https://korzinavdom.kz' + link['href']
            try:
                product_resp = requests.get(product_url)
                if product_resp.status_code != 200:
                    logger.warning(f"Ошибка запроса {product_url}: статус {product_resp.status_code}")
                    continue

                prod_soup = BeautifulSoup(product_resp.text, 'html.parser')

                name_el = prod_soup.select_one('p.description-block__name')
                price_el = prod_soup.select_one('p.sum-block__total')
                article_el = prod_soup.select_one('p.tab-content__article-item')

                item = {
                    'product_name': name_el.text.strip() if name_el else None,
                    'article': article_el.text.strip() if article_el else None,
                    'price': int(price_el.text.strip().replace('₸', '').replace(' ', '').replace('\xa0', '')) if price_el else None,
                    'product_url': product_url,
                    'parsed_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                }

                all_items.append(item)

            except Exception as e:
                logger.error(f"Ошибка при обработке {product_url}: {str(e)}")

        logger.info(f"Обработана страница {page}, найдено товаров: {len(products)}")
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


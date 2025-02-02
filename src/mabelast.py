import re
import time
import threading
from urllib.parse import urljoin
import pandas as pd
import requests
from bs4 import BeautifulSoup
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

console_handler = logging.StreamHandler()

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(console_handler)

BASE_URL = "https://www.drom.ru/catalog/"
FILENAME = "parser_dorm.xlsx"
THREAD_POOL_SIZE = 3  # Количество потоков
SAVE_INTERVAL = 20  # Интервал сохранения данных в секундах
MAX_RETRIES = 50  # Максимальное количество попыток при ошибке
REQUEST_TIMEOUT = 10  # Таймаут запроса в секундах
RETRY_DELAY = 5  # Задержка между попытками при ошибке в секундах

# Глобальный список для хранения данных
all_data = []
last_save_time = time.time()

# Блокировка для безопасной записи в файл
file_lock = threading.Lock()

def fetch_url(url):
    """
    Выполняет HTTP-запрос с таймаутом и повторными попытками.
    Обрабатывает ошибки 429, 503, 443 и другие.
    """
    for attempt in range(MAX_RETRIES):
        try:
            logging.info(f"Запрос к {url} (попытка {attempt + 1})")
            response = requests.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code == 200:
                return response
            else:
                logging.warning(f"Ошибка {response.status_code} при запросе к {url}")
                if response.status_code in [429, 503, 443]:  # Обрабатываем конкретные ошибки
                    logging.warning(f"Повторная попытка через {RETRY_DELAY} секунд...")
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error(f"Критическая ошибка {response.status_code}. Прерывание.")
                    return None
        except requests.exceptions.RequestException as e:
            logging.warning(f"Ошибка при запросе к {url}: {e}")
            time.sleep(RETRY_DELAY)  # Задержка между попытками

    logging.error(f"Не удалось выполнить запрос к {url} после {MAX_RETRIES} попыток")
    return None

def parse_catalog_page(url):
    logging.info(f"Запрос к каталогу: {url}")
    response = fetch_url(url)
    if not response:
        return

    logging.info("Каталог успешно получен. Обрабатываем страницы марок автомобилей.")
    soup = BeautifulSoup(response.text, "html.parser")

    car_list_div = soup.find("div", class_="css-1dk948p ehmqafe0")
    car_type_elem = soup.find("a", class_="_3ynq47a _3ynq47c _3ynq47d _3ynq47g _3ynq47k",
                              attrs={"aria-current": "true"})
    car_type = car_type_elem.get_text() if car_type_elem else "Неизвестно"

    car_brand_links = []
    if car_list_div:
        noscript_tag = car_list_div.find("noscript")
        if noscript_tag:
            noscript_soup = BeautifulSoup(noscript_tag.decode_contents(), "html.parser")
            car_brand_links.extend(noscript_soup.find_all("a"))  # Добавляем ссылки из <noscript>

    with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
        futures = []
        for car_brand_link in car_brand_links:
            car_brand = car_brand_link.get_text(strip=True)
            car_brand_url = urljoin(url, car_brand_link.get("href").strip())

            logging.info(f"Найдена марка: {car_brand}, ссылка: {car_brand_url}")
            data = {"ТИП": car_type, "Марка": car_brand}

            futures.append(executor.submit(parse_brand_page, car_brand_url, data))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Ошибка при обработке страницы бренда: {e}")

def parse_brand_page(car_brand_url, data):
    logging.info(f"Запрос к странице бренда: {car_brand_url}")
    response = fetch_url(car_brand_url)
    if not response:
        return

    logging.info("Страница бренда успешно получена. Обрабатываем модели автомобилей.")
    soup = BeautifulSoup(response.text, "html.parser")
    car_model_links = soup.find_all("a", attrs={"data-ga-stats-name": "model_from_list"},
                                    class_="g6gv8w4 g6gv8w8 _501ok20")

    with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
        futures = []
        for car_model_link in car_model_links:
            care_model = car_model_link.get_text()
            car_model_url = urljoin(car_brand_url, car_model_link.get("href").strip())

            model_data = data.copy()
            model_data["Модель"] = care_model

            futures.append(executor.submit(parse_car_model_page, car_model_url, model_data))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Ошибка при обработке страницы модели: {e}")

def parse_car_model_page(car_model_url, data):
    logging.info(f"Запрос к странице модели автомобиля: {car_model_url}")
    response = fetch_url(car_model_url)
    if not response:
        return

    logging.info("Страница модели успешно получена. Обрабатываем страны выпуска.")
    soup = BeautifulSoup(response.text, "html.parser")
    car_model_countries_html = soup.find_all("div", class_="css-18bfsxm e1ei9t6a4")

    with ThreadPoolExecutor(max_workers=THREAD_POOL_SIZE) as executor:
        futures = []
        for car_model_country_html in car_model_countries_html:
            country_soup = BeautifulSoup(str(car_model_country_html), "html.parser")
            country_name = country_soup.find("div", class_="css-112idg0 e1ei9t6a3")
            country_data = data.copy()
            country_data["Для какой страны"] = country_name.get_text()

            car_generation_links = country_soup.find_all(
                "a",
                class_="css-9ww0hv e1ei9t6a1 _1ms9k6p0 ftkewc0 _1ms9k6p2",
                attrs={"data-ftid": "component_article"}
            )

            for car_generation_link in car_generation_links:
                car_generation_url = car_generation_link.get("href").strip()
                description = car_generation_link.get_text()
                generation_data = country_data.copy()
                generation_data["Описание 1"] = description

                futures.append(executor.submit(parse_car_generation_page, urljoin(car_model_url, car_generation_url), generation_data))

        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Ошибка при обработке страницы поколения: {e}")

def parse_car_generation_page(car_generation_url, data):
    global last_save_time

    logging.info(f"Запрос к странице поколения автомобиля: {car_generation_url}")
    response = fetch_url(car_generation_url)
    if not response:
        return

    logging.info("Страница поколения успешно получена. Обрабатываем таблицу с комплектациями.")
    soup = BeautifulSoup(response.text, "html.parser")
    table = soup.find('table', {'class': 'b-table b-table_text-left'})

    data["Общее"] = "-"
    data["Комплектация"] = '-'
    data["Период выпуска"] = '-'
    data["Рекомендованная цена, руб."] = '-'
    data["Марка двигателя"] = '-'
    data["Марка кузова"] = '-'

    if not table:
        with file_lock:
            all_data.append(data.copy())
        return

    table_soup = BeautifulSoup(str(table), "html.parser")
    table = table_soup.find('table')
    rows = table.find_all('tr')
    headers = [header.text.strip() for header in rows[0].find_all('th')]  # или 'td', если заголовки в ячейках <td>

    for row in rows[1:]:
        columns = row.find_all('td')
        th = row.find('th', colspan=True)
        if th:
            data["Общее"] = th.text.strip()
            continue

        for i, column in enumerate(columns):
            if i < len(headers):
                header = headers[i]
                if header and (not "Сравнить" in header):
                    value = column.text.strip()
                    value = re.sub(r'\s+', ' ', value)
                    data[header] = value

        with file_lock:
            all_data.append(data.copy())

        data["Комплектация"] = '-'
        data["Период выпуска"] = '-'
        data["Рекомендованная цена, руб."] = '-'
        data["Марка двигателя"] = '-'
        data["Марка кузова"] = '-'
        data["Цена*"] = "-"
    data["Общее"] = "-"

    # Периодическое сохранение данных
    if time.time() - last_save_time >= SAVE_INTERVAL:
        save_data_to_excel()
        last_save_time = time.time()

def save_data_to_excel():
    with file_lock:
        df = pd.DataFrame(all_data)
        try:
            df.to_excel(FILENAME, index=False, engine="openpyxl")
            logging.info(f"Данные сохранены в файл {FILENAME}")
        except Exception as e:
            logging.error(f"Ошибка при сохранении файла: {e}")

if __name__ == "__main__":
    try:
        logging.info("Запуск парсинга каталога...")
        parse_catalog_page(BASE_URL)
        BASE_URL = "https://www.drom.ru/catalog/lcv/"
        parse_catalog_page(BASE_URL)
        save_data_to_excel()
        logging.info("Парсинг завершен.")
    except KeyboardInterrupt:
        logging.info("Программа прервана пользователем. Сохраняем данные перед выходом...")
        save_data_to_excel()
        logging.info("Данные сохранены. Завершение работы.")
    except Exception as e:
        logging.error(f"Произошла ошибка: {e}")
        save_data_to_excel()
        logging.info("Данные сохранены. Завершение работы.")
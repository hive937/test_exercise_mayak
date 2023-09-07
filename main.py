import os
import logging
import pandas as pd
import sqlite3
import requests
from lxml import html
from telegram import Update
from telegram.ext import Updater, CommandHandler, MessageHandler, Filters, ConversationHandler, CallbackContext
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")

if BOT_TOKEN is None:
    raise ValueError("Токен бота не найден в файле .env")

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)

UPLOAD, PROCESSING = range(2)

conn = sqlite3.connect('websites.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS websites (
        id INTEGER PRIMARY KEY,
        name TEXT,
        url TEXT,
        xpath TEXT,
        data TEXT
    )
''')
conn.commit()
conn.close()

last_uploaded_data = []


def start(update: Update, context: CallbackContext) -> int:
    update.message.reply_text(
        "Привет! Пожалуйста, загрузите Excel-файл с данными о веб-сайтах или используйте /get_data для получения всей имеющейся информации."
        "Используйте /average_price для получения средней цены товара по каждому сайту.")
    return UPLOAD


def upload_file(update: Update, context: CallbackContext) -> int:
    user = update.message.from_user
    file = update.message.document
    file_extension = os.path.splitext(file.file_name)[-1].lower()

    if file_extension not in ('.xls', '.xlsx'):
        update.message.reply_text("Пожалуйста, загрузите действительный Excel-файл.")
        return UPLOAD

    file.get_file().download('uploaded_file.xlsx')

    try:
        df = pd.read_excel('uploaded_file.xlsx')
    except Exception as e:
        update.message.reply_text(f"Ошибка обработки Excel-файла: {str(e)}")
        return UPLOAD

    global last_uploaded_data
    last_uploaded_data = []

    data_list = []

    for index, row in df.iterrows():
        name = row['name']
        url = row['url']
        xpath = row['xpath']

        with ThreadPoolExecutor() as executor:
            future = executor.submit(process_url, url, xpath)
            data = future.result()

        data_list.append((name, url, xpath, data))

    conn = sqlite3.connect('websites.db')
    cursor = conn.cursor()
    cursor.executemany('INSERT INTO websites (name, url, xpath, data) VALUES (?, ?, ?, ?)', data_list)
    conn.commit()
    conn.close()

    response = "\n".join([f"{name}: {data}" for name, _, _, data in data_list])
    update.message.reply_text(response)

    return UPLOAD


def process_url(url, xpath):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            content = response.content
            tree = html.fromstring(content)
            data_element = tree.xpath(xpath)
            if data_element:
                data = data_element[0].text_content().strip()
            else:
                data = "Данные не найдены"
        else:
            data = "Ошибка при получении данных с веб-сайта"
    except Exception as e:
        data = str(e)

    return data


def get_data(update: Update, context: CallbackContext) -> None:
    conn = sqlite3.connect('websites.db')
    cursor = conn.cursor()
    cursor.execute('SELECT name, url, xpath, data FROM websites')
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        update.message.reply_text("В базе данных нет доступной информации.")
        return

    data_list = [f"Name: {name}\nURL: {url}\nXPath: {xpath}\nData: {data}" for name, url, xpath, data in rows]
    if last_uploaded_data:
        data_list.extend([f"Name: {name}\nURL: {url}\nXPath: {xpath}\nData: {data}" for name, url, xpath, data in
                          last_uploaded_data])
    response = "\n\n".join(data_list)
    update.message.reply_text("Имеющаяся информация:\n" + response)


def average_price(update: Update, context: CallbackContext) -> None:
    conn = sqlite3.connect('websites.db')
    cursor = conn.cursor()
    cursor.execute('SELECT name, data FROM websites')
    rows = cursor.fetchall()

    prices = {}

    if not rows:
        update.message.reply_text("В базе данных нет доступной информации.")
        return

    for name, data in rows:
        if name not in prices:
            prices[name] = []

        try:
            cleaned_data = data.replace("$", "").replace("€", "").replace("£", "").replace("₽", "").strip()
            price = float(cleaned_data)
            prices[name].append(price)
        except ValueError:
            pass

    avg_prices = []
    for name, price_list in prices.items():
        if price_list:
            avg_price = sum(price_list) / len(price_list)
            avg_prices.append(f"Name: {name}\nAverage Price: {avg_price:.2f}")

    conn.close()

    if not avg_prices:
        update.message.reply_text("Средние цены не найдены.")
        return

    response = "\n\n".join(avg_prices)
    update.message.reply_text("Средние цены по каждому сайту:\n" + response)


def main():
    updater = Updater(token=BOT_TOKEN, use_context=True)
    dp = updater.dispatcher

    conv_handler = ConversationHandler(
        entry_points=[CommandHandler("start", start)],
        states={
            UPLOAD: [MessageHandler(Filters.document, upload_file)],
        },
        fallbacks=[],
    )
    dp.add_handler(conv_handler)

    dp.add_handler(CommandHandler("get_data", get_data))
    dp.add_handler(CommandHandler("average_price", average_price))

    updater.start_polling()
    updater.idle()


if __name__ == '__main__':
    main()

import math
import logging
import uvicorn
from datetime import datetime
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from typing import Optional, List
from pydantic import BaseModel, Field
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from pymongo import MongoClient

from mongo_db.mongo_tools import MongoDb
from telegram_decode.telegram_factory import TelegramFactory

logging.basicConfig(level=logging.ERROR, filename='app.log', filemode='a',
                    format='%(name)s - %(levelname)s - %(message)s')



MONGO_URL='mongodb://mongo:27017/'
client = MongoClient(MONGO_URL)
db = client["telegram"]

def clean_data(data):
    """Замінює несумісні з JSON значення."""
    if isinstance(data, dict):
        for key, value in data.items():
            data[key] = clean_data(value)
    elif isinstance(data, list):
        data = [clean_data(item) for item in data]
    elif isinstance(data, float) and (data == float("inf") or data == float("-inf") or data != data):
        data = None
    return data

def clean_nan_values(data):
    if isinstance(data, dict):
        return {k: clean_nan_values(v) for k, v in data.items()}
    elif isinstance(data, list):
        return [clean_nan_values(i) for i in data]
    elif isinstance(data, float) and math.isnan(data):
        return None
    return data

def download_and_process_telegrams(country_code):
    processor = TelegramFactory.create_processor(country_code=country_code)
    df_result = processor.process_telegrams()

    documents = df_result.to_dict('records')

    db_manager = MongoDb().db_manager

    for document in documents:
        id_telegram = document["id_telegram"]
        data_for_mongo = {"id_telegram": id_telegram, "data": document}
        data_for_mongo["data"].pop("id_telegram", None)
        collection = db_manager.get_or_create_collection(country_code)
        print(data_for_mongo)
        db_manager.insert_or_update_document(collection, data_for_mongo)
        print(f"Processed telegrams for {country_code}: {len(documents)} records")


# Створення планувальника задач
scheduler = BackgroundScheduler()

# Додавання задач до планувальника з використанням CronTrigger
# Додавання задачі для білорусі
scheduler.add_job(download_and_process_telegrams, args=['bel'],
                  trigger=CronTrigger(hour='0,3,6,9,12,15,18,21', minute=15, second=0, day_of_week='*'))

# Додавання задачі для росії
scheduler.add_job(download_and_process_telegrams, args=['rus'],
                  trigger=CronTrigger(hour='0,3,6,9,12,15,18,21', minute=20, second=0, day_of_week='*'))

# Додавання задачі для України
scheduler.add_job(download_and_process_telegrams, args=['ua'],
                  trigger=CronTrigger(hour='0,3,6,9,12,15,18,21', minute=25, second=0, day_of_week='*'))

# Запуск планувальника
scheduler.start()

class TelegramFilter(BaseModel):
    """
     Модель для фільтрації телеграм.

     - country_code: Код країни ('ua', 'bel', 'rus'), який відповідає назві колекції в MongoDB.
     - station_id: Ідентифікатор станції.
     - date: Дата в форматі YYYYMMDD.
     - date_start: Початкова дата для діапазону дат у форматі YYYYMMDD.
     - date_end: Кінцева дата для діапазону дат у форматі YYYYMMDD.
     - hour: Година дня (0-23).
     - temperature: Температура.
     - dew_point_temperature: Точка роси.
     - relative_humidity: Відносна вологість.
     - wind_dir: Напрямок вітру.
     - wind_speed: Швидкість вітру.
     - pressure: Тиск.
     - sea_level_pressure: Тиск на рівні моря.
     - maximum_temperature: Максимальна температура.
     - minimum_temperature: Мінімальна температура.
     - precipitation_s1: Опади (перший тип).
     - precipitation_s3: Опади (третій тип).
     - pressure_tendency: Тенденція тиску.
     - present_weather: Поточна погода.
     - past_weather_1: Минуле погодне явище 1.
     - past_weather_2: Минуле погодне явище 2.
     - sunshine: Сонячне сяйво.
     - ground_state_snow: Стан снігу на землі.
     - ground_state: Стан землі.
     - fields_to_return: Список полів, які потрібно повернути у відповіді.
     """
    country_code: Optional[str] = Field(None, description="Country Code ('ua', 'bel', 'rus')")
    station_id: Optional[str] = Field(None, description="Station ID")
    date: Optional[str] = Field(None, description="Date in YYYYMMDD format")
    date_start: Optional[str] = Field(None, description="Start Date in YYYYMMDD format")
    date_end: Optional[str] = Field(None, description="End Date in YYYYMMDD format")
    hour: Optional[int] = Field(None, description="Hour of the day (0-23)")
    temperature: Optional[float] = Field(None, description="Temperature")
    dew_point_temperature: Optional[float] = Field(None, description="Dew Point Temperature")
    relative_humidity: Optional[float] = Field(None, description="Relative Humidity")
    wind_dir: Optional[float] = Field(None, description="Wind Direction")
    wind_speed: Optional[float] = Field(None, description="Wind Speed")
    pressure: Optional[float] = Field(None, description="Pressure")
    sea_level_pressure: Optional[float] = Field(None, description="Sea Level Pressure")
    maximum_temperature: Optional[float] = Field(None, description="Maximum Temperature")
    minimum_temperature: Optional[float] = Field(None, description="Minimum Temperature")
    precipitation_s1: Optional[float] = Field(None, description="Precipitation S1")
    precipitation_s3: Optional[float] = Field(None, description="Precipitation S3")
    pressure_tendency: Optional[float] = Field(None, description="Pressure Tendency")
    present_weather: Optional[str] = Field(None, description="Present Weather")
    past_weather_1: Optional[str] = Field(None, description="Past Weather 1")
    past_weather_2: Optional[str] = Field(None, description="Past Weather 2")
    sunshine: Optional[float] = Field(None, description="Sunshine")
    ground_state_snow: Optional[str] = Field(None, description="Ground State Snow")
    ground_state: Optional[str] = Field(None, description="Ground State")
    fields_to_return: Optional[List[str]] = Field(None, description="Fields to include in the response")



app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/download_telegrams")
def download_telegrams(country_code: str):
    """
        Запускає процес завантаження та обробки телеграм для заданої країни.

        Параметри:
        - `country_code` (str): Код країни, для якої потрібно завантажити телеграми. Очікувані значення: `'bel'`, `'rus'`, `'ua'`.

        Повертає:
        - `message` (str): Повідомлення про успішний старт завантаження.
        - `error` (str): Повідомлення про помилку, якщо процес завантаження не вдалось запустити.

        Цей запит запускає завантаження метеорологічних телеграм з OGIMET та зберігає їх у базі даних MongoDB.
        Завантаження виконується для станцій країни, що визначається параметром `country_code`.


        Приклад запиту:
        ```bash
        curl -X 'POST' \
          'http://localhost:8000/download_telegrams' \
          -H 'accept: application/json' \
          -H 'Content-Type: application/json' \
          -d '{"country_code": "ua"}'
        ```

        Очікуваний результат:
        ```json
        {
          "message": "Successfully started downloading telegrams for ua"
        }
        ```

        Примітки:
        - Запит є синхронним, тобто повертає відповідь після успішного старту процесу завантаження, але не чекає на завершення завантаження.
        - Коди країн використовуються для визначення колекцій у базі даних MongoDB, в які будуть збережені завантажені телеграми.
        """
    try:
        download_and_process_telegrams(country_code)
        return {"message": f"Successfully started downloading telegrams for {country_code}"}
    except Exception as e:
        return {"error": str(e)}



@app.get("/telegram/{collection_name}/{id_teleg}")
def get_data_from_collection(collection_name: str, id_teleg: str):
    """
     Повертає розкодовану телеграму за вказаним ідентифікатором та колекцією.

     ### Параметри:
     - **collection_name** (str): Назва колекції, яка відповідає країні (наприклад, 'ua' для України, 'bel' для Білорусі, 'rus' для Росії).
     - **id_teleg** (str): Унікальний ідентифікатор телеграми, яку потрібно знайти. Ідентифікатор формується на основі наступних компонентів:
         - **station_id**: Ідентифікатор станції, з якої отримана телеграма.
         - **year**: Рік, коли була зібрана телеграма.
         - **month**: Місяць, коли була зібрана телеграма.
         - **day**: День, коли була зібрана телеграма.
        - **hour**: Година, коли була зібрана телеграма.
        Телеграми передаються кожні 3 години протягом доби за наступним графіком: '0, 3, 6, 9, 12, 15, 18, 21'.

     ### Повертає:
     - Повний JSON об'єкт телеграми, якщо її знайдено.
     - Повідомлення про те, що дані за вказаний період відсутні, якщо телеграма не знайдена.

     ### Опис:
     Ця функція дозволяє отримати розкодовану телеграму за її унікальним ідентифікатором та назвою колекції (країни).

     ### Приклад запиту:
     ```bash
     curl -X 'GET' \
       'http://localhost:8000/telegram/ua/3450420249218' \
       -H 'accept: application/json'
     ```

     ### Очікуваний результат:
     ```json
     {
       "id_telegram": "3450420249218",
       "data": {
         "station_id": "34504",
         "year": 2024,
         "month": 9,
         "day": 2,
         "hour": 18,
         "telegram": "AAXX 02181 34504 32975 51106 10251 20129 39989 40151 52027 80001 333 10330",
         "temperature": 25.1,
         "dew_point_temperature": 12.9,
         "relative_humidity": 47,
         "wind_dir": 110,
         "wind_speed": 6,
         "pressure": 998.9,
         "sea_level_pressure": 1015.1,
         "maximum_temperature": 33,
         "minimum_temperature": null,
         "precipitation_s1": {
           "amount": null,
           "time_before_obs": null
         },
         "precipitation_s3": {
           "amount": null,
           "time_before_obs": null
         },
         "pressure_tendency": {
           "tendency": 2,
           "change": 2.7
         },
         "present_weather": null,
         "past_weather_1": null,
         "past_weather_2": null,
         "sunshine": null,
         "ground_state_snow": {
           "state": null,
           "depth": null
         },
         "ground_state": {
           "state": null,
           "temperature_soil_surface": null
         }
       }
     }
     ```

     ### Примітки:
     - Якщо телеграма не знайдена у базі даних MongoDB, повернеться повідомлення: `"Дані за цей період відсутні"`.
     """
    collection = db[collection_name]
    data = collection.find_one({"id_telegram": id_teleg}, {"_id": False})
    if data is None:
        return {"message": "Дані за цей період відсутні"}
    else:
        cleaned_data = clean_data(data)
        return JSONResponse(content=cleaned_data)

@app.delete("/telegram/{collection_name}/{id_teleg}")
def delete_data_from_collection(collection_name: str, id_teleg: str):
    collection = db[collection_name]
    result = collection.delete_one({"id_telegram": id_teleg})
    if result.deleted_count == 1:
        return {"message": "Дані успішно видалено"}
    else:
        return {"message": "Дані за цей id не знайдені"}


@app.put("/telegram/{collection_name}/{id_teleg}")
def update_data_in_collection(collection_name: str, id_teleg: str, dynamic_updates: dict):
    collection = db[collection_name]
    update_fields = {
        f"data.{field}": value
        for field, value in dynamic_updates.items()
    }
    result = collection.update_one(
        {"id_telegram": id_teleg},
        {"$set": update_fields}
    )
    if result.modified_count == 1:
        return {"message": "Дані успішно оновлено"}
    else:
        return {"message": "Дані за цей id не знайдені"}


def parse_date(date_str: str) -> datetime:
    return datetime.strptime(date_str, '%Y%m%d')


@app.post("/filter_telegrams/")
def filter_telegrams(filter: TelegramFilter):
    """
      Фільтрує телеграми за заданими критеріями.

      Параметри:
      - `filter` (TelegramFilter): Об'єкт, який містить критерії фільтрації.

      Повертає:
      - `results` (List[Dict]): Список телеграм, що відповідають критеріям фільтрації.
      - `message` (str): Повідомлення, якщо даних не знайдено.

      Приклад запиту:
      ```bash
      curl -X 'POST' \
        'http://localhost:8000/filter_telegrams/' \
        -H 'accept: application/json' \
        -H 'Content-Type: application/json' \
        -d '{
          "country_code": "ua",
          "date": "20240901",
          "hour": 18,
          "fields_to_return": ["station_id", "temperature"]
        }'
      ```

      Очікуваний результат:
      ```json
      {
        "results": [
          {
            "data": {
              "station_id": "34504",
              "temperature": 25.1
            }
          },
          {
            "data": {
              "station_id": "34505",
              "temperature": 24.7
            }
          }
        ]
      }
      ```
      Примітки:
      - Якщо `country_code` не вказано, пошук виконується у всіх колекціях бази даних.
      - Якщо не вказано `fields_to_return`, повертається вся структура даних.
      - Усі значення `NaN` та інші несумісні з JSON значення замінюються на `null`.
      """
    collections_to_query = [filter.country_code] if filter.country_code else db.list_collection_names()
    results = []

    for collection_name in collections_to_query:
        collection = db[collection_name]
        query = {}

        if filter.station_id:
            query['data.station_id'] = filter.station_id

        if filter.date and len(filter.date) >= 8:
            try:
                year = int(filter.date[:4])
                month = int(filter.date[4:6])
                day = int(filter.date[6:8])
                query['data.year'] = year
                query['data.month'] = month
                query['data.day'] = day
            except ValueError:
                continue

        if filter.hour is not None:
            query['data.hour'] = filter.hour

        projection = {"_id": False}
        if filter.fields_to_return:
            for field in filter.fields_to_return:
                projection[f"data.{field}"] = True

        print(f"Final query for collection {collection_name}: {query}")
        print(f"Projection: {projection}")

        collection_results = list(collection.find(query, projection))
        print(f"Results found in {collection_name}: {len(collection_results)}")

        results.extend(collection_results)

    cleaned_results = clean_data(results)

    if not cleaned_results:
        return {"message": "No data found for the given filters"}

    return {"results": cleaned_results}



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, root_path="/", log_level="info")





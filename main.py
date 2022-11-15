import time
from extract import extract_data
from report import make_reports
from transform_and_load import transform_and_load_data

if __name__ == "__main__":
    try:
        while True:
            # ETL-процесс
            transform_and_load_data(extract_data())
            # Создание отчетов
            make_reports()
            time.sleep(86400)

    # Принудительное завершение работы
    except KeyboardInterrupt:
        print('Процесс остановлен пользователем')
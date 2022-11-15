import datetime
import ftplib
import os
import pandas as pd
import psycopg2
import sys
import xml.etree.ElementTree as ET
from psycopg2 import Error
from config import get_config
from logger import logger

# Установка пути проекта
norm_path = os.getcwd()
# Получение параметров для БД/FTP-сервера
CONFIG = get_config()

# Названия для таблиц данных БД и FTP
DB_TABLE_NAMES = ['main.car_pool', 'main.rides', 'main.movement', 'main.drivers']
FTP_TABLE_NAMES = ['waybills', 'payments']


@logger
def connect_db(configs: dict, dwh=False) -> tuple:
    """Открытие соединения с postgreSQL"""
    # Подключение к базе данных с помощью параметров
    try:
        if not dwh:
            connection = psycopg2.connect(user=configs['POSTGRES_USER'],
                                          password=configs['POSTGRES_PASSWORD'],
                                          host=configs['POSTGRES_HOST'],
                                          port=configs['POSTGRES_PORT'],
                                          database=configs['POSTGRES_DB'],
                                          sslmode=configs['POSTGRES_MODE'])
        else:
            connection = psycopg2.connect(user=configs['DWH_POSTGRES_USER'],
                                          password=configs['DWH_POSTGRES_PASSWORD'],
                                          host=configs['POSTGRES_HOST'],
                                          port=configs['POSTGRES_PORT'],
                                          database=configs['DWH_POSTGRES_DB'],
                                          sslmode=configs['POSTGRES_MODE']
                                          )
        return (connection.cursor(), connection)

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)


@logger
def get_table(table_name, cursor) -> dict:
    """Извлечение таблиц"""

    # Переход в папку/создание папки log
    try:
        os.chdir('log')

    except FileNotFoundError:
        os.mkdir('log')
        os.chdir('log')

    # Для таблиц car_pool и drivers инкрементальная загрузка осуществляется путем
    # считывания времени последнего запуска программы и сравнивания с ним времени изменения строки в БД
    if 'car_pool' in table_name or 'drivers' in table_name:
        try:

            with open('log.txt', mode='r') as log_data:
                dt_last = log_data.readline()

        except FileNotFoundError:
            # При первом запуске кода время устанавливается по умолчанию
            with open('log.txt', mode='w+') as log_data:
                dt_last = '1970-01-01 00:00:00.000000'
                log_data.write(dt_last)

        postgreSQL_select_Query = (f"select * from {table_name} where update_dt > '{dt_last}'")
        os.chdir('..')

    # Для таблицы movement инкрементальная загрузка осуществляется путем
    # считывания id последней обработанной строчки и сравнивания с ним id каждой строчки из данной таблицы
    elif 'movement' in table_name:
        try:

            with open('log_movement.txt', mode='r') as log_data:
                id_last = log_data.readline()

        except FileNotFoundError:

            # При первом запуске кода id устанавливается по умолчанию
            with open('log_movement.txt', mode='w+') as log_data:
                id_last = '0'
                log_data.write(id_last)

        try:
            # На этапе transform and load некоторые поездки могут быть не завершены к моменту запуска программы,
            # информация о таких поездках хранится в файле log_movement_unfinished.txt

            with open('log_movement_unfinished.txt', 'r') as original:
                data_id = original.read()
                data_id = tuple([int(i) for i in data_id.split('\n') if i != ''])

            postgreSQL_select_Query = (
                f"select * from {table_name} where movement_id > '{id_last}' or ride in {data_id}")

        except:
            postgreSQL_select_Query = (
                f"select * from {table_name} where movement_id > '{id_last}'")

        os.chdir('..')

    else:
        # Для таблицы rides логика та же, что и для таблицы movement
        try:
            with open('log_rides.txt', mode='r') as log_data:
                id_last = log_data.readline()

        except FileNotFoundError:

            with open('log_rides.txt', mode='w+') as log_data:
                id_last = '0'
                log_data.write(id_last)

        try:
            with open('log_movement_unfinished.txt', 'r') as original:
                data_id = original.read()
                data_id = tuple([int(i) for i in data_id.split('\n') if i != ''])

            postgreSQL_select_Query = (
                f"select * from {table_name} where ride_id > '{id_last}' or ride_id in {data_id}")

        except:
            postgreSQL_select_Query = (f"select * from {table_name} where ride_id > '{id_last}'")

        os.chdir('..')

    # Извлечение данных по SQL запросу, написанному выше
    cursor.execute(postgreSQL_select_Query)
    column_names = [desc[0] for desc in cursor.description]
    table_records = cursor.fetchall()
    table_list = []
    table_records = [list(row) for row in table_records]

    # Упаковка полученных данных в словарь
    for row in table_records:
        for i in range(len(row)):
            row[i] = str(row[i])

        row = dict(zip(column_names, row))
        table_list.append(row)

    os.chdir('log')

    # Записи последних id для таблиц movement и rides
    if 'movement' in table_name:
        if table_list != []:
            with open('log_movement.txt', mode='w+') as log_data:
                log_data.write(table_list[-1]['movement_id'])

    elif 'rides' in table_name:
        if table_list != []:
            with open('log_rides.txt', mode='w+') as log_data:
                log_data.write(table_list[-1]['ride_id'])

    os.chdir('..')
    return table_list


@logger
def disconnect_db(connection, cursor) -> None:
    """Закрытие соединения с PostgreSQL"""
    cursor.close()
    connection.close()


def connect_ftp():
    """Открытие соединения с FTP"""
    global norm_path

    ftps = ftplib.FTP_TLS()

    # Установка кодировки
    ftps.encoding = 'utf-8'

    # Используются зашифрованные параметры
    try:
        ftps.connect(CONFIG['FTP_HOST'], port=int(CONFIG['FTP_PORT']), timeout=2)
    except:
        return
    ftps.login(CONFIG['FTP_LOGIN'], CONFIG['FTP_PASSWORD'])
    ftps.prot_p()

    return ftps


@logger
def get_ftps(ftp_data):
    """Получение данных с серверов FTP"""
    global FTP_TABLE_NAMES
    ftps = connect_ftp()

    # Создания словаря для выгрузки данных
    ftp_data = {name: [] for name in FTP_TABLE_NAMES}

    for key in ftp_data.keys():

        # Получение списка файлов в папке на FTP сервере
        ftps.cwd(f'/{key}')
        f = ftps.nlst()
        flag = 0

        os.chdir(norm_path)

        try:
            os.chdir('log')
        except FileNotFoundError:
            os.mkdir('log')
            flag = 1

        # Переключения вывода данных в консоль на вывод данных в файл и запись метаданных о FTP-файлах
        stdoutOrigin = sys.stdout
        sys.stdout = open(f"log_files_{key}.txt", "w")
        ftps.dir()
        sys.stdout.close()
        sys.stdout = stdoutOrigin

        if flag:
            with open('log.txt', mode='w+') as log_data:
                log_data.write('1970-01-01 00:00:00.000000')

        # Получение времени последнего запуска программы
        with open('log.txt', 'r') as log_data:
            last_time = datetime.datetime.strptime(log_data.readline(), '%Y-%m-%d %H:%M:%S.%f')

        year = datetime.date.today().year
        os.chdir("../log")
        new_files = []

        with open(f"log_files_{key}.txt", "r") as file_s:
            # Инкрементальный отбор файлов с FTP-сервера (сравнивается дата
            # загрузки файла на сервер и датой последнего запуска программы)
            for line in file_s:
                line_time = ' '.join(line.split(' ')[-4:-1])
                line_name = line.split(' ')[-1]
                line_time = datetime.datetime.strptime(str(year) + ' ' + line_time, '%Y %b %d %H:%M')
                if last_time < line_time:
                    new_files.append(line_name[:-1])

        os.chdir('..')

        for i in range(len(f)):
            filename = f[i]
            if filename in new_files:
                try:
                    # Для каждого файла новое подключение (чтобы не потерять файлы из-за превышения времени запроса)
                    ftps = connect_ftp()
                    ftps.cwd(filename.split("_")[0] + 's')
                    print('Открытие файла: ' + filename)
                    get_ftp_data = []

                    def handle_binary(more_data):
                        get_ftp_data.append(more_data)

                    # Скачивание файла
                    resp = ftps.retrlines('RETR %s' % filename, callback=handle_binary)
                    get_ftp_data = "; ".join(get_ftp_data)

                    # Данные вносятся в словарь, где ключ - название папки на сервере
                    ftp_data[filename.split('_')[0] + 's'].append(get_ftp_data)
                except:
                    # При ошибке скачивания повторно возвращаемся к этому же файлу
                    i -= 1

    # Отключение соединения
    disconnect_ftp(ftps)
    return ftp_data


@logger
def disconnect_ftp(ftps) -> None:
    """Закрытие соединения с FTP"""
    ftps.quit()


def extract_data():
    """Загрузка данных"""
    print(f'Обновление данных {datetime.datetime.now()}')

    # Создаем словарь, ключи - имена таблиц, значения - пустые списки (в них будет записывать информация из БД)
    tables_dict = {name: [] for name in DB_TABLE_NAMES}

    # Подключаемся к БД
    try:
        cursor, connection = connect_db(CONFIG)

        # Получаем данные из таблиц
        for key in tables_dict.keys():
            tables_dict[key] = get_table(key, cursor)

    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if connection:

            # Отключаем соединение
            disconnect_db(connection, cursor)

    # Создаем словарь для FTP, ключи - имена таблиц, значения - пустые списки (в них будет записывать информация с FTP)
    ftp_data_processed = {name: [] for name in FTP_TABLE_NAMES}

    # Получаем информацию с FTP сервера
    ftp_data = get_ftps(ftp_data_processed)

    os.chdir("log")

    # Записываем время подключения к серверу
    with open('log.txt', 'w+') as myfile:
        myfile.write(str(datetime.datetime.now()))

    # Создаем ДатаФрейм для payments
    df_payments = pd.DataFrame()

    for key, value in ftp_data.items():
        #  Вносим в ДатаФрейм для payments данные, скачанные с FTP
        if key == 'payments' and value != []:
            for file in value:
                for string_dt in file.split(';'):
                    df_payments = pd.concat([df_payments, pd.DataFrame(pd.Series(string_dt.split('\t'))).T])

            df_payments.columns = ['date', 'card', 'payment amount']

            for index, row in df_payments.iterrows():
                ftp_data_processed["payments"].append({"date": row[0], "card": row[1], "payment amount": row[2]})

        # "Парсим" данные XML-файлов с FTP
        elif key == 'waybills' and value != []:
            for file in value:
                tree = ET.fromstring(file.replace(';', ''))

                # Данные вносятся в словарь, где ключи - название колонки в будущем ДатаФрейме
                info_dict = {}

                for info in tree.iter():
                    line = list((info.tag, info.keys(), info.items(), info.text))
                    info_dict[line[0]] = line[1:]

                del info_dict['driver']
                del info_dict['period']
                del info_dict['waybills']

                for key, value in info_dict.items():
                    if key != 'waybill':
                        info_dict[key] = value[2]
                    else:
                        temp = value[1]
                        temp_dict = {}
                        for par in temp:
                            temp_dict[par[0]] = par[1]

                info_dict = dict(info_dict, **temp_dict)
                del info_dict['waybill']
                ftp_data_processed["waybills"].append(info_dict)

    print('Данные успешно импортированы и сохранены')
    print('____________________________________________________________________________________________________\n')

    # Удаляем содержимое файла с незавершенными поездками
    global norm_path
    os.chdir(norm_path)
    try:
        os.remove("log/log_movement_unfinished.txt")
    except FileNotFoundError:
        pass

    # Объединяем словарь с данными, извлеченными из БД, со словарем данных FTP-сервера
    return dict(ftp_data_processed, **tables_dict)
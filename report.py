import datetime
import psycopg2
from psycopg2 import extras
from config import get_config
from extract import connect_db
from extract import disconnect_db
import pandas as pd
from logger import logger

CONFIG = get_config()

def get_table(cursor, query:str) -> pd.DataFrame:
    """Получение таблицы по запросу"""
    cursor.execute(query)
    column_names = [desc[0] for desc in cursor.description]
    table_records = cursor.fetchall()
    table_list = []
    table_records = [list(row) for row in table_records]
    for row in table_records:
        for i in range(len(row)):
            row[i] = str(row[i])
        row = dict(zip(column_names, row))
        table_list.append(row)
    return pd.DataFrame(table_list)


def load_values(conn, df, table) -> None:
    tuples = [tuple(x) for x in df.to_numpy()]

    cols = ','.join(list(df.columns))

    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = conn.cursor()

    try:
        extras.execute_values(cursor, query, tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    cursor.close()

@logger
def make_report_1(cursor, connection) -> None:
    """Построение первого отчёта"""

    # Получение таблицы с персональными данными водителя
    postgreSQL_select_Query = "SELECT personnel_num, last_name, first_name, middle_name, card_num FROM dim_drivers"
    dim_drivers = get_table(cursor, postgreSQL_select_Query)

    # Получение таблицы с зарплатой водителя за день
    postgreSQL_select_Query = "SELECT driver_pers_num AS personnel_num, " \
                              "(price_amt - (price_amt * 0.2) - (47.26 * 7 * distance_val / 100) - " \
                              "(5 * distance_val)) AS amount, DATE(ride_end_dt) AS report_dt " \
                              "FROM fact_rides"

    drivers_amount = get_table(cursor, postgreSQL_select_Query)
    drivers_amount['amount'] = drivers_amount['amount'].astype('float')
    drivers_amount['report_dt'] = pd.to_datetime(drivers_amount['report_dt'])
    drivers_amount = drivers_amount.groupby(['personnel_num', 'report_dt'], as_index=False).sum()
    res_df = pd.merge(dim_drivers, drivers_amount, on='personnel_num', how='inner')
    now = str(datetime.datetime.now().date())

    # Подключаемся к БД, для взятия последней даты построения отчёта
    postgreSQL_select_Query = "SELECT MAX(report_dt) FROM rep_drivers_payments"

    # Если таблица пустая, то загружаем все строки. Иначе последняя дата < x < сегодня
    last_report_date = get_table(cursor, postgreSQL_select_Query).values[0][0]
    last_report_date = last_report_date if last_report_date != 'None' else '1970-01-01'

    # Фильтруем df и загружаем в отчёт в БД
    res_df = res_df[(res_df['report_dt'] > last_report_date) & (res_df['report_dt'] < now)]
    load_values(connection, res_df, 'dwh_voronezh.rep_drivers_payments')

@logger
def make_report_2(cursor, connection) -> None:
    """Построение второго отчёта"""

    postgreSQL_select_Query = "SELECT * FROM " \
                              "(SELECT ride_end_dt, driver_pers_num AS personnel_num, ride_id AS ride, " \
                              "distance_val /" \
                              " (DATE_PART('hour', ride_end_dt - ride_start_dt) + " \
                              "DATE_PART('minute', ride_end_dt - ride_start_dt) / 60 + " \
                              "DATE_PART('second', ride_end_dt - ride_start_dt) / 3600)" \
                              " AS speed FROM fact_rides) AS Subquery " \
                              "WHERE speed > 85"

    current_drive = get_table(cursor, postgreSQL_select_Query)
    current_drive['violations_cnt'] = 1

    # Получаем количество нарушений, сделанных водителем ранее
    postgreSQL_select_Query = "SELECT personnel_num, MAX(violations_cnt) AS violations_cnt" \
                              " FROM rep_drivers_violations " \
                              "GROUP BY personnel_num"
    previous_drives = get_table(cursor, postgreSQL_select_Query)

    # Получаем список поездок, которые уже есть в БД, чтобы их не учитывать в отчёте
    postgreSQL_select_Query = "SELECT ride FROM rep_drivers_violations"
    report_drives = get_table(cursor, postgreSQL_select_Query)
    report_drives = list(report_drives['ride'].values)
    current_drive = current_drive.loc[~current_drive['ride'].isin(report_drives)]

    # Присоединяем к текущему отчёту информацию о количестве предыдущих нарушений

    # Обрабатываем исключение если таблица пустая
    try:
        res_df = pd.merge(current_drive, previous_drives, on='personnel_num', how='left')
    except KeyError:
        res_df = current_drive
        res_df.rename(columns={'violations_cnt': 'violations_cnt_x'}, inplace=True)
        res_df['violations_cnt_y'] = -1

    # Нужно учесть, что в день водитель мог совершить несколько нарушений,
    # значит нужно считать накопленную сумму
    res_df['violations_cnt_y'] = res_df['violations_cnt_y'].fillna(-1).astype('int')
    res_df = res_df.sort_values('ride_end_dt')

    # Считаем накопленную сумму, сгруппированную по каждому водителю
    res_df["violations_cnt_x"] = res_df[['personnel_num', "violations_cnt_x"]].groupby('personnel_num').cumsum()
    res_df.sort_index(inplace=True)
    '''Объяснение: столбец violations_count_x считает накопленную сумму внутри дня, 
    а violations_count_y хранит предыдущие нарушения -> при их сложении получится актуальное кол-во нарушений'''
    res_df['violations_cnt'] = res_df['violations_cnt_x'] + res_df['violations_cnt_y']

    # Оставляем только нужные для исходного отчёта столбцы
    res_df = res_df[['personnel_num', 'ride', 'speed', 'violations_cnt']]

    # Загрузка отчёта
    load_values(connection, res_df, 'dwh_voronezh.rep_drivers_violations')

def make_reports():
    # Подключение к нашей базе данных
    cursor, connection = connect_db(configs=CONFIG, dwh=True)

    # Создание первого отчёта
    make_report_1(cursor, connection)

    # Создание второго отчёта
    make_report_2(cursor, connection)

    # Отключаемся от конечной базы данных
    disconnect_db(connection, cursor)
    print('____________________________________________________________________________________________________\n')
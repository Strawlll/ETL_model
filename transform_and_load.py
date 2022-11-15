import os
import pandas as pd
from tqdm import tqdm
from config import get_config
from extract import connect_db
from extract import disconnect_db
from psycopg2 import Error
from datetime import datetime

CONFIG = get_config()


def update_dim_clients(cursor, table):
    """Добавление/обновление данных о клиентах"""
    if not table.empty:
        for row in tqdm(table.iterrows()):
            # Получаем дату последней поездки клиента
            cursor.execute(
                f"select max(ride_end_dt) from dwh_voronezh.fact_rides where client_phone_num = '{row[1].loc['client_phone']}'")
            end = cursor.fetchall()[0][0]
            end = end.date() if end else None

            # Вносим изменения в таблицу при помощи SQL-запроса
            cursor.execute(f'''INSERT INTO dwh_voronezh.dim_clients(phone_num, start_dt, card_num, deleted_flag, end_dt)
                               VALUES('{row[1].loc['client_phone']}', '{row[1].loc['dt']}', '{row[1].loc['card_num'].replace(" ", "")}', '{'N'}', %s)
                               ON CONFLICT (phone_num) DO UPDATE
                               SET card_num={row[1].loc['card_num'].replace(" ", "")}, end_dt = %s''', (end, end))

        print("dim_clients updated")


def update_dim_cars(cursor, car_pool) -> None:
    """Добавление/обновление данных о машинах"""
    if not car_pool.empty:
        for car in tqdm(car_pool.iterrows()):

            plate_num, car_model, revision_dt, register_dt, finished_flg, update_dt = car[1:][0]

            # Проверяем, списана ли машина
            if finished_flg == "Y":
                end_dt = revision_dt
            else:
                # Получаем дату последней поездки на данной машине
                cursor.execute(
                    f"select max(work_end_dt) from dwh_voronezh.fact_waybills where car_plate_num = '{plate_num}'")
                end_dt = cursor.fetchall()[0][0]
                end_dt = end_dt.date() if end_dt else None

            # Вносим изменения в таблицу при помощи SQL-запроса
            cursor.execute('''INSERT INTO dwh_voronezh.dim_cars(
                              plate_num, start_dt, model_name, revision_dt, deleted_flag, end_dt)
                              VALUES(%s, %s, %s, %s, %s, %s)
                              ON CONFLICT (plate_num) DO 
                              UPDATE SET start_dt = %s, revision_dt = %s, deleted_flag = %s, end_dt = %s''',
                              (plate_num, register_dt, car_model, revision_dt, finished_flg, end_dt,
                              register_dt, revision_dt, finished_flg, end_dt))

        print("dim_cars updated")


def update_dim_drivers(cursor, drivers):
    """Добавление/обновление данных о водителях"""
    if not drivers.empty:
        for driver in tqdm(drivers.iterrows()):

            # Дата по умолчанию, при совершении водителем первой поездки она будет обновлена
            start_date = "1970-01-01"
            deleted_flag = 'N'
            driver_license_num, first_name, last_name, middle_name, driver_license_dt, \
            card_num, update_dt, birth_dt = tuple(driver[1])
            personnel_num = driver_license_num[-6:]

            # Получаем дату последней поездки водителя
            cursor.execute(
                f"select max(work_end_dt) from dwh_voronezh.fact_waybills where driver_pers_num = '{personnel_num}'")
            end_dt = cursor.fetchall()
            end_dt = end_dt[0][0]
            end_dt = end_dt.date() if end_dt else None

            # Вносим изменения в таблицу при помощи SQL-запроса
            cursor.execute('''INSERT INTO dwh_voronezh.dim_drivers(
                              personnel_num, start_dt, last_name, first_name, middle_name, birth_dt, card_num,
                              driver_license_num, driver_license_dt, deleted_flag, end_dt)
                              VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                              ON CONFLICT (personnel_num) DO UPDATE 
                              SET last_name = %s, first_name = %s, middle_name = %s, card_num = %s,
                              driver_license_num = %s, driver_license_dt = %s, deleted_flag = %s, end_dt = %s''',
                              (personnel_num, start_date, last_name, first_name, middle_name, birth_dt, card_num,
                              driver_license_num, driver_license_dt, deleted_flag, end_dt,
                              last_name, first_name, middle_name, card_num, driver_license_num,
                              driver_license_dt, deleted_flag, end_dt))

        print("dim_drivers updated")


def update_dim_drivers_start_dt(cursor, personnel_num, date) -> None:
    """Занесение в таблицу dim_drivers значений start_dt при совершении водителем его первой поездки"""
    cursor.execute('''DO $$ BEGIN
                      CASE
                          WHEN (SELECT start_dt FROM dwh_voronezh.dim_drivers WHERE personnel_num = %s) = '1970-01-01'
                          THEN 
                              UPDATE dwh_voronezh.dim_drivers SET start_dt = %s;
                          ELSE
                              null;
                      END CASE;
                      END $$;''', (personnel_num, date))


def update_fact_waybills(cursor, waybill_n, personnel_num, car_plate, start, stop, date) -> None:
    """Добавление новых путевых листов"""
    cursor.execute('''INSERT INTO dwh_voronezh.fact_waybills(
                      waybill_num, driver_pers_num, car_plate_num, work_start_dt, work_end_dt, issue_dt)
                      VALUES(%s, %s, %s, %s, %s, %s)
                      ON CONFLICT (waybill_num) DO NOTHING''',
                      (waybill_n, personnel_num, car_plate, start, stop, date))


def update_fact_rides(cursor, rides, movement) -> None:
    """Добавление новых завершенных поездок"""

    def set_client_dt():
        """Обновление end_dt в dim_clients"""
        cursor.execute(f"""UPDATE dwh_voronezh.dim_clients SET end_dt = %s WHERE phone_num = %s""",
                       (date.date(), client_phone))

    def set_driver_dt():
        """Обновление end_dt в dim_drivers"""
        cursor.execute(f"""UPDATE dwh_voronezh.dim_drivers SET end_dt = %s WHERE personnel_num = %s""",
                       (date.date(), driver_pers_num))

    def set_car_dt():
        """Обновление end_dt в dim_cars"""
        cursor.execute(f"""UPDATE dwh_voronezh.dim_cars SET end_dt = %s WHERE plate_num = %s""",
                       (date.date(), car_plate))

    for record in tqdm(rides.iterrows()):
        ride_id = record[1].loc['ride_id']
        date = datetime.strptime(record[1].loc['dt'], '%Y-%m-%d %H:%M:%S')
        client_phone = record[1].loc['client_phone']
        point_from = record[1].loc['point_from']
        point_to = record[1].loc['point_to']
        distance = record[1].loc['distance']
        price = record[1].loc['price']
        ride = movement[(movement.ride == ride_id)]

        try:
            # Проверяем, что поездка завершена, иначе вызывается ride_id записывается в log_movement_unfinished.txt
            selected = ride[(ride.event == 'END') | (ride.event == 'CANCEL')].iloc[0]
            car_plate = selected['car_plate_num']

            # Получаем номер водителя, работающего на данной машине в данное время
            cursor.execute('''SELECT driver_pers_num
                              FROM dwh_voronezh.fact_waybills
                              WHERE work_end_dt >= %s AND work_start_dt <= %s AND car_plate_num = %s''',
                              (date, date, car_plate))
            driver_pers_num = cursor.fetchall()[0][0]

            # Обновляем дату последне совершенной поездки в dim_ таблицах
            set_client_dt()
            set_driver_dt()
            set_car_dt()

            if len(selected):
                # Получаем время прибытия, начала и конца поездки
                ride_end_dt = selected['dt']
                ride_start_dt = None if selected['event'] == 'CANCEL' else \
                    ride[(ride.ride == ride_id) & (ride.event == 'BEGIN')].dt.values[0]
                ride_arrival_dt = ride[(ride.ride == ride_id) & (ride.event == 'READY')].dt.values[0]

                # Вносим изменения в таблицу при помощи SQL-запроса
                try:
                    cursor.execute(f'''INSERT INTO dwh_voronezh.fact_rides(
                                       ride_id, point_from_txt, point_to_txt, distance_val, price_amt, client_phone_num, 
                                       driver_pers_num, car_plate_num, ride_start_dt, ride_end_dt, ride_arrival_dt) 
                                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) 
                                       ON CONFLICT (ride_id) DO NOTHING''',
                                       (ride_id, point_from, point_to, distance, price, client_phone,
                                       driver_pers_num, car_plate, ride_start_dt, ride_end_dt, ride_arrival_dt))
                    connection.commit()

                except (Exception, Error) as error:
                    print("Ошибка sql-запроса: ", error)
                    connection.rollback()

        except (Exception, Error) as err:
            os.chdir('log')
            with open('log_movement_unfinished.txt', 'a') as modified:
                modified.write(str(ride_id) + '\n')
            os.chdir('..')

    print("fact_rides updated")


def update_fact_payments(cursor, payments) -> None:
    """Добавление новых транзакций"""
    for row in tqdm(payments.iterrows()):
        card = row[1].loc["card"]
        date = row[1].loc["date"].replace('.', '-', 2).strip()
        date = date[-13:-9] + date[2:-13] + date[:2] + date[-9:]
        amount = row[1].loc["payment amount"]

        # Вносим изменения в таблицу при помощи SQL-запроса
        cursor.execute('''INSERT INTO dwh_voronezh.fact_payments (
                            card_num, transaction_amt, transaction_dt)
                            VALUES(%s, %s, %s) ON CONFLICT DO NOTHING ''',
                       (card, amount, date))

    print('fact_payments updated')


def transform_and_load_data(data):
    """Внесение полученных на этапе execute данных в конечную БД"""
    car_pool = pd.DataFrame(data["main.car_pool"])
    rides = pd.DataFrame(data["main.rides"])
    movement = pd.DataFrame(data["main.movement"])
    drivers = pd.DataFrame(data["main.drivers"])
    payments = pd.DataFrame(data["payments"])
    waybills = pd.DataFrame(data["waybills"])

    # Подключаемся к конечной базе данных
    global connection
    cursor, connection = connect_db(configs=CONFIG, dwh=True)

    # Вносим изменения в dim_drivers
    update_dim_drivers(cursor, drivers)
    connection.commit()

    # Вносим изменения в dim_clients
    update_dim_clients(cursor, rides)
    connection.commit()

    # Вносим изменения в dim_cars
    update_dim_cars(cursor, car_pool)
    connection.commit()

    # Берем строки из ДатаФреймов, изначально полученных с ftp сервера
    if not waybills.empty:
        for row in tqdm(waybills.iterrows()):
            cursor = connection.cursor()
            date = row[1].loc["issuedt"]
            waybill_n = row[1].loc["number"]
            car_plate = row[1].loc["car"]
            driver_license = row[1].loc["license"]
            personnel_num = driver_license[-6:]
            start = row[1].loc["start"]
            stop = row[1].loc["stop"]

            # Задаем start_dt в dim_drivers
            update_dim_drivers_start_dt(cursor, personnel_num, date)
            connection.commit()

            # Вносим изменения в fact_waybills
            update_fact_waybills(cursor, waybill_n, personnel_num, car_plate, start, stop, date)
            connection.commit()

        print("dim_drivers, fact_waybills updated")

    # Вносим изменения в fact_payments
    update_fact_payments(cursor, payments)
    connection.commit()

    # Вносим изменения в fact_rides
    update_fact_rides(cursor, rides, movement)
    connection.commit()

    # Отключаемся от конечной базы данных
    disconnect_db(connection, cursor)
    print('____________________________________________________________________________________________________\n')
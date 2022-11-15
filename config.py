import os
from dotenv import dotenv_values

# Получение параметров для прдключения к БД/FTP-серверу
def get_config():
    config = dotenv_values('.env')
    if not config:
        config = os.environ
    return config
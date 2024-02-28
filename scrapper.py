import json
import os

import requests
import time
import pandas as pd
import sqlite3

def getPage(page, name):
    """
    Создаем метод для получения страницы со списком вакансий.
    Аргументы:
        page - Индекс страницы, начинается с 0. Значение по умолчанию 0, т.е. первая страница
    """

    # Справочник для параметров GET-запроса
    params = {
        'text': 'NAME:'+name,  # Текст фильтра. В имени должно быть слово "Инженер"
        'area': 16,  # Поиск ощуществляется по вакансиям Беларуси
        'page': page,  # Индекс страницы поиска на HH
        'per_page': 100  # Кол-во вакансий на 1 странице
    }

    req = requests.get('https://api.hh.ru/vacancies', params)  # Посылаем запрос к API
    data = req.content.decode()  # Декодируем его ответ, чтобы Кириллица отображалась корректно
    req.close()
    return data

def scrappFromServer(name):
    for page in range(0, 5):

        # Преобразуем текст ответа запроса в справочник Python
        jsObj = json.loads(getPage(page,name))

        # Сохраняем файлы в папку {путь до текущего документа со скриптом}\docs\pagination
        # Определяем количество файлов в папке для сохранения документа с ответом запроса
        # Полученное значение используем для формирования имени документа
        nextFileName = './docs/pagination/{}.json'.format(len(os.listdir('./docs/pagination')))

        # Создаем новый документ, записываем в него ответ запроса, после закрываем
        f = open(nextFileName, mode='w', encoding='utf8')
        f.write(json.dumps(jsObj, ensure_ascii=False))
        f.close()

        # Проверка на последнюю страницу, если вакансий меньше 2000
        if (jsObj['pages'] - page) <= 1:
            break

        # Необязательная задержка, но чтобы не нагружать сервисы hh, оставим. 5 секунд мы может подождать
        time.sleep(0.1)
    for fl in os.listdir('./docs/pagination'):

        # Открываем файл, читаем его содержимое, закрываем файл
        f = open('./docs/pagination/{}'.format(fl), encoding='utf8')
        jsonText = f.read()
        f.close()

        # Преобразуем полученный текст в объект справочника
        jsonObj = json.loads(jsonText)

        # Получаем и проходимся по непосредственно списку вакансий
        for v in jsonObj['items']:
            # Обращаемся к API и получаем детальную информацию по конкретной вакансии
            req = requests.get(v['url'])
            data = req.content.decode()
            req.close()

            # Создаем файл в формате json с идентификатором вакансии в качестве названия
            # Записываем в него ответ запроса и закрываем файл
            fileName = './docs/vacancies/{}.json'.format(v['id'])
            f = open(fileName, mode='w', encoding='utf8')
            f.write(data)
            f.close()

            time.sleep(1)


def readFromCSV():
    IDs = []  # Список идентификаторов вакансий
    names = []  # Список наименований вакансий
    descriptions = []  # Список описаний вакансий
    allow_messages_flags = []
    cities = []
    employments = []

    # Создаем списки для столбцов таблицы skills
    skills_vac = []  # Список идентификаторов вакансий
    skills_name = []  # Список названий навыков
    cnt_docs = len(os.listdir('./docs/vacancies'))
    i = 0

    # Проходимся по всем файлам в папке vacancies
    for fl in os.listdir('./docs/vacancies'):

        # Открываем, читаем и закрываем файл
        f = open('./docs/vacancies/{}'.format(fl), encoding='utf8')
        jsonText = f.read()
        f.close()

        # Текст файла переводим в справочник
        jsonObj = json.loads(jsonText)

        # Заполняем списки для таблиц
        IDs.append(jsonObj['id'])
        names.append(jsonObj['name'])
        descriptions.append(jsonObj['description'])
        allow_messages_flags.append(jsonObj['allow_messages'])

        if (jsonObj['area']['name'] is None):
            cities.append('Не указано')
        else:
            cities.append(jsonObj['area']['name'])

        # cities.append(jsonObj['address']['city'])
        if (jsonObj['employment']['name'] is None):
            employments.append('Не указано')
        else:
            employments.append(jsonObj['employment']['name'])
        # Т.к. навыки хранятся в виде массива, то проходимся по нему циклом
        for skl in jsonObj['key_skills']:
            skills_vac.append(jsonObj['id'])
            skills_name.append(skl['name'])

        # Увеличиваем счетчик обработанных файлов на 1, очищаем вывод ячейки и выводим прогресс
        i += 1

    # Создаем датафрейм, который затем сохраняем в БД в таблицу vacancies
    df = pd.DataFrame({'id': IDs, 'name': names, 'description': descriptions, 'city': cities, 'employment': employments,
                       'allow': allow_messages_flags})
    dff = pd.DataFrame({'vacancy': skills_vac, 'skill': skills_name})
    return [df,dff]



def getDataset(name,update=False):


    conn = sqlite3.connect('vacancy_db.sqlite')
    c = conn.cursor()

    # Поучение количества таблиц с именами
    c.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{name}' ''')

    # Если 1, то таблица существует
    isExist = False
    if c.fetchone()[0] == 1:
        isExist=True

    if (update or (isExist==False)):
        delete_files_in_directory('./docs/vacancies')
        delete_files_in_directory('./docs/pagination')

        if (update == True):
            c.execute('drop table '+name)
        scrappFromServer(name)
        df = readFromCSV()
        df[0].to_sql(name, conn, if_exists='replace', index=False)

    df = pd.read_sql_query('SELECT * FROM '+name, conn)

    conn.close()

    return df.to_json(orient='records')

def getDatasetSkill(name,update=False):


    conn = sqlite3.connect('vacancy_skill_db.sqlite')
    c = conn.cursor()

    # Получаем количество таблиц с именами
    c.execute(f''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='{name}' ''')

    # Если 1, то таблица существует
    isExist = False
    if c.fetchone()[0] == 1:
        isExist=True

    if (update or (isExist==False)):
        delete_files_in_directory('./docs/vacancies')
        delete_files_in_directory('./docs/pagination')

        if (update == True):
            c.execute('drop table '+name)
        scrappFromServer(name)
        df = readFromCSV()
        df[1].to_sql(name, conn, if_exists='replace', index=False)

    df = pd.read_sql_query('SELECT * FROM '+name, conn)

    conn.close()

    return df.to_json(orient='records')

def getCity(group_id,update=False):
    host = 'http://127.0.0.1:5000'
    action = 'none'
    if update == True:
        action = 'update'
    response = requests.get(
        '{host}/dataset/{group_id}?={action}'
        .format(host=host, group_id=group_id, action=action))

    if not response.ok:
        raise ConnectionError('Bad response code')

    df = pd.read_json(response.text, orient='records')

    ds = df.groupby('city')['id'].count().sort_values(ascending=False).head(5)
    return ds.to_json()

def getClosedMessage(group_id,update=False):
    host = 'http://127.0.0.1:5000'
    action = 'none'
    if update == True:
        action = 'update'
    response = requests.get(
        '{host}/dataset/{group_id}?={action}'
        .format(host=host, group_id=group_id, action=action))

    if not response.ok:
        raise ConnectionError('Bad response code')

    df = pd.read_json(response.text, orient='records')
    keys = ['labels', 'values']
    labels = ['Закрыты сообщения', 'Открыты сообщения']
    sizes = [df[df['allow'] == True]['id'].count(), df[df['allow'] == False]['id'].count()]
    result = dict(zip(keys, [labels, [str(x) for x in sizes]]))
    return json.dumps(result)

def getEmployment(group_id,update=False):
    host = 'http://127.0.0.1:5000'
    action = 'none'
    if update == True:
        action = 'update'
    response = requests.get(
        '{host}/dataset/{group_id}?={action}'
        .format(host=host, group_id=group_id, action=action))

    if not response.ok:
        raise ConnectionError('Bad response code')

    df = pd.read_json(response.text, orient='records')

    ds = df.groupby('employment')['id'].count().sort_values(ascending=False).head(5)
    return ds.to_json()

def getVacancy(group_id,update=False):
    host = 'http://127.0.0.1:5000'
    action = 'none'
    if update == True:
        action = 'update'
    response = requests.get(
        '{host}/dataset/{group_id}?={action}'
        .format(host=host, group_id=group_id, action=action))

    if not response.ok:
        raise ConnectionError('Bad response code')

    df = pd.read_json(response.text, orient='records')

    ds = df.groupby('name')['id'].count().sort_values(ascending=False).head(5)
    return ds.to_json()


def getSkill(group_id,update=False):
    host = 'http://127.0.0.1:5000'
    action = 'none'
    if update == True:
        action = 'update'
    response = requests.get(
        '{host}/dataset_skill/{group_id}?={action}'
        .format(host=host, group_id=group_id, action=action))

    if not response.ok:
        raise ConnectionError('Bad response code')

    df = pd.read_json(response.text, orient='records')


    ds = df.groupby('skill')['vacancy'].count().sort_values(ascending=False).head(5)
    return ds.to_json()

def delete_files_in_directory(directory_path):
   try:
     files = os.listdir(directory_path)
     for file in files:
       file_path = os.path.join(directory_path, file)
       if os.path.isfile(file_path):
         os.remove(file_path)
     print("All files deleted successfully.")
   except OSError:
     print("Error occurred while deleting files.")
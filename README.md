## ETL Covid19
___

Приветствую, это мой первый небольшой ETL pipeline.
В данном пайплайне выгружаются, преобразовываются данные об статистике заболеваниях covid19
для пяти стран: Россия, США, Китай, Индия, Бразилия.

### Описание
___
Выгрузка данных об заболевания covid19( public data by Johns Hopkins CSSE ) просиходит из api который предоставляет сайт rapidapi.com <br>
Эти данные загружаются в таблицу базы данных covid19_stage . <br>
Затем эти данные читаются из базы данных stage и трансформируются с помощью pandas и загружаются в базу данных warehouse.<br>
Далее происходит проверка целостности данных по количеству загруженных строк с погрешностью 2% (data_quality)
Данные о состоянии пайплайна отправляются сообщением в telegram.

### Технологии
___
1. Airflow в качестве оркестратора и планировщика выполнения задач. <br>
2. PostgreSQL используется как хранилище метаданных airflow, а также для храннения данных о статистике заболеваний.<br>
3. Pandas производит трансформацию и склеивание данных об статистике из пяти стран. <br>
4. Docker-compose контейнизатор для сборки и развертывания пайплайна.

### Начнем
___
1. Необходимо клонировать репозиторий на ваш ПК.
2. Отркыть терминал в папке куда загружен репозиторий и ввести команды:
```commandline
mkdir -p ./logs
echo -e "AIRFLOW_UID=$(id -u)" > .env
```
3. Можно запускать проект:
```commandline
docker-compose up -d
```
4. Заходим в браузер и вводим<br>
```commandline
localhost:8080
```
+ Имя учетной записи: airflow <br>
+ Пароль: airflow <br>

### Примечания
___
1. Для тестирования сервиса отправки сообщений в Telegram необходимо в файл .env ввести <b>TELEGRAM_API_TOKEN</b> бота и Ваш <b>TELEGRAM_CHAT_ID</b>.
   Затем в файле dags/ETL_covid19.py разкомментировать строки.

2. База Postgresql на которой находятся выгруженные данные о заболеваниях covid19 открыта на порту 5433 <br>
   Логин: bmk <br>
   Пароль: bmk <br>

### Скриншоты
___
![ETL_covid19_dag_graph](https://github.com/Bambik-git/ETL_covid19/blob/main/img/airflow_dag_graph.png)
![Telegram message](https://github.com/Bambik-git/ETL_covid19/blob/main/img/telegram.png)

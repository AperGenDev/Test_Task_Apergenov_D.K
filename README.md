# Test_Task_Apergenov_D.K

Проект реализует ETL-пайплайн для выгрузки данных прогнозов погоды.
Данные загружаются из API в csv файл и PostgreSQL.

## Структура проекта
```
weather_project/
├── data/                   
│   └── MyWeather.csv             # Итоговый CSV-файл
├── scripts/                 
│   ├── Test_task.ipynb           # Python-скрипты
│   ├── init.sql                  # Инициализация БД
├── docker-compose.yml       # Конфигурация контейнеров
```
## Запуск проекта

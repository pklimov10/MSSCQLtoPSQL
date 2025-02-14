# MSSCQLtoPSQL 


# MSSQL to PostgreSQL Converter

Утилита для конвертации SQL-скриптов из формата Microsoft SQL Server в PostgreSQL. Поддерживает конвертацию CREATE TABLE и INSERT-запросов с автоматическим преобразованием типов данных и синтаксиса.

## Возможности

- Конвертация CREATE TABLE запросов
- Преобразование INSERT запросов
- Автоматическое преобразование типов данных MSSQL в PostgreSQL
- Поддержка кодировок UTF-8 и CP1251
- Обработка IDENTITY полей
- Сохранение PRIMARY KEY ограничений
- Логирование процесса конвертации

## Поддерживаемые преобразования типов

| MSSQL тип | PostgreSQL тип |
|-----------|----------------|
| NVARCHAR | VARCHAR |
| NTEXT | TEXT |
| DATETIME | TIMESTAMP |
| SMALLDATETIME | TIMESTAMP |
| UNIQUEIDENTIFIER | UUID |
| MONEY | DECIMAL(19,4) |
| SMALLMONEY | DECIMAL(10,4) |
| IMAGE | BYTEA |
| BIT | BOOLEAN |
| TINYINT | SMALLINT |
| REAL | REAL |
| FLOAT | DOUBLE PRECISION |
| VARBINARY | BYTEA |
| BINARY | BYTEA |
| TIMESTAMP | BYTEA |

## Установка

```bash
git clone <repository-url>
cd mssql-to-postgresql-converter
```

## Использование

```bash
python script.py input_file.sql output_file.sql
```

### Параметры

- `input_file.sql` - путь к исходному файлу с MSSQL скриптом
- `output_file.sql` - путь к файлу, в который будет сохранен результат конвертации

## Особенности работы

### Обработка CREATE TABLE

- Удаление специфичных для MSSQL конструкций (например, `ON [PRIMARY]`)
- Преобразование IDENTITY в SERIAL
- Сохранение и преобразование PRIMARY KEY ограничений
- Конвертация типов данных согласно таблице соответствия

### Обработка INSERT

- Поддержка множественных INSERT запросов
- Обработка N-префикса строковых литералов
- Конвертация CAST выражений
- Обработка NULL значений
- Приведение имен таблиц и столбцов к нижнему регистру

### Дополнительные возможности

- Автоматическое определение кодировки входного файла
- Подробное логирование процесса конвертации
- Пропуск специфичных для MSSQL команд (USE, SET)
- Очистка идентификаторов от квадратных скобок и схемы dbo

## Обработка ошибок

Утилита включает в себя:
- Логирование всех этапов конвертации
- Обработку исключений при чтении файлов
- Проверку соответствия количества столбцов и значений в INSERT
- Восстановление после ошибок парсинга

## Требования

- Python 3.6+
- Стандартная библиотека Python

## Логирование

Логи содержат информацию о:
- Начале и завершении конвертации
- Ошибках при чтении файлов
- Проблемах парсинга SQL запросов
- Несоответствиях в структуре данных

## Ограничения

- Не поддерживается конвертация хранимых процедур
- Не обрабатываются специфичные для MSSQL индексы
- Некоторые сложные CAST выражения могут требовать ручной доработки

## Примеры

### Конвертация CREATE TABLE

```sql
-- MSSQL
CREATE TABLE [dbo].[Users] (
    [Id] INT IDENTITY(1,1),
    [Name] NVARCHAR(100),
    [CreateDate] DATETIME,
    PRIMARY KEY ([Id])
)

-- PostgreSQL (после конвертации)
CREATE TABLE users (
    id serial,
    name varchar(100),
    createdate timestamp,
    PRIMARY KEY (id)
);
```

### Конвертация INSERT

```sql
-- MSSQL
INSERT INTO [dbo].[Users] ([Name], [CreateDate])
VALUES (N'John', CAST('2024-01-01' AS DATETIME))

-- PostgreSQL (после конвертации)
INSERT INTO users (name, createdate)
VALUES ('John', '2024-01-01'::timestamp);
```

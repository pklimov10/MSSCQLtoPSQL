import re
import logging
from typing import List, Dict

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Словарь соответствия типов данных
TYPE_MAPPING = {
    'NVARCHAR': 'VARCHAR',
    'NTEXT': 'TEXT',
    'DATETIME': 'TIMESTAMP',
    'SMALLDATETIME': 'TIMESTAMP',
    'UNIQUEIDENTIFIER': 'UUID',
    'MONEY': 'DECIMAL(19,4)',
    'SMALLMONEY': 'DECIMAL(10,4)',
    'IMAGE': 'BYTEA',
    'BIT': 'BOOLEAN',
    'TINYINT': 'SMALLINT',
    'REAL': 'REAL',
    'FLOAT': 'DOUBLE PRECISION',
    'VARBINARY': 'BYTEA',
    'BINARY': 'BYTEA',
    'TIMESTAMP': 'BYTEA'
}

def read_sql_file(file_path: str) -> str:
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return f.read()
    except UnicodeDecodeError:
        with open(file_path, 'r', encoding='cp1251') as f:
            return f.read()
    except Exception as e:
        logger.error(f"Error reading file: {e}")
        raise

def clean_identifier(identifier: str) -> str:
    """Очищает идентификатор от квадратных скобок и dbo."""
    identifier = re.sub(r'\[dbo\]\.', '', identifier)
    identifier = re.sub(r'\[([^\]]+)\]', r'\1', identifier)
    return identifier.strip().lower()  # PostgreSQL предпочитает нижний регистр

def extract_type_size(data_type: str) -> tuple:
    """Извлекает тип данных и его размер (если есть)."""
    size_match = re.search(r'(\w+)\s*\((\d+(?:,\s*\d+)?)\)', data_type)
    if size_match:
        base_type = size_match.group(1).upper()
        size = size_match.group(2)
        return base_type, size
    return data_type.upper(), None

def convert_type_with_size(mssql_type: str) -> str:
    """Конвертирует тип данных с сохранением размера."""
    base_type, size = extract_type_size(mssql_type)

    if base_type in TYPE_MAPPING:
        pg_type = TYPE_MAPPING[base_type]
        # Если у нового типа уже есть скобки (например DECIMAL(19,4)), не добавляем размер
        if '(' in pg_type or size is None:
            return pg_type
        return f"{pg_type}({size})"
    elif size is not None:
        return f"{base_type.lower()}({size})"
    else:
        return mssql_type.lower()

def convert_insert(statement: str) -> str:
    """Конвертирует INSERT-запрос из MSSQL в PostgreSQL формат."""
    if 'SET IDENTITY_INSERT' in statement.upper():
        return ''

    # Разделяем множественные INSERT-запросы
    # Улучшенный поиск INSERT-запросов с учетом сложной структуры
    insert_statements = []
    current_stmt = ""
    in_insert = False

    for line in statement.split('\n'):
        if re.search(r'^\s*INSERT\s+', line, re.IGNORECASE):
            if in_insert and current_stmt:
                insert_statements.append(current_stmt)
            current_stmt = line
            in_insert = True
        elif in_insert:
            current_stmt += "\n" + line

    if in_insert and current_stmt:
        insert_statements.append(current_stmt)

    if not insert_statements:
        logger.warning(f"Could not parse INSERT statements: {statement[:100]}...")
        return statement

    converted_statements = []

    for insert_stmt in insert_statements:
        # Извлекаем имя таблицы
        table_match = re.search(r'INSERT\s+(?:\[dbo\]\.)?(?:\[)?([^\]]+)(?:\])?\s*\(', insert_stmt, re.IGNORECASE)
        if not table_match:
            logger.warning(f"Could not parse table name in INSERT statement: {insert_stmt[:100]}...")
            continue

        table_name = clean_identifier(table_match.group(1))

        # Извлекаем столбцы
        columns_match = re.search(r'\((.*?)\)\s*VALUES', insert_stmt, re.IGNORECASE | re.DOTALL)
        if not columns_match:
            logger.warning(f"Could not parse columns in INSERT statement: {insert_stmt[:100]}...")
            continue

        # Очищаем имена столбцов
        columns = [clean_identifier(col) for col in columns_match.group(1).split(',')]

        # Извлекаем значения
        # Более надежный поиск значений с учетом переносов строк
        values_match = re.search(r'VALUES\s*\((.*)\)', insert_stmt, re.IGNORECASE | re.DOTALL)
        if not values_match:
            logger.warning(f"Could not parse VALUES in INSERT statement: {insert_stmt[:100]}...")
            continue

        # Обработка значений с учетом вложенных скобок
        values_str = values_match.group(1)
        values = []
        current_value = ""
        bracket_count = 0
        in_string = False
        string_char = None

        for char in values_str:
            if char in ["'", '"'] and (not string_char or char == string_char):
                if not in_string:
                    in_string = True
                    string_char = char
                else:
                    in_string = False
                    string_char = None
                current_value += char
            elif char == '(' and not in_string:
                bracket_count += 1
                current_value += char
            elif char == ')' and not in_string:
                bracket_count -= 1
                current_value += char
            elif char == ',' and bracket_count == 0 and not in_string:
                values.append(current_value.strip())
                current_value = ""
            else:
                current_value += char

        if current_value:
            values.append(current_value.strip())

        # Если количество значений не соответствует количеству столбцов, пытаемся исправить
        if len(values) != len(columns):
            # Проверяем, есть ли в строке VALUES несколько наборов значений
            all_values_match = re.findall(r'VALUES\s*\((.*?)\)', insert_stmt, re.IGNORECASE | re.DOTALL)
            if len(all_values_match) > 1:
                # Обрабатываем множественные VALUES
                processed_statements = []
                for value_set in all_values_match:
                    # Разбиваем значения и обрабатываем их
                    value_items = []
                    current_item = ""
                    bracket_count = 0
                    in_string = False
                    string_char = None

                    for char in value_set:
                        if char in ["'", '"'] and (not string_char or char == string_char):
                            if not in_string:
                                in_string = True
                                string_char = char
                            else:
                                in_string = False
                                string_char = None
                            current_item += char
                        elif char == '(' and not in_string:
                            bracket_count += 1
                            current_item += char
                        elif char == ')' and not in_string:
                            bracket_count -= 1
                            current_item += char
                        elif char == ',' and bracket_count == 0 and not in_string:
                            value_items.append(current_item.strip())
                            current_item = ""
                        else:
                            current_item += char

                    if current_item:
                        value_items.append(current_item.strip())

                    # Обрабатываем значения и создаем INSERT
                    processed_values = []
                    for value in value_items:
                        if value.upper() == 'NULL':
                            processed_values.append('NULL')
                        elif re.match(r'^-?\d+(\.\d+)?$', value):
                            processed_values.append(value)
                        elif value.startswith("N'"):
                            processed_values.append(value[1:])
                        elif 'CAST' in value.upper():
                            cast_match = re.search(r"CAST\(([^AS]+)AS\s+([^\)]+)\)", value, re.IGNORECASE)
                            if cast_match:
                                cast_value = cast_match.group(1).strip()
                                cast_type = cast_match.group(2).strip()
                                pg_type = TYPE_MAPPING.get(cast_type.upper(), cast_type.lower())
                                processed_values.append(f"{cast_value}::{pg_type}")
                        else:
                            processed_values.append(value)

                    if len(processed_values) == len(columns):
                        columns_str = ', '.join(columns)
                        values_str = ', '.join(processed_values)
                        processed_statements.append(f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});")

                if processed_statements:
                    converted_statements.extend(processed_statements)
                    continue

            logger.warning(f"Column count ({len(columns)}) does not match values count ({len(values)}) in: {insert_stmt[:100]}...")
            # Попытка восстановить значения, если их не хватает
            if len(values) < len(columns):
                values.extend(['NULL'] * (len(columns) - len(values)))
            elif len(values) > len(columns):
                values = values[:len(columns)]

        processed_values = []
        for value in values:
            value = value.strip()
            # Обработка NULL
            if value.upper() == 'NULL':
                processed_values.append('NULL')
            # Обработка чисел
            elif re.match(r'^-?\d+(\.\d+)?$', value):
                processed_values.append(value)
            # Обработка строк с N префиксом
            elif value.startswith("N'"):
                processed_values.append(value[1:])
            # Обработка CAST выражений
            elif 'CAST' in value.upper():
                cast_match = re.search(r"CAST\(([^AS]+)AS\s+([^\)]+)\)", value, re.IGNORECASE)
                if cast_match:
                    cast_value = cast_match.group(1).strip()
                    cast_type = cast_match.group(2).strip()
                    # Конвертируем тип данных если нужно
                    pg_type = TYPE_MAPPING.get(cast_type.upper(), cast_type.lower())
                    processed_values.append(f"{cast_value}::{pg_type}")
            # Остальные значения оставляем как есть
            else:
                processed_values.append(value)

        # Собираем новый INSERT-запрос
        columns_str = ', '.join(columns)
        values_str = ', '.join(processed_values)
        insert_statement = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str});"
        converted_statements.append(insert_statement)

    return '\n'.join(converted_statements)

def convert_create_table(statement: str) -> str:
    """Конвертирует CREATE TABLE из MSSQL в PostgreSQL."""
    # Убираем USE statement и другие MS SQL специфичные команды
    if statement.upper().startswith('USE'):
        return ''

    # Извлекаем имя таблицы
    table_match = re.search(r'CREATE\s+TABLE\s+(?:\[dbo\]\.)?(?:\[)?([^\]]+)(?:\])?', statement, re.IGNORECASE)
    if not table_match:
        logger.warning(f"Could not parse table name in CREATE TABLE statement: {statement[:100]}...")
        return statement

    table_name = clean_identifier(table_match.group(1))

    # Ищем основное содержимое таблицы
    main_content = re.search(r'\((.*)\)\s*ON\s*\[PRIMARY\]', statement, re.IGNORECASE | re.DOTALL)
    if not main_content:
        # Пробуем найти содержимое без ON [PRIMARY]
        main_content = re.search(r'\((.*?)\)(?:\s*GO)?$', statement, re.IGNORECASE | re.DOTALL)
        if not main_content:
            logger.warning(f"Could not parse table content in CREATE TABLE statement: {statement[:100]}...")
            return statement

    content = main_content.group(1)

    # Разбиваем на части и очищаем
    parts = []

    # Флаги для отслеживания ограничений
    primary_key_columns = []

    # Разбиваем контент на строки, сохраняя структуру PRIMARY KEY
    current_definition = ""
    bracket_count = 0

    for char in content:
        current_definition += char
        if char == '(':
            bracket_count += 1
        elif char == ')':
            bracket_count -= 1
        elif char == ',' and bracket_count == 0:
            # Обрабатываем текущее определение
            definition = current_definition.strip().rstrip(',')
            if definition:
                # Проверяем, является ли это PRIMARY KEY CLUSTERED
                pk_match = re.search(r'PRIMARY\s+KEY\s+CLUSTERED\s*\(\s*\[?([^\]]+)\]?\s*(?:ASC|DESC)?\s*\)',
                                   definition, re.IGNORECASE)
                if pk_match:
                    primary_key_columns.append(clean_identifier(pk_match.group(1)))
                # Проверяем, является ли это CONSTRAINT PRIMARY KEY
                elif 'CONSTRAINT' in definition.upper() and 'PRIMARY KEY' in definition.upper():
                    pk_match = re.search(r'PRIMARY\s+KEY\s*\(\s*\[?([^\]]+)\]?\s*(?:ASC|DESC)?\s*\)',
                                       definition, re.IGNORECASE)
                    if pk_match:
                        primary_key_columns.append(clean_identifier(pk_match.group(1)))
                # Обычное определение колонки
                elif not re.search(r'WITH\s*\(', definition, re.IGNORECASE):
                    # Конвертируем типы данных
                    for mssql_type in TYPE_MAPPING.keys():
                        pattern = r'\b' + re.escape(mssql_type) + r'(?:\s*\(\s*(\d+)(?:\s*,\s*\d+)?\s*\))?'
                        type_match = re.search(pattern, definition, re.IGNORECASE)
                        if type_match:
                            old_type = type_match.group(0)
                            new_type = convert_type_with_size(old_type)
                            definition = definition.replace(old_type, new_type)
                            break

                    # Заменяем IDENTITY на SERIAL
                    definition = re.sub(r'IDENTITY\(\d+,\s*\d+\)', 'SERIAL', definition, flags=re.IGNORECASE)

                    # Очищаем от квадратных скобок
                    definition = clean_identifier(definition)

                    if definition:
                        parts.append(definition)
            current_definition = ""

    # Обрабатываем последнее определение
    if current_definition.strip():
        definition = current_definition.strip()
        if not re.search(r'WITH\s*\(', definition, re.IGNORECASE):
            parts.append(clean_identifier(definition))

    # Добавляем PRIMARY KEY в конец, если нашли
    if primary_key_columns:
        parts.append(f"PRIMARY KEY ({', '.join(primary_key_columns)})")

    # Собираем финальный CREATE TABLE
    create_statement = f"CREATE TABLE {table_name} (\n    "
    create_statement += ',\n    '.join(parts)
    create_statement += "\n);"

    return create_statement


def convert_mssql_to_postgresql(input_file: str, output_file: str):
    try:
        logger.info(f"Reading file {input_file}")
        content = read_sql_file(input_file)

        # Разделяем файл на блоки по GO
        blocks = re.split(r'\bGO\b', content, flags=re.IGNORECASE)

        converted_statements = []
        for block in blocks:
            if not block.strip():
                continue

            # Разделяем блок на отдельные команды по точке с запятой
            # Учитываем, что точка с запятой может быть в строках в кавычках
            statements = []
            current_stmt = ""
            in_string = False
            string_char = None

            for char in block:
                if char in ["'", '"'] and (not string_char or char == string_char):
                    if not in_string:
                        in_string = True
                        string_char = char
                    else:
                        in_string = False
                        string_char = None
                    current_stmt += char
                elif char == ';' and not in_string:
                    current_stmt += char
                    if current_stmt.strip():
                        statements.append(current_stmt.strip())
                    current_stmt = ""
                else:
                    current_stmt += char

            if current_stmt.strip():
                statements.append(current_stmt.strip())

            for stmt in statements:
                stmt = stmt.strip()
                if not stmt:
                    continue

                stmt_upper = stmt.upper()
                if 'CREATE TABLE' in stmt_upper:
                    converted = convert_create_table(stmt)
                    if converted:
                        converted_statements.append(converted)
                elif 'INSERT' in stmt_upper:
                    converted = convert_insert(stmt)
                    if converted:
                        converted_statements.append(converted)
                elif not any(cmd in stmt_upper for cmd in ['USE', 'SET']):
                    # Убираем точку с запятой, если она есть, и добавляем снова
                    if stmt.endswith(';'):
                        stmt = stmt[:-1]
                    converted = clean_identifier(stmt) + ";"
                    converted_statements.append(converted)

        logger.info(f"Writing output to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            for stmt in converted_statements:
                f.write(f"{stmt}\n\n")

        logger.info("Conversion completed successfully")

    except Exception as e:
        logger.error(f"Error during conversion: {e}")
        raise

if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python script.py input_file.sql output_file.sql")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]

    try:
        convert_mssql_to_postgresql(input_file, output_file)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

import os
import sys
from pathlib import Path
from dotenv import load_dotenv

# Добавляем корневую папку проекта в sys.path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Загружаем .env файлы
env_files = ['.env.test']
for env_file in env_files:
    env_path = project_root / env_file
    if env_path.exists():
        load_dotenv(env_path)
        print(f"Загружен {env_file}")

from src.utils.env import get_int, get_str
from src.validators.config_validator import validate_settings


def extract_settings_from_file(file_path):
    # Читает файл и выполняет только settings
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()

        # Выполняем файл в отдельном namespace
        namespace = {
            'get_str': get_str,
            'get_int': get_int,
            'os': os,
            '__builtins__': __builtins__
        }

        exec(content, namespace)

        # Ищем settings
        found_settings = []
        for var_name in ['settings', 'settings_get', 'settings_put']:
            if var_name in namespace and isinstance(namespace[var_name], dict):
                found_settings.append((var_name, namespace[var_name]))

        return found_settings

    except Exception as e:
        print(f"   ERROR: Ошибка чтения/выполнения файла: {e}")
        return []


def main():
    dags_path = project_root / "dags"

    total_files = 0
    total_configs = 0
    valid_configs = 0
    errors_found = []

    print("Поиск дагов и валидация settings...")
    print("=" * 60)

    # Проходим по всем .py файлам в dags
    for py_file in dags_path.rglob("*.py"):
        if py_file.name.startswith('__'):
            continue

        total_files += 1
        relative_path = str(py_file.relative_to(project_root))
        print(f"\n{relative_path}")

        # Извлекаем settings через exec
        found_settings = extract_settings_from_file(py_file)

        if not found_settings:
            print("   WARNING: Переменные settings не найдены")
            continue

        # Валидируем каждую найденную settings
        for var_name, settings in found_settings:
            total_configs += 1
            print(f"   Проверяю {var_name}...")

            try:
                validate_settings(settings)
                valid_configs += 1
                print(f"   OK: {var_name} - валидна")
            except ValueError as e:
                print(f"   ERROR: {var_name} - НЕ валидна: {e}")
                errors_found.append((relative_path, var_name, str(e)))

    # Итоговый отчет
    print("\n" + "=" * 60)
    print("ИТОГО:")
    print(f"   Файлов проверено: {total_files}")
    print(f"   Конфигураций найдено: {total_configs}")
    print(f"   Валидных: {valid_configs}")
    print(f"   НЕ валидных: {total_configs - valid_configs}")

    if errors_found:
        print(f"\nНАЙДЕНО {len(errors_found)} ОШИБОК:")
        for file_path, var_name, error in errors_found:
            print(f"   {file_path} -> {var_name}: {error}")
        sys.exit(1)
    elif total_configs == 0:
        print("\nWARNING: Конфигурации не найдены!")
        sys.exit(2)
    else:
        print(f"\nSUCCESS: ВСЕ {valid_configs} КОНФИГУРАЦИЙ ВАЛИДНЫ!")
        sys.exit(0)


if __name__ == "__main__":
    main()
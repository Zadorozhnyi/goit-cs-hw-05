import asyncio
import aiofiles
import shutil
import os
import argparse
import logging
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

def setup_logging():
    """Налаштовує логування."""
    logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

def create_argument_parser():
    """Створює об'єкт ArgumentParser для обробки аргументів командного рядка."""
    parser = argparse.ArgumentParser(description="Асинхронний сортувальник файлів за розширенням")
    parser.add_argument("source", type=str, help="Шлях до вихідної папки")
    parser.add_argument("output", type=str, help="Шлях до цільової папки")
    return parser

def cpu_bound_task(file_path, target_path):
    """Виконує CPU-bound операцію (імітація)."""
    shutil.copy2(file_path, target_path)  # Може бути замінено на складніші операції

async def copy_file_io_bound(src: Path, dest_folder: Path):
    """Асинхронно копіює файл у відповідну підпапку на основі розширення (I/O-bound)."""
    try:
        ext = src.suffix.lstrip('.') or 'unknown'
        target_folder = dest_folder / ext
        target_folder.mkdir(parents=True, exist_ok=True)
        target_path = target_folder / src.name
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(ThreadPoolExecutor(), shutil.copy2, src, target_path)
        print(f"(I/O) Файл {src} скопійовано у {target_path}")
    except Exception as e:
        logging.error(f"Помилка при копіюванні (I/O) {src}: {e}")

async def copy_file_cpu_bound(src: Path, dest_folder: Path, executor):
    """Асинхронно виконує CPU-bound завдання для копіювання файлів."""
    try:
        ext = src.suffix.lstrip('.') or 'unknown'
        target_folder = dest_folder / ext
        target_folder.mkdir(parents=True, exist_ok=True)
        target_path = target_folder / src.name
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(executor, cpu_bound_task, src, target_path)
        print(f"(CPU) Файл {src} скопійовано у {target_path}")
    except Exception as e:
        logging.error(f"Помилка при копіюванні (CPU) {src}: {e}")

async def read_folder(src_folder: Path, dest_folder: Path):
    """Асинхронно читає всі файли у вихідній папці та передає їх для копіювання."""
    tasks_io = []
    tasks_cpu = []
    executor = ProcessPoolExecutor()
    
    for root, _, files in os.walk(src_folder):
        for file in files:
            file_path = Path(root) / file
            if file_path.suffix in {'.zip', '.rar'}:  # Імітація CPU-bound роботи
                tasks_cpu.append(copy_file_cpu_bound(file_path, dest_folder, executor))
            else:
                tasks_io.append(copy_file_io_bound(file_path, dest_folder))
    
    await asyncio.gather(*tasks_io, *tasks_cpu, return_exceptions=True)
    executor.shutdown()

async def main():
    """Обробка аргументів командного рядка та запуск процесу сортування файлів."""
    setup_logging()
    parser = create_argument_parser()
    args = parser.parse_args()

    src_folder = Path(args.source)
    dest_folder = Path(args.output)

    if not src_folder.exists() or not src_folder.is_dir():
        logging.error("Вихідна папка не існує або не є директорією")
        return
    
    dest_folder.mkdir(parents=True, exist_ok=True)
    await read_folder(src_folder, dest_folder)

if __name__ == "__main__":
    asyncio.run(main())

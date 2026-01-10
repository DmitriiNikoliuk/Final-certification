import tkinter as tk
import logging
from gui import DataCenterGUI

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    """Основная функция приложения"""
    try:
        root = tk.Tk()
        app = DataCenterGUI(root)
        root.mainloop()
    except Exception as e:
        logging.error(f"Ошибка при запуске приложения: {e}")
        raise

if __name__ == "__main__":
    main()
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import pandas as pd
from datetime import datetime
import asyncio
import threading
import logging

from database import Database
from models import VirtualMachine, Hypervisor, Cluster
from utils import Validator, NameGenerator, ResourceCalculator, Formatter
from async_operations import AsyncOperations
from analysis import DataAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataCenterGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("Учет инфраструктуры кластера Москва")
        self.root.geometry("1200x700")
        
        # Инициализация компонентов
        self.db = Database()
        self.analyzer = DataAnalyzer(self.db)
        self.async_ops = AsyncOperations(self.db)
        self.cluster = Cluster()
        
        # Создание вкладок
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Создание вкладок
        self.create_vm_tab()
        self.create_hypervisor_tab()
        self.create_analysis_tab()
        
        # Обновление данных при запуске
        self.refresh_vm_data()
        self.refresh_hv_data()
        self.update_cluster_info()
    
    def update_cluster_info(self):
        """Обновление информации о кластере в заголовке"""
        try:
            config = self.db.get_cluster_config()
            stats = self.db.get_cluster_statistics()
            
            title = f"Кластер: {config.get('cluster_name', 'Москва')} | "
            title += f"Гипервизоров: {stats.get('total_hypervisors', 0)} | "
            title += f"ВМ: {stats.get('total_vms', 0)}"
            
            self.root.title(title)
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении информации о кластере: {e}")
    
    def create_vm_tab(self):
        """Создание вкладки для виртуальных машин"""
        vm_frame = ttk.Frame(self.notebook)
        self.notebook.add(vm_frame, text="Виртуальные машины")
        
        # Верхняя панель с кнопками
        control_frame = ttk.Frame(vm_frame)
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Поля для ввода данных ВМ
        ttk.Label(control_frame, text="Имя ВМ:").grid(row=0, column=0, padx=5, pady=2)
        self.vm_name_entry = ttk.Entry(control_frame, width=20)
        self.vm_name_entry.grid(row=0, column=1, padx=5, pady=2)
        self.vm_name_entry.insert(0, "vm77app01")
        
        ttk.Label(control_frame, text="vCPU:").grid(row=0, column=2, padx=5, pady=2)
        self.vm_cpu_entry = ttk.Entry(control_frame, width=10)
        self.vm_cpu_entry.grid(row=0, column=3, padx=5, pady=2)
        self.vm_cpu_entry.insert(0, "2")
        
        ttk.Label(control_frame, text="vRAM (ГБ):").grid(row=0, column=4, padx=5, pady=2)
        self.vm_ram_entry = ttk.Entry(control_frame, width=10)
        self.vm_ram_entry.grid(row=0, column=5, padx=5, pady=2)
        self.vm_ram_entry.insert(0, "4")
        
        ttk.Label(control_frame, text="vHDD (ГБ):").grid(row=0, column=6, padx=5, pady=2)
        self.vm_hdd_entry = ttk.Entry(control_frame, width=10)
        self.vm_hdd_entry.grid(row=0, column=7, padx=5, pady=2)
        self.vm_hdd_entry.insert(0, "40")
        
        ttk.Label(control_frame, text="Количество:").grid(row=1, column=0, padx=5, pady=2)
        self.vm_count_entry = ttk.Entry(control_frame, width=10)
        self.vm_count_entry.grid(row=1, column=1, padx=5, pady=2)
        self.vm_count_entry.insert(0, "1")
        
        # Кнопки операций
        ttk.Button(control_frame, text="Создать ВМ", 
                  command=self.create_vm).grid(row=1, column=2, padx=5, pady=2)
        ttk.Button(control_frame, text="Массовое создание", 
                  command=self.mass_deploy_vms).grid(row=1, column=3, padx=5, pady=2)
        ttk.Button(control_frame, text="Удалить ВМ", 
                  command=self.delete_vm).grid(row=1, column=4, padx=5, pady=2)
        ttk.Button(control_frame, text="Обновить", 
                  command=self.refresh_vm_data).grid(row=1, column=5, padx=5, pady=2)
        ttk.Button(control_frame, text="Ограничения", 
                  command=self.show_vm_limits).grid(row=1, column=6, padx=5, pady=2)
        ttk.Button(control_frame, text="Сгенерировать имя",
              command=self.generate_vm_name).grid(row=1, column=7, padx=5, pady=2)   
        
        # Информационная панель
        info_frame = ttk.Frame(vm_frame)
        info_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.vm_info_label = ttk.Label(info_frame, text="", foreground="blue", font=('Arial', 10))
        self.vm_info_label.pack(anchor=tk.W)
        
        # Таблица с ВМ
        tree_frame = ttk.Frame(vm_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        columns = ("Имя ВМ", "vCPU", "vRAM (ГБ)", "vHDD (ГБ)", "Гипервизор", "Дата создания", "Тип")
        self.vm_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=20)
        
        column_widths = [120, 70, 90, 90, 100, 150, 120]
        for idx, col in enumerate(columns):
            self.vm_tree.heading(col, text=col)
            self.vm_tree.column(col, width=column_widths[idx])
        
        # Добавляем скроллбар
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.vm_tree.yview)
        self.vm_tree.configure(yscrollcommand=scrollbar.set)
        
        self.vm_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Привязываем событие выбора
        self.vm_tree.bind('<<TreeviewSelect>>', self.on_vm_selected)
    
    def show_vm_limits(self):
        """Показать ограничения для ВМ"""
        limits_text = "ОГРАНИЧЕНИЯ ДЛЯ ВИРТУАЛЬНЫХ МАШИН:\n\n"
        limits_text += "• Имя: vm77[app|db|ts]XX (пример: vm77app01)\n"
        limits_text += "• vCPU: от 2 до 24 ядер (кратно 2)\n"
        limits_text += "• vRAM: от 4 до 128 ГБ\n"
        limits_text += "• vHDD: от 40 до 4096 ГБ\n\n"
        limits_text += "ПРИМЕРЫ ИМЁН:\n"
        limits_text += "• vm77app01 - сервер приложений\n"
        limits_text += "• vm77db01 - сервер базы данных\n"
        limits_text += "• vm77ts01 - терминальный сервер"
        
        messagebox.showinfo("Ограничения ВМ", limits_text)
    
    def on_vm_selected(self, event):
        """Обработчик выбора ВМ в таблице"""
        selection = self.vm_tree.selection()
        if selection:
            item = self.vm_tree.item(selection[0])
            vm_name = item['values'][0]
            vm_type = Formatter.format_vm_type(vm_name)
            self.vm_info_label.config(text=f"Выбрана ВМ: {vm_name} ({vm_type})")
    
    def create_hypervisor_tab(self):
        """Создание вкладки для гипервизоров"""
        hv_frame = ttk.Frame(self.notebook)
        self.notebook.add(hv_frame, text="Гипервизоры")
        
        # Верхняя панель
        control_frame = ttk.Frame(hv_frame)
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        
        # Поля для ввода данных гипервизора
        ttk.Label(control_frame, text="Имя гипервизора:").grid(row=0, column=0, padx=5, pady=2)
        self.hv_name_entry = ttk.Entry(control_frame, width=20)
        self.hv_name_entry.grid(row=0, column=1, padx=5, pady=2)
        self.hv_name_entry.insert(0, "s77hv01")
        
        ttk.Label(control_frame, text="CPU (ядра):").grid(row=0, column=2, padx=5, pady=2)
        self.hv_cpu_entry = ttk.Entry(control_frame, width=10)
        self.hv_cpu_entry.grid(row=0, column=3, padx=5, pady=2)
        self.hv_cpu_entry.insert(0, "48")
        
        ttk.Label(control_frame, text="RAM (ГБ):").grid(row=0, column=4, padx=5, pady=2)
        self.hv_ram_entry = ttk.Entry(control_frame, width=10)
        self.hv_ram_entry.grid(row=0, column=5, padx=5, pady=2)
        self.hv_ram_entry.insert(0, "256")
        
        # Кнопки операций
        ttk.Button(control_frame, text="Добавить гипервизор", 
                  command=self.add_hypervisor).grid(row=1, column=2, padx=5, pady=2)
        ttk.Button(control_frame, text="Удалить гипервизор", 
                  command=self.delete_hypervisor).grid(row=1, column=3, padx=5, pady=2)
        ttk.Button(control_frame, text="Обновить", 
                  command=self.refresh_hv_data).grid(row=1, column=4, padx=5, pady=2)
        ttk.Button(control_frame, text="Проверить ресурсы", 
                  command=self.check_resources).grid(row=1, column=5, padx=5, pady=2)
        ttk.Button(control_frame, text="Сгенерировать имя", 
                  command=self.generate_hv_name).grid(row=1, column=6, padx=5, pady=2)
        
        # Информационная панель о кластере
        info_frame = ttk.Frame(hv_frame)
        info_frame.pack(fill=tk.X, padx=5, pady=5)
        
        self.cluster_info_label = ttk.Label(info_frame, text="", foreground="green", font=('Arial', 10))
        self.cluster_info_label.pack(anchor=tk.W)
        
        # Таблица с гипервизорами
        tree_frame = ttk.Frame(hv_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        columns = ("Имя", "CPU", "RAM", "Свободно CPU", "Свободно RAM", 
                  "ВМ", "Исп. CPU%", "Исп. RAM%", "Статус CPU", "Статус RAM")
        self.hv_tree = ttk.Treeview(tree_frame, columns=columns, show="headings", height=20)
        
        column_widths = [100, 70, 70, 100, 100, 70, 90, 90, 90, 90]
        for idx, col in enumerate(columns):
            self.hv_tree.heading(col, text=col)
            self.hv_tree.column(col, width=column_widths[idx])
        
        scrollbar = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=self.hv_tree.yview)
        self.hv_tree.configure(yscrollcommand=scrollbar.set)
        
        self.hv_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # Привязываем событие выбора
        self.hv_tree.bind('<<TreeviewSelect>>', self.on_hv_selected)
        
        # Обновляем информацию о кластере
        self.update_cluster_status()
    
    def generate_hv_name(self):
        """Генерация имени для нового гипервизора"""
        try:
            existing_hvs = self.db.get_all_hypervisors()
            existing_names = [hv['hv_name'] for hv in existing_hvs]
            
            next_name = NameGenerator.get_next_hv_name(existing_names)
            self.hv_name_entry.delete(0, tk.END)
            self.hv_name_entry.insert(0, next_name)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сгенерировать имя: {str(e)}")
    
    def update_cluster_status(self):
        """Обновление статуса кластера"""
        try:
            stats = self.db.get_cluster_statistics()
            
            if stats.get('total_cpu', 0) > 0:
                cpu_usage = ResourceCalculator.calculate_cpu_usage(
                    stats['total_cpu'], stats.get('free_cpu', 0)
                )
                ram_usage = ResourceCalculator.calculate_ram_usage(
                    stats['total_ram'], stats.get('free_ram', 0)
                )
                
                info_text = f"Кластер: {stats.get('total_hypervisors', 0)} гипервизоров, "
                info_text += f"{stats.get('total_vms', 0)} ВМ | "
                info_text += f"Использование CPU: {cpu_usage:.1f}%, RAM: {ram_usage:.1f}%"
                
                self.cluster_info_label.config(text=info_text)
            
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса кластера: {e}")
    
    def on_hv_selected(self, event):
        """Обработчик выбора гипервизора в таблице"""
        selection = self.hv_tree.selection()
        if selection:
            item = self.hv_tree.item(selection[0])
            hv_name = item['values'][0]
            
            # Получаем дополнительную информацию о гипервизоре
            hvs = self.db.get_all_hypervisors()
            for hv in hvs:
                if hv['hv_name'] == hv_name:
                    cpu_usage = ResourceCalculator.calculate_cpu_usage(hv['cpu'], hv['free_cpu'])
                    ram_usage = ResourceCalculator.calculate_ram_usage(hv['ram'], hv['free_ram'])
                    
                    info_text = f"Выбран гипервизор: {hv_name} | "
                    info_text += f"Использование CPU: {cpu_usage:.1f}%, RAM: {ram_usage:.1f}% | "
                    info_text += f"ВМ: {hv['num_vms']}"
                    
                    self.cluster_info_label.config(text=info_text)
                    break
    
    def check_resources(self):
        """Проверка доступных ресурсов в кластере"""
        try:
            stats = self.db.get_cluster_statistics()
            
            if stats.get('total_cpu', 0) == 0:
                messagebox.showinfo("Ресурсы", "В кластере нет гипервизоров")
                return
            
            cpu_usage = ResourceCalculator.calculate_cpu_usage(
                stats['total_cpu'], stats.get('free_cpu', 0)
            )
            ram_usage = ResourceCalculator.calculate_ram_usage(
                stats['total_ram'], stats.get('free_ram', 0)
            )
            
            # Проверяем минимальные свободные ресурсы
            is_ok, message = ResourceCalculator.check_minimum_resources(
                stats['total_cpu'], stats.get('free_cpu', 0),
                stats['total_ram'], stats.get('free_ram', 0)
            )
            
            resources_text = "=== РЕСУРСЫ КЛАСТЕРА ===\n\n"
            resources_text += f"Гипервизоров: {stats.get('total_hypervisors', 0)}\n"
            resources_text += f"Виртуальных машин: {stats.get('total_vms', 0)}\n\n"
            resources_text += f"Общие ресурсы:\n"
            resources_text += f"• CPU: {stats['total_cpu']} ядер\n"
            resources_text += f"• RAM: {stats['total_ram']} ГБ\n\n"
            resources_text += f"Свободные ресурсы:\n"
            resources_text += f"• CPU: {stats.get('free_cpu', 0)} ядер\n"
            resources_text += f"• RAM: {stats.get('free_ram', 0)} ГБ\n\n"
            resources_text += f"Использование:\n"
            resources_text += f"• CPU: {cpu_usage:.1f}%\n"
            resources_text += f"• RAM: {ram_usage:.1f}%\n\n"
            resources_text += f"Статус: {message}"
            
            messagebox.showinfo("Ресурсы кластера", resources_text)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось проверить ресурсы: {str(e)}")
    
    def create_analysis_tab(self):
        """Создание вкладки для анализа и отчетов"""
        analysis_frame = ttk.Frame(self.notebook)
        self.notebook.add(analysis_frame, text="Анализ и отчеты")
        
        # Панель управления
        control_frame = ttk.Frame(analysis_frame)
        control_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(control_frame, text="Сгенерировать графики", 
                  command=self.generate_plots).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Экспорт в Excel", 
                  command=self.export_to_excel).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Показать статистику", 
                  command=self.show_statistics).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Обновить данные", 
                  command=self.refresh_analysis).pack(side=tk.LEFT, padx=5)
        ttk.Button(control_frame, text="Отчет по кластеру", 
                  command=self.cluster_report).pack(side=tk.LEFT, padx=5)
        
        # Область для вывода информации
        self.analysis_text = scrolledtext.ScrolledText(analysis_frame, width=100, height=30)
        self.analysis_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
    
    def refresh_analysis(self):
        """Обновление данных в анализе"""
        self.show_statistics()
        messagebox.showinfo("Обновлено", "Данные для анализа обновлены")
    
    # Методы для работы с ВМ
    def create_vm(self):
        """Создание виртуальной машины"""
        try:
            vm_name = self.vm_name_entry.get().strip()
            vcpu_text = self.vm_cpu_entry.get().strip()
            vram_text = self.vm_ram_entry.get().strip()
            vhdd_text = self.vm_hdd_entry.get().strip()
            
            if not vm_name or not vcpu_text or not vram_text or not vhdd_text:
                messagebox.showerror("Ошибка", "Заполните все поля")
                return
            
            vcpu = int(vcpu_text)
            vram = int(vram_text)
            vhdd = int(vhdd_text)
            
            # Валидация имени
            is_valid, message = Validator.validate_vm_name(vm_name)
            if not is_valid:
                messagebox.showerror("Ошибка", message)
                return
            
            # Валидация ресурсов
            is_valid, message = Validator.validate_vm_resources(vcpu, vram, vhdd)
            if not is_valid:
                messagebox.showerror("Ошибка", message)
                return
            
            # Создание ВМ
            vm_data = {
                'vm_name': vm_name,
                'vcpu': vcpu,
                'vram': vram,
                'vhdd': vhdd
            }
            
            success = self.db.create_vm(vm_data)
            if success:
                messagebox.showinfo("Успех", f"ВМ {vm_name} успешно создана")
                self.refresh_vm_data()
                self.refresh_hv_data()
                self.update_cluster_info()
                self.update_cluster_status()
            else:
                messagebox.showerror("Ошибка", "Не удалось создать ВМ. Проверьте наличие свободных ресурсов.")
                
        except ValueError as e:
            messagebox.showerror("Ошибка", "Проверьте правильность введенных числовых значений")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {str(e)}")
    
    def mass_deploy_vms(self):
        """Массовое развертывание ВМ"""
        try:
            base_name = self.vm_name_entry.get().strip()
            vcpu_text = self.vm_cpu_entry.get().strip()
            vram_text = self.vm_ram_entry.get().strip()
            vhdd_text = self.vm_hdd_entry.get().strip()
            count_text = self.vm_count_entry.get().strip()
            
            if not base_name or not vcpu_text or not vram_text or not vhdd_text or not count_text:
                messagebox.showerror("Ошибка", "Заполните все поля")
                return
            
            vcpu = int(vcpu_text)
            vram = int(vram_text)
            vhdd = int(vhdd_text)
            count = int(count_text)
            
            if count < 1:
                messagebox.showerror("Ошибка", "Количество должно быть положительным числом")
                return
            if count > 50:
                if not messagebox.askyesno("Подтверждение", 
                                          f"Вы уверены, что хотите создать {count} ВМ? Это может занять некоторое время."):
                    return
            
            # Валидация ресурсов
            is_valid, message = Validator.validate_vm_resources(vcpu, vram, vhdd)
            if not is_valid:
                messagebox.showerror("Ошибка", message)
                return
            
            # Проверяем базовое имя
            is_valid, message = Validator.validate_vm_name(base_name)
            if not is_valid:
                messagebox.showerror("Ошибка", message)
                return
            
            # Базовые данные ВМ
            vm_data = {
                'vm_name': base_name,
                'vcpu': vcpu,
                'vram': vram,
                'vhdd': vhdd
            }
            
            # Запуск в отдельном потоке
            def run_async():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    results = loop.run_until_complete(
                        self.async_ops.mass_deploy_vms(vm_data, count)
                    )
                    
                    success_count = sum(1 for r in results if r is True)
                    
                    self.root.after(0, lambda: messagebox.showinfo(
                        "Завершено", 
                        f"Создано {success_count} из {count} ВМ"
                    ))
                    
                    self.root.after(0, self.refresh_vm_data)
                    self.root.after(0, self.refresh_hv_data)
                    self.root.after(0, self.update_cluster_info)
                    self.root.after(0, self.update_cluster_status)
                    
                except Exception as e:
                    self.root.after(0, lambda: messagebox.showerror(
                        "Ошибка", f"Произошла ошибка: {str(e)}"
                    ))
                finally:
                    loop.close()
            
            thread = threading.Thread(target=run_async)
            thread.daemon = True
            thread.start()
            
            messagebox.showinfo("Запущено", f"Начато массовое создание {count} ВМ")
            
        except ValueError as e:
            messagebox.showerror("Ошибка", "Проверьте правильность введенных значений")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {str(e)}")
    
    def delete_vm(self):
        """Удаление виртуальной машины"""
        selection = self.vm_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите ВМ для удаления")
            return
        
        item = self.vm_tree.item(selection[0])
        vm_name = item['values'][0]
        vm_type = Formatter.format_vm_type(vm_name)
        
        if messagebox.askyesno("Подтверждение", f"Удалить ВМ {vm_name} ({vm_type})?"):
            try:
                success = self.db.delete_vm(vm_name)
                if success:
                    messagebox.showinfo("Успех", f"ВМ {vm_name} удалена")
                    self.refresh_vm_data()
                    self.refresh_hv_data()
                    self.update_cluster_info()
                    self.update_cluster_status()
                else:
                    messagebox.showerror("Ошибка", f"Не удалось удалить ВМ {vm_name}")
                    
            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось удалить ВМ: {str(e)}")
    
    def refresh_vm_data(self):
        """Обновление данных о ВМ"""
        for item in self.vm_tree.get_children():
            self.vm_tree.delete(item)
        
        vms = self.db.get_all_vms()
        for vm in vms:
            vm_type = Formatter.format_vm_type(vm['vm_name'])
            creation_date = Formatter.format_datetime(vm['creation_date']) if vm.get('creation_date') else ""
            
            self.vm_tree.insert("", tk.END, values=(
                vm['vm_name'],
                vm['vcpu'],
                vm['vram'],
                vm['vhdd'],
                vm['hv_name'],
                creation_date,
                vm_type
            ))

    def generate_vm_name(self):
        """Генерация имени для новой виртуальной машины (автоматическое определение типа)"""
        try:
            # Получаем существующие имена ВМ
            existing_vms = self.db.get_all_vms()
            existing_names = [vm['vm_name'] for vm in existing_vms]
            
            # Определяем тип ВМ из текущего поля ввода
            current_name = self.vm_name_entry.get().strip()
            
            # Автоматически определяем тип ВМ
            vm_type = 'app'  # По умолчанию сервер приложений
            
            if current_name:
                # Пробуем извлечь тип из текущего имени
                import re
                match = re.search(r'(app|db|ts)', current_name.lower())
                if match:
                    vm_type = match.group(1)
            
            # Генерируем следующее имя
            from utils import NameGenerator
            next_name = NameGenerator.get_next_vm_name(vm_type, existing_names)
            
            # Обновляем поле ввода
            self.vm_name_entry.delete(0, tk.END)
            self.vm_name_entry.insert(0, next_name)
            
            # Не меняем автоматически ресурсы - оставляем как есть
            
            messagebox.showinfo("Сгенерировано имя", f"Сгенерировано имя: {next_name}")
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сгенерировать имя: {str(e)}")    
    
    # Методы для работы с гипервизорами
    def add_hypervisor(self):
        """Добавление гипервизора"""
        try:
            hv_name = self.hv_name_entry.get().strip()
            cpu_text = self.hv_cpu_entry.get().strip()
            ram_text = self.hv_ram_entry.get().strip()
            
            if not hv_name or not cpu_text or not ram_text:
                messagebox.showerror("Ошибка", "Заполните все поля")
                return
            
            cpu = int(cpu_text)
            ram = int(ram_text)
            
            # Валидация имени
            is_valid, message = Validator.validate_hv_name(hv_name)
            if not is_valid:
                messagebox.showerror("Ошибка", message)
                return
            
            # Валидация ресурсов (сообщения об ошибках уже содержат новые минимальные значения)
            is_valid, message = Validator.validate_cpu(cpu)
            if not is_valid:
                messagebox.showerror("Ошибка", message)
                return
            
            is_valid, message = Validator.validate_ram(ram)
            if not is_valid:
                messagebox.showerror("Ошибка", message)
                return
            
            # Проверяем ограничение на количество гипервизоров
            config = self.db.get_cluster_config()
            max_hv = int(config.get('max_hypervisors', 24))
            stats = self.db.get_cluster_statistics()
            
            if stats.get('total_hypervisors', 0) >= max_hv:
                messagebox.showerror("Ошибка", f"Достигнуто максимальное количество гипервизоров: {max_hv}")
                return
            
            # Добавление гипервизора
            hv_data = {
                'hv_name': hv_name,
                'cpu': cpu,
                'ram': ram
            }
            
            success = self.db.add_hypervisor(hv_data)
            if success:
                messagebox.showinfo("Успех", f"Гипервизор {hv_name} добавлен в кластер")
                self.refresh_hv_data()
                self.update_cluster_info()
                self.update_cluster_status()
            else:
                messagebox.showerror("Ошибка", "Не удалось добавить гипервизор. Проверьте минимальные требования: CPU ≥ 24 ядер, RAM ≥ 256 ГБ")
                    
        except ValueError as e:
            messagebox.showerror("Ошибка", "Проверьте правильность введенных числовых значений")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Произошла ошибка: {str(e)}")
    
    def delete_hypervisor(self):
        """Удаление гипервизора"""
        selection = self.hv_tree.selection()
        if not selection:
            messagebox.showwarning("Предупреждение", "Выберите гипервизор для удаления")
            return
        
        item = self.hv_tree.item(selection[0])
        hv_name = item['values'][0]
        
        if messagebox.askyesno("Подтверждение", f"Удалить гипервизор {hv_name} из кластера?"):
            try:
                success, message = self.db.delete_hypervisor(hv_name)
                if success:
                    messagebox.showinfo("Успех", f"Гипервизор {hv_name} удален из кластера")
                    self.refresh_hv_data()
                    self.refresh_vm_data()  # Обновляем ВМ на случай если были изменения
                    self.update_cluster_info()
                    self.update_cluster_status()
                else:
                    messagebox.showerror("Ошибка", message)
                
            except Exception as e:
                messagebox.showerror("Ошибка", f"Не удалось удалить гипервизор: {str(e)}")
    
    def refresh_hv_data(self):
        """Обновление данных о гипервизорах"""
        for item in self.hv_tree.get_children():
            self.hv_tree.delete(item)
        
        hvs = self.db.get_all_hypervisors()
        for hv in hvs:
            cpu_usage = ResourceCalculator.calculate_cpu_usage(hv['cpu'], hv['free_cpu'])
            ram_usage = ResourceCalculator.calculate_ram_usage(hv['ram'], hv['free_ram'])
            
            # Определяем статус
            cpu_status = 'Высокая' if cpu_usage > 80 else 'Средняя' if cpu_usage > 50 else 'Низкая'
            ram_status = 'Высокая' if ram_usage > 80 else 'Средняя' if ram_usage > 50 else 'Низкая'
            
            self.hv_tree.insert("", tk.END, values=(
                hv['hv_name'],
                hv['cpu'],
                hv['ram'],
                hv['free_cpu'],
                hv['free_ram'],
                hv['num_vms'],
                f"{cpu_usage:.1f}%",
                f"{ram_usage:.1f}%",
                cpu_status,
                ram_status
            ))
    
    # Методы для анализа
    def generate_plots(self):
        """Генерация графиков"""
        try:
            self.analyzer.generate_visualizations("cluster_analysis.png")
            messagebox.showinfo("Успех", "Графики сгенерированы и сохранены в cluster_analysis.png")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сгенерировать графики: {str(e)}")
    
    def export_to_excel(self):
        """Экспорт данных в Excel"""
        try:
            success = self.analyzer.save_report_to_csv("cluster_report.xlsx")
            if success:
                messagebox.showinfo("Успех", "Отчет сохранен в cluster_report.xlsx")
            else:
                messagebox.showwarning("Предупреждение", "Нет данных для экспорта")
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось экспортировать данные: {str(e)}")
    
    def cluster_report(self):
        """Генерация отчета по кластеру"""
        try:
            report = self.analyzer.generate_cluster_report()
            
            if not report:
                messagebox.showinfo("Отчет", "Нет данных для отчета")
                return
            
            report_text = "=== ОТЧЕТ ПО КЛАСТЕРУ ===\n\n"
            
            # Конфигурация
            report_text += "КОНФИГУРАЦИЯ КЛАСТЕРА:\n"
            config = report.get('config', {})
            for key, value in config.items():
                report_text += f"  {key.replace('_', ' ').title()}: {value}\n"
            
            report_text += "\nСТАТИСТИКА:\n"
            stats = report.get('statistics', {})
            for key, value in stats.items():
                report_text += f"  {key.replace('_', ' ').title()}: {value}\n"
            
            # Анализ гипервизоров
            hv_analysis = report.get('hypervisor_analysis', {})
            if hv_analysis:
                report_text += "\nАНАЛИЗ ГИПЕРВИЗОРОВ:\n"
                for key, value in hv_analysis.items():
                    report_text += f"  {key.replace('_', ' ').title()}: {value}\n"
            
            # Анализ ВМ
            vm_analysis = report.get('vm_analysis', {})
            if vm_analysis:
                report_text += "\nАНАЛИЗ ВИРТУАЛЬНЫХ МАШИН:\n"
                for key, value in vm_analysis.items():
                    if key == 'by_type':
                        report_text += "  Распределение по типам:\n"
                        for vm_type, count in value.items():
                            report_text += f"    {vm_type}: {count}\n"
                    else:
                        report_text += f"  {key.replace('_', ' ').title()}: {value}\n"
            
            # Рекомендации
            recommendations = report.get('recommendations', [])
            if recommendations:
                report_text += "\nРЕКОМЕНДАЦИИ:\n"
                for i, rec in enumerate(recommendations, 1):
                    report_text += f"  {i}. {rec}\n"
            
            self.analysis_text.delete(1.0, tk.END)
            self.analysis_text.insert(1.0, report_text)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось сгенерировать отчет: {str(e)}")
    
    def show_statistics(self):
        """Показать статистику"""
        try:
            hv_df, vm_df = self.analyzer.get_resource_usage_report()
            
            stats_text = "=== СТАТИСТИКА КЛАСТЕРА ===\n\n"
            
            # Статистика по гипервизорам
            if not hv_df.empty:
                stats_text += "ГИПЕРВИЗОРЫ:\n"
                stats_text += f"Всего гипервизоров: {len(hv_df)}\n"
                
                total_cpu = hv_df['cpu'].sum()
                total_ram = hv_df['ram'].sum()
                free_cpu = hv_df['free_cpu'].sum()
                free_ram = hv_df['free_ram'].sum()
                total_vms = hv_df['num_vms'].sum()
                
                cpu_usage = ResourceCalculator.calculate_cpu_usage(total_cpu, free_cpu)
                ram_usage = ResourceCalculator.calculate_ram_usage(total_ram, free_ram)
                
                stats_text += f"Общие ресурсы: CPU {total_cpu} ядер, RAM {total_ram} ГБ\n"
                stats_text += f"Свободные ресурсы: CPU {free_cpu} ядер, RAM {free_ram} ГБ\n"
                stats_text += f"Использование: CPU {cpu_usage:.1f}%, RAM {ram_usage:.1f}%\n"
                stats_text += f"Всего ВМ на гипервизорах: {total_vms}\n\n"
                
                # Самый загруженный гипервизор
                if 'cpu_usage_percent' in hv_df.columns:
                    max_cpu_idx = hv_df['cpu_usage_percent'].idxmax()
                    most_loaded = hv_df.loc[max_cpu_idx]
                    stats_text += f"Самый загруженный гипервизор: {most_loaded['hv_name']} "
                    stats_text += f"(CPU: {most_loaded['cpu_usage_percent']:.1f}%)\n\n"
            
            # Статистика по ВМ
            if not vm_df.empty:
                stats_text += "ВИРТУАЛЬНЫЕ МАШИНЫ:\n"
                stats_text += f"Всего ВМ: {len(vm_df)}\n"
                stats_text += f"Общее количество vCPU: {vm_df['vcpu'].sum()}\n"
                stats_text += f"Общее количество vRAM: {vm_df['vram'].sum()} ГБ\n"
                stats_text += f"Общее количество vHDD: {vm_df['vhdd'].sum()} ГБ\n"
                stats_text += f"Средний размер vCPU на ВМ: {vm_df['vcpu'].mean():.1f}\n"
                stats_text += f"Средний размер vRAM на ВМ: {vm_df['vram'].mean():.1f} ГБ\n"
                stats_text += f"Средний размер vHDD на ВМ: {vm_df['vhdd'].mean():.1f} ГБ\n\n"
                
                # Распределение по типам
                if 'vm_type' in vm_df.columns:
                    type_counts = vm_df['vm_type'].value_counts()
                    stats_text += "Распределение по типам:\n"
                    for vm_type, count in type_counts.items():
                        stats_text += f"  {vm_type}: {count} ВМ\n"
            
            self.analysis_text.delete(1.0, tk.END)
            self.analysis_text.insert(1.0, stats_text)
            
        except Exception as e:
            messagebox.showerror("Ошибка", f"Не удалось получить статистику: {str(e)}")
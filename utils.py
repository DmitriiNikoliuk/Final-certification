import re
from datetime import datetime
from typing import Tuple, List, Dict, Any
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Validator:
    """Класс для валидации данных"""
    
    @staticmethod
    def validate_vm_name(vm_name: str) -> Tuple[bool, str]:
        """Валидация имени виртуальной машины"""
        pattern = r'^vm77(app|db|ts)\d{2}$'
        if not re.match(pattern, vm_name):
            return False, "Имя ВМ должно соответствовать формату: vm77[app|db|ts]XX"
        return True, ""
    
    @staticmethod
    def validate_hv_name(hv_name: str) -> Tuple[bool, str]:
        """Валидация имени гипервизора"""
        pattern = r'^s77hv\d{2}$'
        if not re.match(pattern, hv_name):
            return False, "Имя гипервизора должно соответствовать формату: s77hvXX"
        return True, ""
    
    @staticmethod
    def validate_cpu(cpu: int) -> Tuple[bool, str]:
        """Валидация количества CPU"""
        if cpu < 24: 
            return False, "CPU должно быть не менее 24 ядер"
        if cpu > 256:
            return False, "CPU не может быть больше 256 ядер"
        return True, ""
    
    @staticmethod
    def validate_ram(ram: int) -> Tuple[bool, str]:
        """Валидация количества RAM"""
        if ram < 256: 
            return False, "RAM должно быть не менее 256 ГБ"
        if ram > 2048:
            return False, "RAM не может быть больше 2048 ГБ"
        return True, ""
    
    @staticmethod
    def validate_vm_resources(vcpu: int, vram: int, vhdd: int) -> Tuple[bool, str]:
        """Валидация ресурсов ВМ"""
        if not (2 <= vcpu <= 24):
            return False, "vCPU должно быть от 2 до 24"
        if vcpu % 2 != 0:
            return False, "vCPU должно быть кратно 2"
        if not (4 <= vram <= 128):
            return False, "vRAM должно быть от 4 до 128 ГБ"
        if not (40 <= vhdd <= 4096):
            return False, "vHDD должно быть от 40 до 4096 ГБ"
        return True, ""


class NameGenerator:
    """Класс для генерации имен"""
    
    @staticmethod
    def generate_vm_name(base_name: str, existing_names: List[str]) -> str:
        """Генерация уникального имени ВМ"""
        pattern = r'^vm77(app|db|ts)(\d{2})$'
        match = re.match(pattern, base_name)
        
        if not match:
            # Если базовое имя не соответствует формату, пробуем исправить
            if 'app' in base_name:
                prefix = 'app'
            elif 'db' in base_name:
                prefix = 'db'
            elif 'ts' in base_name:
                prefix = 'ts'
            else:
                prefix = 'app'
            
            number = 1
        else:
            prefix = match.group(1)
            number = int(match.group(2))
        
        while True:
            new_name = f"vm77{prefix}{number:02d}"
            if new_name not in existing_names:
                return new_name
            number += 1
    
    @staticmethod
    def get_next_hv_name(existing_names: List[str]) -> str:
        """Генерация следующего имени гипервизора"""
        pattern = r'^s77hv(\d{2})$'
        numbers = []
        
        for name in existing_names:
            match = re.match(pattern, name)
            if match:
                numbers.append(int(match.group(1)))
        
        next_number = max(numbers) + 1 if numbers else 1
        return f"s77hv{next_number:02d}"
    
    @staticmethod
    def get_next_vm_name(vm_type: str, existing_names: List[str]) -> str:
        """Генерация следующего имени ВМ для указанного типа"""
        pattern_map = {
            'app': r'^vm77app(\d{2})$',
            'db': r'^vm77db(\d{2})$',
            'ts': r'^vm77ts(\d{2})$'
        }
        
        if vm_type not in pattern_map:
            vm_type = 'app'  # По умолчанию сервер приложений
        
        pattern = pattern_map[vm_type]
        numbers = []
        
        for name in existing_names:
            match = re.match(pattern, name)
            if match:
                numbers.append(int(match.group(1)))
            else:
                # Пробуем определить тип из имени
                for vtype in ['app', 'db', 'ts']:
                    if vtype in name.lower():
                        type_pattern = pattern_map[vtype]
                        match = re.match(type_pattern, name)
                        if match:
                            numbers.append(int(match.group(1)))
        
        next_number = max(numbers) + 1 if numbers else 1
        return f"vm77{vm_type}{next_number:02d}"

class ResourceCalculator:
    """Класс для расчета ресурсов"""
    
    @staticmethod
    def calculate_cpu_usage(total_cpu: int, free_cpu: int) -> float:
        """Расчет использования CPU в процентах"""
        if total_cpu == 0:
            return 0.0
        return ((total_cpu - free_cpu) / total_cpu) * 100
    
    @staticmethod
    def calculate_ram_usage(total_ram: int, free_ram: int) -> float:
        """Расчет использования RAM в процентах"""
        if total_ram == 0:
            return 0.0
        return ((total_ram - free_ram) / total_ram) * 100
    
    @staticmethod
    def calculate_required_physical_cpu(vcpus: int, overcommit: float = 3.0) -> float:
        """Расчет требуемых физических CPU с учетом переподписки"""
        return vcpus / overcommit
    
    @staticmethod
    def check_minimum_resources(total_cpu: int, free_cpu: int, 
                                total_ram: int, free_ram: int) -> Tuple[bool, str]:
        """Проверка минимальных свободных ресурсов (10%)"""
        cpu_percent = ResourceCalculator.calculate_cpu_usage(total_cpu, free_cpu)
        ram_percent = ResourceCalculator.calculate_ram_usage(total_ram, free_ram)
        
        warnings = []
        if cpu_percent > 90:
            warnings.append(f"Использование CPU: {cpu_percent:.1f}% (близко к пределу)")
        if ram_percent > 90:
            warnings.append(f"Использование RAM: {ram_percent:.1f}% (близко к пределу)")
        
        if warnings:
            return False, "; ".join(warnings)
        return True, "Ресурсы в норме"


class Formatter:
    """Класс для форматирования данных"""
    
    @staticmethod
    def format_datetime(dt: datetime) -> str:
        """Форматирование даты и времени"""
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    
    @staticmethod
    def format_bytes(size_bytes: int) -> str:
        """Форматирование размера в байтах в читаемый вид"""
        for unit in ['Б', 'КБ', 'МБ', 'ГБ', 'ТБ']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.1f} ПБ"
    
    @staticmethod
    def format_vm_type(vm_name: str) -> str:
        """Определение типа ВМ по имени"""
        if 'app' in vm_name:
            return "Сервер приложений"
        elif 'db' in vm_name:
            return "Сервер БД"
        elif 'ts' in vm_name:
            return "Терминальный сервер"
        return "Неизвестный"
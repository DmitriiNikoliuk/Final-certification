import asyncio
import logging
from typing import List, Dict, Any
from database import Database
from utils import NameGenerator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class AsyncOperations:
    """Класс для асинхронных операций"""
    
    def __init__(self, db: Database):
        self.db = db
    
    async def create_vm_async(self, vm_data: Dict[str, Any]) -> bool:
        """Асинхронное создание ВМ"""
        try:
            # Имитация долгой операции для демонстрации асинхронности
            await asyncio.sleep(0.5)
            
            # Используем синхронный метод с run_in_executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None, self.db.create_vm, vm_data
            )
            
            if result:
                logger.info(f"Асинхронно создана ВМ: {vm_data['vm_name']}")
            else:
                logger.warning(f"Не удалось асинхронно создать ВМ: {vm_data['vm_name']}")
            
            return result
            
        except Exception as e:
            logger.error(f"Ошибка при асинхронном создании ВМ: {e}")
            return False
    
    async def mass_deploy_vms(self, base_vm_data: Dict[str, Any], count: int) -> List[bool]:
        """Массовое развертывание ВМ"""
        try:
            # Получаем существующие имена
            existing_vms = self.db.get_all_vms()
            existing_names = [vm['vm_name'] for vm in existing_vms]
            
            tasks = []
            vms_to_create = []
            
            # Подготавливаем данные для всех ВМ
            for i in range(count):
                vm_data = base_vm_data.copy()
                
                # Генерируем уникальное имя
                vm_data['vm_name'] = NameGenerator.generate_vm_name(
                    base_vm_data['vm_name'], 
                    existing_names + [vm['vm_name'] for vm in vms_to_create]
                )
                
                vms_to_create.append(vm_data)
                existing_names.append(vm_data['vm_name'])
                
                # Создаем задачу для каждой ВМ
                task = self.create_vm_async(vm_data)
                tasks.append(task)
            
            # Запускаем все задачи параллельно
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Обрабатываем исключения
                processed_results = []
                for result in results:
                    if isinstance(result, Exception):
                        logger.error(f"Исключение при создании ВМ: {result}")
                        processed_results.append(False)
                    else:
                        processed_results.append(result)
                
                success_count = sum(1 for r in processed_results if r is True)
                logger.info(f"Массовое развертывание завершено. Успешно: {success_count}/{count}")
                
                return processed_results
            
            return []
            
        except Exception as e:
            logger.error(f"Ошибка при массовом развертывании ВМ: {e}")
            return []
    
    async def check_resources_async(self) -> Dict[str, Any]:
        """Асинхронная проверка ресурсов кластера"""
        try:
            # Имитация долгой операции
            await asyncio.sleep(0.3)
            
            loop = asyncio.get_event_loop()
            stats = await loop.run_in_executor(
                None, self.db.get_cluster_statistics
            )
            
            logger.info("Асинхронная проверка ресурсов завершена")
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при асинхронной проверке ресурсов: {e}")
            return {}
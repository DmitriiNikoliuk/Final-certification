import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Tuple
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Database:
    def __init__(self, dbname="datacenter_db2", user="postgres", 
                 password="pass", host="localhost", port="5432"):
        self.connection_params = {
            "dbname": dbname,
            "user": user,
            "password": password,
            "host": host,
            "port": port
        }
        self._create_tables()
        self._initialize_cluster()
    
    def _get_connection(self):
        return psycopg2.connect(**self.connection_params)
    
    def _create_tables(self):
        """Создание таблиц в базе данных"""
        queries = [
            """
            CREATE TABLE IF NOT EXISTS hypervisors (
                hv_name VARCHAR(50) PRIMARY KEY,
                cpu INTEGER NOT NULL CHECK (cpu > 0),
                ram INTEGER NOT NULL CHECK (ram > 0),
                free_cpu INTEGER NOT NULL CHECK (free_cpu >= 0 AND free_cpu <= cpu),
                free_ram INTEGER NOT NULL CHECK (free_ram >= 0 AND free_ram <= ram),
                num_vms INTEGER DEFAULT 0 CHECK (num_vms >= 0),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS virtual_machines (
                vm_name VARCHAR(50) PRIMARY KEY,
                vcpu INTEGER NOT NULL CHECK (vcpu BETWEEN 2 AND 24 AND vcpu % 2 = 0),
                vram INTEGER NOT NULL CHECK (vram BETWEEN 4 AND 128),
                vhdd INTEGER NOT NULL CHECK (vhdd BETWEEN 40 AND 4096),
                hv_name VARCHAR(50) NOT NULL,
                creation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (hv_name) REFERENCES hypervisors(hv_name) ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS cluster_config (
                config_key VARCHAR(50) PRIMARY KEY,
                config_value VARCHAR(200),
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        ]
        
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            for query in queries:
                cur.execute(query)
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Таблицы успешно созданы или уже существуют")
        except Exception as e:
            logger.error(f"Ошибка при создании таблиц: {e}")
    
    def _initialize_cluster(self):
        """Инициализация конфигурации кластера"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            configs = [
                ("cluster_name", "Moscow_Cluster"),
                ("disk_pool", "1000000"),
                ("overcommit_cpu", "3.0"),
                ("overcommit_ram", "1.0"),
                ("max_hypervisors", "24")
            ]
            
            for key, value in configs:
                cur.execute("""
                    INSERT INTO cluster_config (config_key, config_value) 
                    VALUES (%s, %s)
                    ON CONFLICT (config_key) DO NOTHING
                """, (key, value))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info("Конфигурация кластера инициализирована")
            
        except Exception as e:
            logger.error(f"Ошибка при инициализации кластера: {e}")
    
    # Методы для работы с виртуальными машинами
    def create_vm(self, vm_data: Dict[str, Any]) -> bool:
        """Создание виртуальной машины"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Находим подходящий гипервизор
            cur.execute("""
                SELECT hv_name, free_cpu, free_ram 
                FROM hypervisors 
                WHERE free_cpu >= %s AND free_ram >= %s
                ORDER BY num_vms ASC, free_cpu DESC
                LIMIT 1
            """, (vm_data['vcpu'], vm_data['vram']))
            
            result = cur.fetchone()
            if not result:
                logger.error("Нет доступных гипервизоров с достаточными ресурсами")
                cur.close()
                conn.close()
                return False
            
            hv_name = result[0]
            
            # Создаем ВМ
            cur.execute("""
                INSERT INTO virtual_machines (vm_name, vcpu, vram, vhdd, hv_name)
                VALUES (%s, %s, %s, %s, %s)
            """, (vm_data['vm_name'], vm_data['vcpu'], vm_data['vram'], 
                  vm_data['vhdd'], hv_name))
            
            # Обновляем ресурсы гипервизора
            cur.execute("""
                UPDATE hypervisors 
                SET free_cpu = free_cpu - %s, 
                    free_ram = free_ram - %s,
                    num_vms = num_vms + 1
                WHERE hv_name = %s
            """, (vm_data['vcpu'], vm_data['vram'], hv_name))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"ВМ {vm_data['vm_name']} успешно создана на гипервизоре {hv_name}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при создании ВМ: {e}")
            return False
    
    def get_all_vms(self) -> List[Dict[str, Any]]:
        """Получение всех виртуальных машин"""
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT vm_name, vcpu, vram, vhdd, hv_name, creation_date 
                FROM virtual_machines 
                ORDER BY vm_name
            """)
            vms = cur.fetchall()
            cur.close()
            conn.close()
            return vms
        except Exception as e:
            logger.error(f"Ошибка при получении ВМ: {e}")
            return []
    
    def delete_vm(self, vm_name: str) -> bool:
        """Удаление виртуальной машины"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Получаем данные о ВМ
            cur.execute("SELECT hv_name, vcpu, vram FROM virtual_machines WHERE vm_name = %s", (vm_name,))
            result = cur.fetchone()
            
            if not result:
                cur.close()
                conn.close()
                return False
            
            hv_name, vcpu, vram = result
            
            # Удаляем ВМ
            cur.execute("DELETE FROM virtual_machines WHERE vm_name = %s", (vm_name,))
            
            # Освобождаем ресурсы на гипервизоре
            cur.execute("""
                UPDATE hypervisors 
                SET free_cpu = free_cpu + %s, 
                    free_ram = free_ram + %s,
                    num_vms = num_vms - 1
                WHERE hv_name = %s
            """, (vcpu, vram, hv_name))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"ВМ {vm_name} успешно удалена")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при удалении ВМ: {e}")
            return False
    
    # Методы для работы с гипервизорами
    def add_hypervisor(self, hv_data: Dict[str, Any]) -> bool:
        """Добавление гипервизора"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Проверяем ограничение на количество гипервизоров
            cur.execute("SELECT COUNT(*) FROM hypervisors")
            hv_count = cur.fetchone()[0]
            
            cur.execute("SELECT config_value FROM cluster_config WHERE config_key = 'max_hypervisors'")
            max_hv = cur.fetchone()
            max_hypervisors = int(max_hv[0]) if max_hv else 24
            
            if hv_count >= max_hypervisors:
                logger.error(f"Достигнуто максимальное количество гипервизоров: {max_hypervisors}")
                cur.close()
                conn.close()
                return False
            
            # Проверяем минимальные требования к гипервизору (ДОБАВЛЕНО)
            if hv_data['cpu'] < 24:  # Минимум 24 ядра CPU
                logger.error(f"CPU гипервизора должно быть не менее 24 ядер")
                cur.close()
                conn.close()
                return False
            
            if hv_data['ram'] < 256:  # Минимум 256 ГБ RAM
                logger.error(f"RAM гипервизора должно быть не менее 256 ГБ")
                cur.close()
                conn.close()
                return False
            
            # Проверяем, существует ли уже гипервизор с таким именем
            cur.execute("SELECT 1 FROM hypervisors WHERE hv_name = %s", (hv_data['hv_name'],))
            if cur.fetchone():
                logger.error(f"Гипервизор с именем {hv_data['hv_name']} уже существует")
                cur.close()
                conn.close()
                return False
            
            cur.execute("""
                INSERT INTO hypervisors 
                (hv_name, cpu, ram, free_cpu, free_ram, num_vms)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (hv_data['hv_name'], hv_data['cpu'], hv_data['ram'],
                hv_data['cpu'], hv_data['ram'], 0))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Гипервизор {hv_data['hv_name']} успешно добавлен")
            return True
        
        except Exception as e:
            logger.error(f"Ошибка при добавлении гипервизора: {e}")
            return False
    
    def get_all_hypervisors(self) -> List[Dict[str, Any]]:
        """Получение всех гипервизоров"""
        try:
            conn = self._get_connection()
            cur = conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT hv_name, cpu, ram, free_cpu, free_ram, num_vms, created_at 
                FROM hypervisors 
                ORDER BY hv_name
            """)
            hvs = cur.fetchall()
            cur.close()
            conn.close()
            return hvs
        except Exception as e:
            logger.error(f"Ошибка при получении гипервизоров: {e}")
            return []
    
    def delete_hypervisor(self, hv_name: str) -> Tuple[bool, str]:
        """Удаление гипервизора"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Проверяем, есть ли ВМ на гипервизоре
            cur.execute("SELECT COUNT(*) FROM virtual_machines WHERE hv_name = %s", (hv_name,))
            vm_count = cur.fetchone()[0]
            
            if vm_count > 0:
                cur.close()
                conn.close()
                return False, f"На гипервизоре {hv_name} запущено {vm_count} ВМ"
            
            # Удаляем гипервизор
            cur.execute("DELETE FROM hypervisors WHERE hv_name = %s", (hv_name,))
            
            conn.commit()
            cur.close()
            conn.close()
            logger.info(f"Гипервизор {hv_name} успешно удален")
            return True, ""
            
        except Exception as e:
            logger.error(f"Ошибка при удалении гипервизора: {e}")
            return False, str(e)
    
    def get_cluster_config(self) -> Dict[str, str]:
        """Получение конфигурации кластера"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            cur.execute("SELECT config_key, config_value FROM cluster_config")
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            config = {}
            for key, value in rows:
                config[key] = value
            return config
            
        except Exception as e:
            logger.error(f"Ошибка при получении конфигурации кластера: {e}")
            return {}
    
    def get_cluster_statistics(self) -> Dict[str, Any]:
        """Получение статистики кластера"""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Общая статистика
            cur.execute("""
                SELECT 
                    COUNT(*) as total_hypervisors,
                    SUM(cpu) as total_cpu,
                    SUM(ram) as total_ram,
                    SUM(free_cpu) as free_cpu,
                    SUM(free_ram) as free_ram,
                    SUM(num_vms) as total_vms
                FROM hypervisors
            """)
            
            stats_result = cur.fetchone()
            
            # Статистика по ВМ
            cur.execute("""
                SELECT 
                    COUNT(*) as vm_count,
                    SUM(vcpu) as total_vcpu,
                    SUM(vram) as total_vram,
                    SUM(vhdd) as total_vhdd
                FROM virtual_machines
            """)
            
            vm_stats = cur.fetchone()
            
            cur.close()
            conn.close()
            
            stats = {}
            if stats_result:
                stats['total_hypervisors'] = stats_result[0] or 0
                stats['total_cpu'] = stats_result[1] or 0
                stats['total_ram'] = stats_result[2] or 0
                stats['free_cpu'] = stats_result[3] or 0
                stats['free_ram'] = stats_result[4] or 0
                stats['total_vms'] = stats_result[5] or 0
            
            if vm_stats:
                stats['vm_count'] = vm_stats[0] or 0
                stats['total_vcpu'] = vm_stats[1] or 0
                stats['total_vram'] = vm_stats[2] or 0
                stats['total_vhdd'] = vm_stats[3] or 0
            
            return stats
            
        except Exception as e:
            logger.error(f"Ошибка при получении статистики кластера: {e}")
            return {}
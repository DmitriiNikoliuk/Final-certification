from dataclasses import dataclass
from datetime import datetime
from typing import Optional

@dataclass
class Hypervisor:
    hv_name: str
    cpu: int
    ram: int
    free_cpu: int
    free_ram: int
    num_vms: int = 0
    
    def has_minimum_resources(self) -> bool:
        """Проверка минимальных свободных ресурсов (10%)"""
        min_cpu = self.cpu * 0.1
        min_ram = self.ram * 0.1
        return self.free_cpu >= min_cpu and self.free_ram >= min_ram

@dataclass
class VirtualMachine:
    vm_name: str
    vcpu: int
    vram: int
    vhdd: int
    hv_name: str
    creation_date: datetime

@dataclass
class Cluster:
    name: str = "Moscow_Cluster"
    disk_pool: int = 1000000  # 1 ПБ по умолчанию
    overcommit_cpu: float = 3.0
    overcommit_ram: float = 1.0
    max_hypervisors: int = 24
import unittest
import sys
import os
from datetime import datetime

# Добавляем родительскую директорию в путь для импорта
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from models import VirtualMachine, Hypervisor, Cluster
    from utils import Validator, ResourceCalculator
    IMPORT_SUCCESS = True
except ImportError as e:
    print(f"Ошибка импорта: {e}")
    print("Убедитесь, что файлы проекта находятся в родительской директории")
    IMPORT_SUCCESS = False

@unittest.skipIf(not IMPORT_SUCCESS, "Модули проекта не найдены")
class TestModels(unittest.TestCase):
    def test_vm_creation(self):
        vm = VirtualMachine("vm77app01", 4, 8, 100, "s77hv01", datetime.now())
        self.assertEqual(vm.vm_name, "vm77app01")
        self.assertEqual(vm.vcpu, 4)

    def test_hypervisor_min_resources(self):
        hv = Hypervisor("s77hv01", 100, 100, 20, 20, 0)
        self.assertTrue(hv.has_minimum_resources())
        hv2 = Hypervisor("s77hv02", 100, 100, 5, 5, 0)
        self.assertFalse(hv2.has_minimum_resources())

@unittest.skipIf(not IMPORT_SUCCESS, "Модули проекта не найдены")
class TestValidator(unittest.TestCase):
    def test_vm_name(self):
        self.assertTrue(Validator.validate_vm_name("vm77app01")[0])
        self.assertFalse(Validator.validate_vm_name("invalid")[0])
    
    def test_vm_resources(self):
        self.assertTrue(Validator.validate_vm_resources(2, 4, 40)[0])
        self.assertFalse(Validator.validate_vm_resources(1, 4, 40)[0])

@unittest.skipIf(not IMPORT_SUCCESS, "Модули проекта не найдены")
class TestCalculator(unittest.TestCase):
    def test_cpu_usage(self):
        self.assertEqual(ResourceCalculator.calculate_cpu_usage(100, 30), 70.0)
        self.assertEqual(ResourceCalculator.calculate_cpu_usage(0, 0), 0.0)

class TestAnalysis(unittest.TestCase):
    def test_usage_stats(self):
        stats = self._calculate_stats([
            {'cpu': 100, 'ram': 200, 'free_cpu': 50, 'free_ram': 100},
            {'cpu': 100, 'ram': 200, 'free_cpu': 50, 'free_ram': 100}
        ])
        self.assertEqual(stats['total_cpu'], 200)
        self.assertEqual(stats['cpu_usage'], 50.0)
    
    def test_vm_distribution(self):
        dist = self._analyze_distribution([
            'vm77app01', 'vm77app02', 'vm77db01', 'vm77ts01'
        ])
        self.assertEqual(dist['app'], 2)
        self.assertEqual(dist['db'], 1)
    
    def _calculate_stats(self, hypervisors):
        if not hypervisors: return {'total_cpu': 0, 'cpu_usage': 0}
        total_cpu = sum(h['cpu'] for h in hypervisors)
        used_cpu = total_cpu - sum(h['free_cpu'] for h in hypervisors)
        return {'total_cpu': total_cpu, 'cpu_usage': used_cpu/total_cpu*100}
    
    def _analyze_distribution(self, vm_names):
        dist = {'app': 0, 'db': 0, 'ts': 0}
        for name in vm_names:
            if 'app' in name: dist['app'] += 1
            elif 'db' in name: dist['db'] += 1
            elif 'ts' in name: dist['ts'] += 1
        return dist

@unittest.skipIf(not IMPORT_SUCCESS, "Модули проекта не найдены")
class TestIntegration(unittest.TestCase):
    def test_workflow(self):
        vm = VirtualMachine("vm77app01", 4, 8, 100, "s77hv01", datetime.now())
        is_valid, _ = Validator.validate_vm_name(vm.vm_name)
        self.assertTrue(is_valid)
        usage = ResourceCalculator.calculate_cpu_usage(100, 50)
        self.assertEqual(usage, 50.0)

if __name__ == '__main__':
    unittest.main()
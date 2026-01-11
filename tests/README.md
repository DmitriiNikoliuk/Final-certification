## Запуск:
```
python test.py -v
```
## Краткое описание тестов:
### TestModels:

- test_vm_creation - проверка создания объекта виртуальной машины

- test_hypervisor_min_resources - проверка метода определения минимальных ресурсов гипервизора

### TestValidator:

- test_vm_name - валидация имен виртуальных машин по формату vm77[app|db|ts]XX

- test_vm_resources - проверка валидации ресурсов ВМ (vCPU, vRAM, vHDD)

### TestCalculator:

- test_cpu_usage - проверка расчета использования CPU в процентах

### TestAnalysis:

- test_usage_stats - проверка расчета статистики использования ресурсов

- test_vm_distribution - анализ распределения ВМ по типам

### TestIntegration:

- test_workflow - интеграционный тест рабочего процесса (создание ВМ → валидация → расчеты)
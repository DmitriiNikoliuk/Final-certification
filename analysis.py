import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List, Dict, Tuple, Any
import logging
from utils import ResourceCalculator, Formatter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataAnalyzer:
    """Класс для анализа данных кластера"""
    
    def __init__(self, db):
        self.db = db
    
    def get_resource_usage_report(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Отчет об использовании ресурсов в кластере"""
        try:
            # Получаем данные
            hypervisors = self.db.get_all_hypervisors()
            vms = self.db.get_all_vms()
            
            # Создаем DataFrame
            hv_df = pd.DataFrame(hypervisors)
            vm_df = pd.DataFrame(vms)
            
            # Анализ использования ресурсов
            if not hv_df.empty:
                # Добавляем расчет использования в процентах
                hv_df['cpu_usage_percent'] = hv_df.apply(
                    lambda row: ResourceCalculator.calculate_cpu_usage(row['cpu'], row['free_cpu']), 
                    axis=1
                )
                hv_df['ram_usage_percent'] = hv_df.apply(
                    lambda row: ResourceCalculator.calculate_ram_usage(row['ram'], row['free_ram']), 
                    axis=1
                )
                
                # Добавляем статус загрузки
                hv_df['cpu_status'] = hv_df['cpu_usage_percent'].apply(
                    lambda x: 'Высокая' if x > 80 else 'Средняя' if x > 50 else 'Низкая'
                )
                hv_df['ram_status'] = hv_df['ram_usage_percent'].apply(
                    lambda x: 'Высокая' if x > 80 else 'Средняя' if x > 50 else 'Низкая'
                )
            
            if not vm_df.empty:
                # Добавляем тип ВМ
                vm_df['vm_type'] = vm_df['vm_name'].apply(Formatter.format_vm_type)
                
                # Форматируем дату
                vm_df['creation_date_str'] = vm_df['creation_date'].apply(
                    lambda x: Formatter.format_datetime(x) if pd.notnull(x) else ''
                )
            
            return hv_df, vm_df
            
        except Exception as e:
            logger.error(f"Ошибка при получении отчета об использовании ресурсов: {e}")
            return pd.DataFrame(), pd.DataFrame()
    
    def generate_visualizations(self, save_path: str = None):
        """Генерация визуализаций для кластера"""
        try:
            hv_df, vm_df = self.get_resource_usage_report()
            
            if hv_df.empty:
                logger.warning("Нет данных для визуализации")
                return
            
            # Создаем фигуру с несколькими графиками
            fig, axes = plt.subplots(2, 2, figsize=(16, 12))
            fig.suptitle('Анализ использования ресурсов кластера Москва', 
                        fontsize=18, fontweight='bold', y=0.98)
            
            # 1. Использование CPU по гипервизорам
            if not hv_df.empty and 'cpu_usage_percent' in hv_df.columns:
                colors = ['red' if x > 80 else 'orange' if x > 50 else 'green' 
                         for x in hv_df['cpu_usage_percent']]
                
                bars = axes[0, 0].bar(hv_df['hv_name'], hv_df['cpu_usage_percent'], 
                                     color=colors, edgecolor='black', linewidth=1)
                axes[0, 0].set_title('Использование CPU по гипервизорам', 
                                    fontsize=14, fontweight='bold', pad=15)
                axes[0, 0].set_ylabel('Использование CPU (%)', fontsize=12)
                axes[0, 0].set_xlabel('Гипервизоры', fontsize=12)
                axes[0, 0].tick_params(axis='x', rotation=45, labelsize=10)
                axes[0, 0].tick_params(axis='y', labelsize=10)
                axes[0, 0].grid(axis='y', alpha=0.3, linestyle='--')
                
                # Линия порога
                axes[0, 0].axhline(y=80, color='darkred', linestyle='--', 
                                  alpha=0.7, linewidth=2, label='Критический порог (80%)')
                axes[0, 0].axhline(y=50, color='darkorange', linestyle='--', 
                                  alpha=0.7, linewidth=2, label='Средний порог (50%)')
                axes[0, 0].legend(fontsize=9)
                
                # Добавляем значения на столбцы
                for bar, value in zip(bars, hv_df['cpu_usage_percent']):
                    height = bar.get_height()
                    axes[0, 0].text(bar.get_x() + bar.get_width()/2, height + 1, 
                                   f'{value:.1f}%', ha='center', va='bottom', 
                                   fontsize=9, fontweight='bold')
            
            # 2. Использование RAM по гипервизорам
            if not hv_df.empty and 'ram_usage_percent' in hv_df.columns:
                colors = ['red' if x > 80 else 'orange' if x > 50 else 'blue' 
                         for x in hv_df['ram_usage_percent']]
                
                bars = axes[0, 1].bar(hv_df['hv_name'], hv_df['ram_usage_percent'], 
                                     color=colors, edgecolor='black', linewidth=1)
                axes[0, 1].set_title('Использование RAM по гипервизорам', 
                                    fontsize=14, fontweight='bold', pad=15)
                axes[0, 1].set_ylabel('Использование RAM (%)', fontsize=12)
                axes[0, 1].set_xlabel('Гипервизоры', fontsize=12)
                axes[0, 1].tick_params(axis='x', rotation=45, labelsize=10)
                axes[0, 1].tick_params(axis='y', labelsize=10)
                axes[0, 1].grid(axis='y', alpha=0.3, linestyle='--')
                
                # Линия порога
                axes[0, 1].axhline(y=80, color='darkred', linestyle='--', 
                                  alpha=0.7, linewidth=2, label='Критический порог (80%)')
                axes[0, 1].axhline(y=50, color='darkorange', linestyle='--', 
                                  alpha=0.7, linewidth=2, label='Средний порог (50%)')
                axes[0, 1].legend(fontsize=9)
                
                # Добавляем значения на столбцы
                for bar, value in zip(bars, hv_df['ram_usage_percent']):
                    height = bar.get_height()
                    axes[0, 1].text(bar.get_x() + bar.get_width()/2, height + 1, 
                                   f'{value:.1f}%', ha='center', va='bottom', 
                                   fontsize=9, fontweight='bold')
            
            # 3. Количество ВМ на гипервизорах
            if not hv_df.empty and 'num_vms' in hv_df.columns:
                colors = plt.cm.summer(hv_df['num_vms'] / max(hv_df['num_vms'].max(), 1))
                
                bars = axes[1, 0].bar(hv_df['hv_name'], hv_df['num_vms'], 
                                     color=colors, edgecolor='black', linewidth=1)
                axes[1, 0].set_title('Количество ВМ на гипервизорах', 
                                    fontsize=14, fontweight='bold', pad=15)
                axes[1, 0].set_ylabel('Количество ВМ', fontsize=12)
                axes[1, 0].set_xlabel('Гипервизоры', fontsize=12)
                axes[1, 0].tick_params(axis='x', rotation=45, labelsize=10)
                axes[1, 0].tick_params(axis='y', labelsize=10)
                axes[1, 0].grid(axis='y', alpha=0.3, linestyle='--')
                
                # Добавляем значения на столбцы
                for bar, value in zip(bars, hv_df['num_vms']):
                    height = bar.get_height()
                    axes[1, 0].text(bar.get_x() + bar.get_width()/2, height + 0.1, 
                                   f'{value}', ha='center', va='bottom', 
                                   fontsize=9, fontweight='bold')
            
            # 4. Распределение ВМ по типам (если есть данные)
            if not vm_df.empty and 'vm_type' in vm_df.columns:
                vm_type_counts = vm_df['vm_type'].value_counts()
                
                if not vm_type_counts.empty:
                    colors = plt.cm.Set3(range(len(vm_type_counts)))
                    wedges, texts, autotexts = axes[1, 1].pie(
                        vm_type_counts.values, 
                        labels=vm_type_counts.index, 
                        autopct='%1.1f%%',
                        startangle=90,
                        colors=colors,
                        textprops={'fontsize': 10}
                    )
                    axes[1, 1].set_title('Распределение ВМ по типам', 
                                        fontsize=14, fontweight='bold', pad=15)
                    
                    # Делаем подписи более читаемыми
                    for autotext in autotexts:
                        autotext.set_color('white')
                        autotext.set_fontweight('bold')
                else:
                    axes[1, 1].text(0.5, 0.5, 'Нет данных о ВМ', 
                                   ha='center', va='center', fontsize=12)
                    axes[1, 1].set_title('Распределение ВМ по типам', 
                                        fontsize=14, fontweight='bold', pad=15)
            else:
                axes[1, 1].text(0.5, 0.5, 'Нет данных о ВМ', 
                               ha='center', va='center', fontsize=12)
                axes[1, 1].set_title('Распределение ВМ по типам', 
                                    fontsize=14, fontweight='bold', pad=15)
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"Графики сохранены в {save_path}")
            
            plt.show()
            
        except Exception as e:
            logger.error(f"Ошибка при генерации визуализаций: {e}")
    
    def generate_cluster_report(self) -> Dict[str, Any]:
        """Генерация комплексного отчета по кластеру"""
        try:
            report = {}
            
            # Получаем конфигурацию кластера
            config = self.db.get_cluster_config()
            report['config'] = config
            
            # Получаем статистику
            stats = self.db.get_cluster_statistics()
            report['statistics'] = stats
            
            # Получаем данные для анализа
            hv_df, vm_df = self.get_resource_usage_report()
            
            # Анализ гипервизоров
            if not hv_df.empty:
                report['hypervisor_analysis'] = {
                    'total': len(hv_df),
                    'high_cpu_usage': len(hv_df[hv_df['cpu_usage_percent'] > 80]),
                    'high_ram_usage': len(hv_df[hv_df['ram_usage_percent'] > 80]),
                    'avg_cpu_usage': hv_df['cpu_usage_percent'].mean(),
                    'avg_ram_usage': hv_df['ram_usage_percent'].mean(),
                    'most_loaded_hv': hv_df.loc[hv_df['cpu_usage_percent'].idxmax()]['hv_name'] 
                                     if not hv_df.empty else None
                }
            
            # Анализ ВМ
            if not vm_df.empty:
                report['vm_analysis'] = {
                    'total': len(vm_df),
                    'by_type': vm_df['vm_type'].value_counts().to_dict(),
                    'avg_vcpu': vm_df['vcpu'].mean(),
                    'avg_vram': vm_df['vram'].mean(),
                    'avg_vhdd': vm_df['vhdd'].mean(),
                    'total_vcpu': vm_df['vcpu'].sum(),
                    'total_vram': vm_df['vram'].sum(),
                    'total_vhdd': vm_df['vhdd'].sum()
                }
            
            # Рекомендации
            recommendations = []
            
            if stats.get('total_hypervisors', 0) >= int(config.get('max_hypervisors', 24)):
                recommendations.append("Достигнуто максимальное количество гипервизоров в кластере")
            
            if stats.get('free_cpu', 0) < stats.get('total_cpu', 1) * 0.1:
                recommendations.append("Свободных ресурсов CPU менее 10% - рассмотрите добавление гипервизора")
            
            if stats.get('free_ram', 0) < stats.get('total_ram', 1) * 0.1:
                recommendations.append("Свободных ресурсов RAM менее 10% - рассмотрите добавление гипервизора")
            
            report['recommendations'] = recommendations
            
            return report
            
        except Exception as e:
            logger.error(f"Ошибка при генерации отчета по кластеру: {e}")
            return {}
    
    def save_report_to_csv(self, filepath: str = "cluster_report.xlsx"):
        """Сохранение отчета в Excel файл"""
        try:
            hv_df, vm_df = self.get_resource_usage_report()
            
            if hv_df.empty and vm_df.empty:
                logger.warning("Нет данных для экспорта")
                return False
            
            with pd.ExcelWriter(filepath, engine='openpyxl') as writer:
                if not hv_df.empty:
                    # Сохраняем гипервизоры
                    hv_report = hv_df.copy()
                    if 'created_at' in hv_report.columns:
                        hv_report['created_at'] = hv_report['created_at'].apply(
                            lambda x: Formatter.format_datetime(x) if pd.notnull(x) else ''
                        )
                    hv_report.to_excel(writer, sheet_name='Гипервизоры', index=False)
                
                if not vm_df.empty:
                    # Сохраняем ВМ
                    vm_report = vm_df.copy()
                    if 'creation_date' in vm_report.columns:
                        vm_report['creation_date'] = vm_report['creation_date'].apply(
                            lambda x: Formatter.format_datetime(x) if pd.notnull(x) else ''
                        )
                    vm_report.to_excel(writer, sheet_name='Виртуальные машины', index=False)
                
                # Сохраняем сводную статистику
                report = self.generate_cluster_report()
                if report:
                    summary_data = []
                    
                    # Конфигурация
                    summary_data.append(["КОНФИГУРАЦИЯ КЛАСТЕРА", ""])
                    for key, value in report.get('config', {}).items():
                        summary_data.append([key.replace('_', ' ').title(), value])
                    
                    summary_data.append([])
                    summary_data.append(["СТАТИСТИКА", ""])
                    
                    # Статистика
                    stats = report.get('statistics', {})
                    for key, value in stats.items():
                        summary_data.append([key.replace('_', ' ').title(), value])
                    
                    summary_data.append([])
                    summary_data.append(["АНАЛИЗ ГИПЕРВИЗОРОВ", ""])
                    
                    # Анализ гипервизоров
                    hv_analysis = report.get('hypervisor_analysis', {})
                    for key, value in hv_analysis.items():
                        summary_data.append([key.replace('_', ' ').title(), value])
                    
                    summary_data.append([])
                    summary_data.append(["АНАЛИЗ ВИРТУАЛЬНЫХ МАШИН", ""])
                    
                    # Анализ ВМ
                    vm_analysis = report.get('vm_analysis', {})
                    for key, value in vm_analysis.items():
                        summary_data.append([key.replace('_', ' ').title(), value])
                    
                    summary_data.append([])
                    summary_data.append(["РЕКОМЕНДАЦИИ", ""])
                    
                    # Рекомендации
                    for i, rec in enumerate(report.get('recommendations', []), 1):
                        summary_data.append([f"{i}.", rec])
                    
                    summary_df = pd.DataFrame(summary_data, columns=['Параметр', 'Значение'])
                    summary_df.to_excel(writer, sheet_name='Сводный отчет', index=False)
            
            logger.info(f"Отчет сохранен в {filepath}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении отчета: {e}")
            return False
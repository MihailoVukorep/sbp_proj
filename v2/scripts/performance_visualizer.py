import json
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
from pathlib import Path
from typing import Dict, List


class PerformanceVisualizer:
    
    def __init__(self, results_file: str = 'performance_results.json'):
        self.results_file = results_file
        self.results = self._load_results()
        self.output_dir = Path('performance_charts')
        self.output_dir.mkdir(exist_ok=True)
        
        sns.set_theme(style="whitegrid")
        plt.rcParams['figure.figsize'] = (14, 8)
    
    def _load_results(self) -> Dict:
        try:
            with open(self.results_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            print(f"Results file not found: {self.results_file}")
            return {}
    
    def create_execution_time_comparison(self):
        if not self.results:
            return
        
        queries = list(self.results.keys())
        v1_times = [self.results[q]['v1_avg_ms'] for q in queries]
        v2_times = [self.results[q]['v2_avg_ms'] for q in queries]
        
        x = np.arange(len(queries))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars1 = ax.bar(x - width/2, v1_times, width, label='V1 (Original)', color='#e74c3c', alpha=0.8)
        bars2 = ax.bar(x + width/2, v2_times, width, label='V2 (Optimized)', color='#2ecc71', alpha=0.8)
        
        ax.set_xlabel('Upit', fontsize=12, fontweight='bold')
        ax.set_ylabel('Vreme izvršavanja (ms)', fontsize=12, fontweight='bold')
        ax.set_title('Poređenje vremena izvršavanja: V1 vs V2', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(queries)
        ax.legend(fontsize=11)
        ax.grid(axis='y', alpha=0.3)
        
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.1f}ms',
                       ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'execution_time_comparison.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'execution_time_comparison.png'}")
        plt.close()
    
    def create_improvement_percentage_chart(self):
        if not self.results:
            return
        
        queries = list(self.results.keys())
        improvements = [self.results[q]['improvement_percent'] for q in queries]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        colors = ['#27ae60' if imp > 0 else '#e74c3c' for imp in improvements]
        bars = ax.barh(queries, improvements, color=colors, alpha=0.8)
        
        ax.set_xlabel('Poboljšanje performansi (%)', fontsize=12, fontweight='bold')
        ax.set_title('Procentualno poboljšanje performansi V2 u odnosu na V1', fontsize=14, fontweight='bold')
        ax.grid(axis='x', alpha=0.3)
        
        for i, (bar, imp) in enumerate(zip(bars, improvements)):
            ax.text(imp + 1, i, f'{imp:.1f}%', va='center', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'improvement_percentage.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'improvement_percentage.png'}")
        plt.close()
    
    def create_speedup_factor_chart(self):
        if not self.results:
            return
        
        queries = list(self.results.keys())
        speedups = [self.results[q]['speedup_factor'] for q in queries]
        
        fig, ax = plt.subplots(figsize=(12, 6))
        bars = ax.bar(queries, speedups, color='#3498db', alpha=0.8, edgecolor='#2980b9', linewidth=2)
        
        ax.axhline(y=1, color='red', linestyle='--', linewidth=2, label='Bez poboljšanja')
        ax.set_ylabel('Faktor ubrzanja (x)', fontsize=12, fontweight='bold')
        ax.set_title('Faktor ubrzanja: V1 / V2', fontsize=14, fontweight='bold')
        ax.legend(fontsize=11)
        ax.grid(axis='y', alpha=0.3)
        
        for bar, speedup in zip(bars, speedups):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height,
                   f'{speedup:.2f}x',
                   ha='center', va='bottom', fontsize=11, fontweight='bold')
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'speedup_factor.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'speedup_factor.png'}")
        plt.close()
    
    def create_min_max_range_chart(self):
        if not self.results:
            return
        
        queries = list(self.results.keys())
        v1_mins = [self.results[q]['v1_min_ms'] for q in queries]
        v1_maxs = [self.results[q]['v1_max_ms'] for q in queries]
        v2_mins = [self.results[q]['v2_min_ms'] for q in queries]
        v2_maxs = [self.results[q]['v2_max_ms'] for q in queries]
        
        x = np.arange(len(queries))
        width = 0.35
        
        fig, ax = plt.subplots(figsize=(14, 7))
        
        v1_range = [v1_maxs[i] - v1_mins[i] for i in range(len(queries))]
        v2_range = [v2_maxs[i] - v2_mins[i] for i in range(len(queries))]
        
        bars1 = ax.bar(x - width/2, v1_range, width, label='V1 (raspon)', color='#e74c3c', alpha=0.6)
        bars2 = ax.bar(x + width/2, v2_range, width, label='V2 (raspon)', color='#2ecc71', alpha=0.6)
        
        for i, q in enumerate(queries):
            ax.text(i - width/2, v1_mins[i], f'{v1_mins[i]:.1f}', ha='center', va='top', fontsize=8)
            ax.text(i + width/2, v2_mins[i], f'{v2_mins[i]:.1f}', ha='center', va='top', fontsize=8)
        
        ax.set_xlabel('Upit', fontsize=12, fontweight='bold')
        ax.set_ylabel('Vreme (ms)', fontsize=12, fontweight='bold')
        ax.set_title('Raspon vremena izvršavanja (Min-Max): V1 vs V2', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(queries)
        ax.legend(fontsize=11)
        ax.grid(axis='y', alpha=0.3)
        
        plt.tight_layout()
        plt.savefig(self.output_dir / 'min_max_range.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'min_max_range.png'}")
        plt.close()
    
    def create_summary_table_chart(self):
        if not self.results:
            return
        
        fig, ax = plt.subplots(figsize=(14, 8))
        ax.axis('off')
        
        queries = list(self.results.keys())
        columns = ['Upit', 'V1 (ms)', 'V2 (ms)', 'Poboljšanje %', 'Ubrzanje (x)']
        
        data = []
        for q in queries:
            data.append([
                q,
                f"{self.results[q]['v1_avg_ms']:.2f}",
                f"{self.results[q]['v2_avg_ms']:.2f}",
                f"{self.results[q]['improvement_percent']:.1f}%",
                f"{self.results[q]['speedup_factor']:.2f}x"
            ])
        
        table = ax.table(cellText=data, colLabels=columns,
                        cellLoc='center', loc='center',
                        colWidths=[0.15, 0.15, 0.15, 0.25, 0.2])
        
        table.auto_set_font_size(False)
        table.set_fontsize(11)
        table.scale(1, 2.5)
        
        for i in range(len(columns)):
            table[(0, i)].set_facecolor('#34495e')
            table[(0, i)].set_text_props(weight='bold', color='white')
        
        for i in range(1, len(data) + 1):
            for j in range(len(columns)):
                if i % 2 == 0:
                    table[(i, j)].set_facecolor('#ecf0f1')
                else:
                    table[(i, j)].set_facecolor('#ffffff')
        
        plt.title('Detaljni pregled performansi svih upita', fontsize=14, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(self.output_dir / 'summary_table.png', dpi=300, bbox_inches='tight')
        print(f"Saved: {self.output_dir / 'summary_table.png'}")
        plt.close()
    
    def create_all_charts(self):
        print("\nGenerisanje dijagrama...\n")
        self.create_execution_time_comparison()
        self.create_improvement_percentage_chart()
        self.create_speedup_factor_chart()
        self.create_min_max_range_chart()
        self.create_summary_table_chart()
        print(f"\nSvi dijagrami su sačuvani u: {self.output_dir}\n")


if __name__ == "__main__":
    visualizer = PerformanceVisualizer('performance_results.json')
    visualizer.create_all_charts()

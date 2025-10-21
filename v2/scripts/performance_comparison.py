#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Performance Comparison Script - V1 vs V2
Koristi query-je iz odvojenih fajlova
Generiše grafove za poređenje performansi
"""

import time
import json
import os
import statistics
from pymongo import MongoClient
from typing import Dict, List, Tuple
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')

# Importuj sve queries
from queries.query_1 import QUERY_1_V1, QUERY_1_V2, QUERY_NAME as Q1_NAME
from queries.query_2 import QUERY_2_V1, QUERY_2_V2, QUERY_NAME as Q2_NAME
from queries.query_3 import QUERY_3_V1, QUERY_3_V2, QUERY_NAME as Q3_NAME
from queries.query_4 import QUERY_4_V1, QUERY_4_V2, QUERY_NAME as Q4_NAME
from queries.query_5 import QUERY_5_V1, QUERY_5_V2, QUERY_NAME as Q5_NAME


class PerformanceComparator:
    
    def __init__(self, v1_collection, v2_collection, iterations=3):
        self.v1_collection = v1_collection
        self.v2_collection = v2_collection
        self.iterations = iterations
        self.results = {
            'query_1': {'v1': {'times': [], 'docs': [], 'keys': []}, 'v2': {'times': [], 'docs': [], 'keys': []}},
            'query_2': {'v1': {'times': [], 'docs': [], 'keys': []}, 'v2': {'times': [], 'docs': [], 'keys': []}},
            'query_3': {'v1': {'times': [], 'docs': [], 'keys': []}, 'v2': {'times': [], 'docs': [], 'keys': []}},
            'query_4': {'v1': {'times': [], 'docs': [], 'keys': []}, 'v2': {'times': [], 'docs': [], 'keys': []}},
            'query_5': {'v1': {'times': [], 'docs': [], 'keys': []}, 'v2': {'times': [], 'docs': [], 'keys': []}}
        }
    
    def measure_query(self, collection, query_pipeline, query_name: str, version: str) -> Tuple[List, List, List]:
            """Meri vreme izvršavanja query-ja i prikuplja metriku"""
            times = []
            docs_examined = []
            keys_examined = []
            
            for i in range(self.iterations):
                try:
                    start = time.time()
                    cursor = collection.aggregate(query_pipeline, allowDiskUse=True)
                    result = list(cursor)
                    end = time.time()
                    
                    exec_time = (end - start) * 1000
                    times.append(exec_time)
                    
                    # Prikupli metriku iz executionStats
                    try:
                        # Koristimo explain sa verbosity='executionStats'
                        explain_result = collection.database.command(
                            'explain',
                            {
                                'aggregate': collection.name,
                                'pipeline': query_pipeline,
                                'cursor': {},
                                'allowDiskUse': True
                            },
                            verbosity='executionStats'
                        )
                        
                        total_docs = 0
                        total_keys = 0
                        
                        # Prvo proveri da li postoji executionStats na top nivou
                        if 'executionStats' in explain_result:
                            exec_stats = explain_result['executionStats']
                            total_docs = exec_stats.get('totalDocsExamined', 0)
                            total_keys = exec_stats.get('totalKeysExamined', 0)
                            
                            # Ako nema na top nivou, probaj executionStages
                            if total_docs == 0 and 'executionStages' in exec_stats:
                                docs_list = []
                                keys_list = []
                                self._extract_stats_from_stages(exec_stats['executionStages'], docs_list, keys_list)
                                total_docs = sum(docs_list)
                                total_keys = sum(keys_list)
                        
                        # Probaj stages za sharded cluster
                        if total_docs == 0 and 'stages' in explain_result:
                            for stage in explain_result['stages']:
                                if '$cursor' in stage:
                                    cursor_info = stage['$cursor']
                                    if 'executionStats' in cursor_info:
                                        exec_stats = cursor_info['executionStats']
                                        total_docs += exec_stats.get('totalDocsExamined', 0)
                                        total_keys += exec_stats.get('totalKeysExamined', 0)
                                        
                                        if total_docs == 0 and 'executionStages' in exec_stats:
                                            docs_list = []
                                            keys_list = []
                                            self._extract_stats_from_stages(exec_stats['executionStages'], docs_list, keys_list)
                                            total_docs += sum(docs_list)
                                            total_keys += sum(keys_list)
                        
                        # Ako i dalje ništa, probaj queryPlanner stages
                        if total_docs == 0 and 'queryPlanner' in explain_result:
                            planner = explain_result['queryPlanner']
                            if 'winningPlan' in planner:
                                # Za agregaciju, winning plan može biti u stages
                                pass  # Ne možemo dobiti executionStats iz queryPlanner
                            
                    except Exception as e:
                        # Pokušaj alternativni način - direktno explain bez executionStats
                        try:
                            explain_result = collection.database.command(
                                'aggregate',
                                collection.name,
                                pipeline=query_pipeline,
                                explain=True
                            )
                            
                            # Debug: ispiši strukturu
                            if i == 0:  # Samo prvi put
                                print(f"    [Debug] Explain keys: {list(explain_result.keys())}")
                            
                            total_docs = 0
                            total_keys = 0
                            
                        except Exception as e2:
                            print(f"    ⚠️  Could not get explain stats: {str(e2)}")
                            total_docs = 0
                            total_keys = 0
                    
                    docs_examined.append(total_docs)
                    keys_examined.append(total_keys)
                    
                    print(f"  Iteration {i+1}: {exec_time:.2f}ms, Docs: {total_docs}, Keys: {total_keys}")
                    
                except Exception as e:
                    print(f"  ✗ Error in iteration {i+1}: {str(e)}")
                    times.append(None)
                    docs_examined.append(None)
                    keys_examined.append(None)
            
            return times, docs_examined, keys_examined
        
    def _extract_stats_from_stages(self, stage, docs_list, keys_list):
        """Rekurzivno izvlači statistiku iz executionStages"""
        if isinstance(stage, dict):
            # Dodaj trenutne vrednosti
            if 'docsExamined' in stage:
                docs_list.append(stage['docsExamined'])
            if 'keysExamined' in stage:
                keys_list.append(stage['keysExamined'])
            
            # Proveri inputStage
            if 'inputStage' in stage:
                self._extract_stats_from_stages(stage['inputStage'], docs_list, keys_list)
            
            # Proveri inputStages (za union i slične operacije)
            if 'inputStages' in stage:
                for input_stage in stage['inputStages']:
                    self._extract_stats_from_stages(input_stage, docs_list, keys_list)
    
    
    def run_comparison(self):
        """Izvršava sve upite i poredi performanse"""
        print("\n" + "="*70)
        print("PERFORMANCE COMPARISON: V1 vs V2")
        print("="*70)
        
        queries = [
            ('query_1', QUERY_1_V1, QUERY_1_V2, Q1_NAME),
            ('query_2', QUERY_2_V1, QUERY_2_V2, Q2_NAME),
            ('query_3', QUERY_3_V1, QUERY_3_V2, Q3_NAME),
            ('query_4', QUERY_4_V1, QUERY_4_V2, Q4_NAME),
            ('query_5', QUERY_5_V1, QUERY_5_V2, Q5_NAME),
        ]
        
        for query_name, query_v1_pipeline, query_v2_pipeline, description in queries:
            print(f"\n{query_name.upper()}: {description}")
            print("-" * 70)
            
            # Meri V1
            print(f"V1 (Original - no optimization):")
            v1_times, v1_docs, v1_keys = self.measure_query(
                self.v1_collection, query_v1_pipeline, query_name, "V1"
            )
            
            # Meri V2
            print(f"\nV2 (Optimized - computed fields + indexes):")
            v2_times, v2_docs, v2_keys = self.measure_query(
                self.v2_collection, query_v2_pipeline, query_name, "V2"
            )
            
            # Sačuvaj rezultate
            self.results[query_name]['v1']['times'] = [t for t in v1_times if t is not None]
            self.results[query_name]['v1']['docs'] = [d for d in v1_docs if d is not None]
            self.results[query_name]['v1']['keys'] = [k for k in v1_keys if k is not None]
            
            self.results[query_name]['v2']['times'] = [t for t in v2_times if t is not None]
            self.results[query_name]['v2']['docs'] = [d for d in v2_docs if d is not None]
            self.results[query_name]['v2']['keys'] = [k for k in v2_keys if k is not None]
            
            # Prikaži statistiku
            self._print_query_stats(query_name, description)
    
    def _print_query_stats(self, query_name: str, description: str):
        """Prikaži statistiku za query"""
        v1_times = self.results[query_name]['v1']['times']
        v2_times = self.results[query_name]['v2']['times']
        v1_docs = self.results[query_name]['v1']['docs']
        v2_docs = self.results[query_name]['v2']['docs']
        v1_keys = self.results[query_name]['v1']['keys']
        v2_keys = self.results[query_name]['v2']['keys']
        
        if v1_times and v2_times:
            v1_avg = statistics.mean(v1_times)
            v2_avg = statistics.mean(v2_times)
            v1_docs_avg = statistics.mean(v1_docs) if v1_docs else 0
            v2_docs_avg = statistics.mean(v2_docs) if v2_docs else 0
            v1_keys_avg = statistics.mean(v1_keys) if v1_keys else 0
            v2_keys_avg = statistics.mean(v2_keys) if v2_keys else 0
            
            time_improvement = ((v1_avg - v2_avg) / v1_avg) * 100 if v1_avg > 0 else 0
            docs_improvement = ((v1_docs_avg - v2_docs_avg) / v1_docs_avg) * 100 if v1_docs_avg > 0 else 0
            keys_improvement = ((v1_keys_avg - v2_keys_avg) / v1_keys_avg) * 100 if v1_keys_avg > 0 else 0
            
            speedup = v1_avg / v2_avg if v2_avg > 0 else 0
            
            print(f"\n  📊 RESULTS:")
            print(f"  ⏱️  Execution Time:")
            print(f"     V1: {v1_avg:.2f}ms | V2: {v2_avg:.2f}ms | Improvement: {time_improvement:.1f}% | Speedup: {speedup:.2f}x")
            print(f"  📄 Docs Examined:")
            print(f"     V1: {v1_docs_avg:.0f} | V2: {v2_docs_avg:.0f} | Improvement: {docs_improvement:.1f}%")
            print(f"  🔑 Keys Examined:")
            print(f"     V1: {v1_keys_avg:.0f} | V2: {v2_keys_avg:.0f} | Improvement: {keys_improvement:.1f}%")
        else:
            print("  ✗ Error: Could not measure times")
    
    def get_summary(self) -> Dict:
        """Generiše sumarnu statistiku"""
        summary = {}
        
        for query_name, data in self.results.items():
            v1_times = data['v1']['times']
            v2_times = data['v2']['times']
            v1_docs = data['v1']['docs']
            v2_docs = data['v2']['docs']
            v1_keys = data['v1']['keys']
            v2_keys = data['v2']['keys']
            
            if v1_times and v2_times:
                v1_avg_time = statistics.mean(v1_times)
                v2_avg_time = statistics.mean(v2_times)
                v1_avg_docs = statistics.mean(v1_docs) if v1_docs else 0
                v2_avg_docs = statistics.mean(v2_docs) if v2_docs else 0
                v1_avg_keys = statistics.mean(v1_keys) if v1_keys else 0
                v2_avg_keys = statistics.mean(v2_keys) if v2_keys else 0
                
                summary[query_name] = {
                    'v1_avg_time_ms': round(v1_avg_time, 2),
                    'v2_avg_time_ms': round(v2_avg_time, 2),
                    'v1_avg_docs': round(v1_avg_docs, 0),
                    'v2_avg_docs': round(v2_avg_docs, 0),
                    'v1_avg_keys': round(v1_avg_keys, 0),
                    'v2_avg_keys': round(v2_avg_keys, 0),
                    'time_improvement_percent': round(((v1_avg_time - v2_avg_time) / v1_avg_time) * 100, 1),
                    'docs_improvement_percent': round(((v1_avg_docs - v2_avg_docs) / v1_avg_docs) * 100, 1) if v1_avg_docs > 0 else 0,
                    'keys_improvement_percent': round(((v1_avg_keys - v2_avg_keys) / v1_avg_keys) * 100, 1) if v1_avg_keys > 0 else 0,
                    'speedup_factor': round(v1_avg_time / v2_avg_time, 2)
                }
        
        return summary
    
    def generate_graphs(self, output_dir: str = 'output/graphs'):
        """Generiše sve grafove za poređenje"""
        os.makedirs(output_dir, exist_ok=True)
        summary = self.get_summary()
        
        query_names = ['query_1', 'query_2', 'query_3', 'query_4', 'query_5']
        query_labels = ['Q1: Companies', 'Q2: Genre/Decade', 'Q3: Blockbuster Month', 'Q4: Profitability', 'Q5: Runtime/Country']
        
        # 1. Glavni graf - Execution Time Comparison
        self._generate_execution_time_graph(summary, query_labels, output_dir)
        
        # 2-6. Po jedan graf za svaki query sa tri metrike
        for i, query_name in enumerate(query_names):
            self._generate_query_metrics_graph(query_name, query_labels[i], output_dir)
    
    def _generate_execution_time_graph(self, summary, query_labels, output_dir):
        """Generiše graf poređenja vremena izvršavanja"""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        queries = list(summary.keys())
        v1_times = [summary[q]['v1_avg_time_ms'] for q in queries]
        v2_times = [summary[q]['v2_avg_time_ms'] for q in queries]
        
        x = range(len(queries))
        width = 0.35
        
        bars1 = ax.bar([i - width/2 for i in x], v1_times, width, label='V1 (Original)', color='#FF6B6B', alpha=0.8)
        bars2 = ax.bar([i + width/2 for i in x], v2_times, width, label='V2 (Optimized)', color='#4ECDC4', alpha=0.8)
        
        ax.set_xlabel('Queries', fontsize=12, fontweight='bold')
        ax.set_ylabel('Execution Time (ms)', fontsize=12, fontweight='bold')
        ax.set_title('Performance Comparison: V1 vs V2 - Execution Time', fontsize=14, fontweight='bold')
        ax.set_xticks(x)
        ax.set_xticklabels(query_labels, rotation=15, ha='right')
        ax.legend(fontsize=11)
        ax.grid(axis='y', alpha=0.3)
        
        # Dodaj vrijednosti na stupce
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{height:.2f}',
                       ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        plt.savefig(f'{output_dir}/execution_time_comparison.png', dpi=300, bbox_inches='tight')
        print(f"✓ Graph saved: {output_dir}/execution_time_comparison.png")
        plt.close()
    
    def _generate_query_metrics_graph(self, query_name, query_label, output_dir):
        """Generiše graf sa tri metrike za specifičan query"""
        data = self.results[query_name]
        
        if not data['v1']['times'] or not data['v2']['times']:
            print(f"⚠️  Skipping {query_name} - insufficient data")
            return
        
        v1_times_avg = statistics.mean(data['v1']['times'])
        v2_times_avg = statistics.mean(data['v2']['times'])
        
        v1_docs_avg = statistics.mean(data['v1']['docs']) if data['v1']['docs'] else 0
        v2_docs_avg = statistics.mean(data['v2']['docs']) if data['v2']['docs'] else 0
        
        v1_keys_avg = statistics.mean(data['v1']['keys']) if data['v1']['keys'] else 0
        v2_keys_avg = statistics.mean(data['v2']['keys']) if data['v2']['keys'] else 0
        
        fig, axes = plt.subplots(1, 3, figsize=(15, 5))
        fig.suptitle(f'{query_label} - Performance Metrics (V1 vs V2)', fontsize=14, fontweight='bold')
        
        metrics = [
            ('Execution Time (ms)', [v1_times_avg, v2_times_avg], axes[0]),
            ('Total Docs Examined', [v1_docs_avg, v2_docs_avg], axes[1]),
            ('Total Keys Examined', [v1_keys_avg, v2_keys_avg], axes[2])
        ]
        
        for metric_name, values, ax in metrics:
            colors = ['#FF6B6B', '#4ECDC4']
            bars = ax.bar(['V1', 'V2'], values, color=colors, alpha=0.8)
            
            ax.set_ylabel(metric_name, fontsize=11, fontweight='bold')
            ax.set_title(metric_name, fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3)
            
            # Dodaj vrijednosti
            for bar, value in zip(bars, values):
                height = bar.get_height()
                ax.text(bar.get_x() + bar.get_width()/2., height,
                       f'{value:.2f}' if value < 1000 else f'{value:.0f}',
                       ha='center', va='bottom', fontsize=10, fontweight='bold')
            
            # Dodaj improvement
            if values[0] > 0:
                improvement = ((values[0] - values[1]) / values[0]) * 100
                ax.text(0.5, max(values) * 0.9, f'Improvement: {improvement:.1f}%',
                       ha='center', fontsize=10, 
                       bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        plt.tight_layout()
        filename = f'{output_dir}/{query_name}_metrics.png'
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        print(f"✓ Graph saved: {filename}")
        plt.close()
    
    def export_results(self, filepath: str):
        """Eksportuje rezultate u JSON"""
        os.makedirs(os.path.dirname(filepath) if os.path.dirname(filepath) else '.', exist_ok=True)
        summary = self.get_summary()
        
        # Dodaj ukupnu statistiku
        total_v1_time = sum(q['v1_avg_time_ms'] for q in summary.values())
        total_v2_time = sum(q['v2_avg_time_ms'] for q in summary.values())
        total_v1_docs = sum(q['v1_avg_docs'] for q in summary.values())
        total_v2_docs = sum(q['v2_avg_docs'] for q in summary.values())
        total_v1_keys = sum(q['v1_avg_keys'] for q in summary.values())
        total_v2_keys = sum(q['v2_avg_keys'] for q in summary.values())
        
        output = {
            'timestamp': time.strftime('%Y-%m-%d %H:%M:%S'),
            'iterations': self.iterations,
            'queries': summary,
            'totals': {
                'total_v1_time_ms': round(total_v1_time, 2),
                'total_v2_time_ms': round(total_v2_time, 2),
                'total_v1_docs': round(total_v1_docs, 0),
                'total_v2_docs': round(total_v2_docs, 0),
                'total_v1_keys': round(total_v1_keys, 0),
                'total_v2_keys': round(total_v2_keys, 0),
                'total_time_improvement_percent': round(((total_v1_time - total_v2_time) / total_v1_time) * 100, 1),
                'total_docs_improvement_percent': round(((total_v1_docs - total_v2_docs) / total_v1_docs) * 100, 1) if total_v1_docs > 0 else 0,
                'total_keys_improvement_percent': round(((total_v1_keys - total_v2_keys) / total_v1_keys) * 100, 1) if total_v1_keys > 0 else 0,
                'total_speedup': round(total_v1_time / total_v2_time, 2)
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Results exported to: {filepath}")


if __name__ == "__main__":
    # Konekcija na MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['SBP_DB']
    
    v1_collection = db['movies']
    v2_collection = db['movies_optimized']
    
    # Proveri kolekcije
    print("\n🔍 Checking collections...")
    v1_count = v1_collection.count_documents({})
    v2_count = v2_collection.count_documents({})
    
    print(f"✓ V1 (movies): {v1_count:,} documents")
    print(f"✓ V2 (movies_optimized): {v2_count:,} documents")
    
    # Pokreni poređenje
    comparator = PerformanceComparator(v1_collection, v2_collection, iterations=3)
    comparator.run_comparison()
    
    # Prikaži summary
    summary = comparator.get_summary()
    print("\n" + "="*70)
    print("📈 SUMMARY REPORT")
    print("="*70)
    
    total_v1_time = 0
    total_v2_time = 0
    total_v1_docs = 0
    total_v2_docs = 0
    total_v1_keys = 0
    total_v2_keys = 0
    
    for query, metrics in summary.items():
        print(f"\n{query}:")
        print(f"  ⏱️  V1: {metrics['v1_avg_time_ms']}ms | V2: {metrics['v2_avg_time_ms']}ms")
        print(f"  📄 V1 Docs: {metrics['v1_avg_docs']:.0f} | V2 Docs: {metrics['v2_avg_docs']:.0f}")
        print(f"  🔑 V1 Keys: {metrics['v1_avg_keys']:.0f} | V2 Keys: {metrics['v2_avg_keys']:.0f}")
        print(f"  ⚡ Time Improvement: {metrics['time_improvement_percent']}% | Speedup: {metrics['speedup_factor']}x")
        
        total_v1_time += metrics['v1_avg_time_ms']
        total_v2_time += metrics['v2_avg_time_ms']
        total_v1_docs += metrics['v1_avg_docs']
        total_v2_docs += metrics['v2_avg_docs']
        total_v1_keys += metrics['v1_avg_keys']
        total_v2_keys += metrics['v2_avg_keys']
    
    total_time_improvement = ((total_v1_time - total_v2_time) / total_v1_time) * 100
    total_time_speedup = total_v1_time / total_v2_time
    
    print("\n" + "="*70)
    print(f"🎯 OVERALL PERFORMANCE:")
    print(f"  V1 Total Time: {total_v1_time:.2f}ms")
    print(f"  V2 Total Time: {total_v2_time:.2f}ms")
    print(f"  V1 Total Docs: {total_v1_docs:.0f}")
    print(f"  V2 Total Docs: {total_v2_docs:.0f}")
    print(f"  V1 Total Keys: {total_v1_keys:.0f}")
    print(f"  V2 Total Keys: {total_v2_keys:.0f}")
    print(f"  Total Time Improvement: {total_time_improvement:.1f}%")
    print(f"  Total Speedup: {total_time_speedup:.2f}x")
    print("="*70)
    
    # Eksportuj rezultate
    comparator.export_results('output/performance_results.json')
    
    # Generiši grafove
    print("\n📊 Generating graphs...")
    comparator.generate_graphs()
    print("✓ All graphs generated successfully!")
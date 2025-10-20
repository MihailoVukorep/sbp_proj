#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import sys
from pathlib import Path
from datetime import datetime


def print_performance_chart(query_results: list):
    print("\n" + "="*80)
    print("PERFORMANCE CHART - V1 vs V2")
    print("="*80 + "\n")
    
    max_time = max(
        max(q['v1_avg_ms'], q['v2_avg_ms']) 
        for q in query_results
    )
    
    width_scale = 50 / max_time
    
    print(f"{'Query':<5} {'V1':<35} {'V2':<35} {'Poboljšanje':<15}")
    print("-" * 95)
    
    for i, query in enumerate(query_results, 1):
        v1_bar_width = int(query['v1_avg_ms'] * width_scale)
        v2_bar_width = int(query['v2_avg_ms'] * width_scale)
        
        v1_bar = "█" * v1_bar_width
        v2_bar = "█" * v2_bar_width
        
        v1_str = f"{v1_bar} {query['v1_avg_ms']:.1f}ms"
        v2_str = f"{v2_bar} {query['v2_avg_ms']:.1f}ms"
        
        improvement = query['improvement_percent']
        speedup = query['speedup']
        
        print(f"Q{i:<4} {v1_str:<35} {v2_str:<35} {improvement:>5.1f}% ({speedup:.1f}x)")
    
    print("\n")


def print_comparison_table(query_results: list):
    print("\n" + "="*80)
    print("DETALJNO POREĐENJE - V1 vs V2")
    print("="*80 + "\n")
    
    print(f"{'Q':<3} {'V1 Avg (ms)':<15} {'V2 Avg (ms)':<15} {'Poboljšanje':<15} {'Ubrzanje':<10}")
    print("-" * 80)
    
    for i, query in enumerate(query_results, 1):
        improvement = f"{query['improvement_percent']:.1f}%"
        speedup = f"{query['speedup']:.2f}x"
        
        print(f"Q{i:<2} {query['v1_avg_ms']:<15.2f} {query['v2_avg_ms']:<15.2f} {improvement:<15} {speedup:<10}")
    
    print("\n")


def print_optimization_guide(query_results: list):
    print("\n" + "="*80)
    print("PRIMENJENA POBOLJŠANJA PO UPITIMA")
    print("="*80 + "\n")
    
    optimizations = {
        "Query 1: Average revenue per company (budget > 50M)": [
            "Precomputed budget_category (ne računaj u pipeline)",
            "Index na budget_category (umesto full collection scan)",
            "Denormalizovane vrednosti na top nivou",
        ],
        "Query 2: Average rating per genre by decade": [
            "Precomputed decade field (ne računaj (year // 10) * 10)",
            "Denormalizovana godina i mesec",
            "Index na decade i genres",
        ],
        "Query 3: Months with most blockbuster movies": [
            "Precomputed budget_category (blockbuster)",
            "Denormalizovani mesec u release_info.month",
            "Compound index na (budget_category, month)",
        ],
        "Query 4: Most profitable genre combinations": [
            "Precomputed profit i roi (ne računaj u pipeline)",
            "Precomputed genre_pairs (sve kombinacije od 2)",
            "Precomputed is_profitable boolean",
            "Index na genre_pairs (multikey index)",
        ],
        "Query 5: Average runtime per country (rating > 7)": [
            "Precomputed quality_tier (mapiranje vote_average)",
            "Index na quality_tier (umesto vote_average filter)",
            "Denormalizovani runtime na top nivou",
        ]
    }
    
    for i, query in enumerate(query_results, 1):
        query_name = query['name']
        improvements = optimizations.get(query_name, [])
        
        print(f"Query {i}: {query['name']}")
        print(f"Opis: {query['description']}")
        print(f"Poboljšanje: {query['improvement_percent']:.1f}% ({query['speedup']:.2f}x)")
        print("\nPrimenjena poboljšanja:")
        for opt in improvements:
            print(f"  - {opt}")
        print()


def print_summary_statistics(data: dict):
    print("\n" + "="*80)
    print("SUMARIZOVANA STATISTIKA")
    print("="*80 + "\n")
    
    print(f"Ukupno vreme izvršavanja:")
    print(f"  V1: {data['total_v1_time_ms']:.2f}ms")
    print(f"  V2: {data['total_v2_time_ms']:.2f}ms")
    print(f"  Razlika: {data['total_v1_time_ms'] - data['total_v2_time_ms']:.2f}ms")
    print()
    
    print(f"Performance poboljšanja:")
    print(f"  Ukupno poboljšanje: {data['total_improvement_percent']:.1f}%")
    avg_improvement = sum(q['improvement_percent'] for q in data['queries']) / len(data['queries'])
    print(f"  Prosečno poboljšanje: {avg_improvement:.1f}%")
    print(f"  Ukupno ubrzanje: {data['total_speedup']:.2f}x")
    avg_speedup = sum(q['speedup'] for q in data['queries']) / len(data['queries'])
    print(f"  Prosečno ubrzanje: {avg_speedup:.2f}x")
    print()
    
    print(f"Broj upita: {data['total_queries']}")
    print(f"Vreme izvršavanja: {data['execution_time']}")
    print()


def generate_html_report(data: dict, output_file: Path):
    queries = data['queries']
    query_labels = [f"Q{i+1}" for i in range(len(queries))]
    v1_times = [q['v1_avg_ms'] for q in queries]
    v2_times = [q['v2_avg_ms'] for q in queries]
    improvements = [q['improvement_percent'] for q in queries]
    
    table_rows = ""
    for i, q in enumerate(queries, 1):
        improvement_class = "improvement-high" if q['improvement_percent'] > 40 else "improvement-medium"
        table_rows += f"""                    <tr>
                        <td>Query {i}</td>
                        <td>{q['v1_avg_ms']:.2f}</td>
                        <td>{q['v2_avg_ms']:.2f}</td>
                        <td class="{improvement_class}">{q['improvement_percent']:.1f}%</td>
                        <td>{q['speedup']:.2f}x</td>
                    </tr>
"""
    
    avg_improvement = sum(q['improvement_percent'] for q in queries) / len(queries)
    avg_speedup = sum(q['speedup'] for q in queries) / len(queries)
    
    html_content = f"""<!DOCTYPE html>
<html lang="sr">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Performance Analysis V1 vs V2</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #f5f5f5; }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}
        h1 {{ color: #333; margin-bottom: 10px; }}
        .subtitle {{ color: #666; margin-bottom: 30px; font-size: 14px; }}
        .summary-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .summary-card {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .summary-card h3 {{ color: #333; font-size: 14px; margin-bottom: 10px; }}
        .summary-card .value {{ font-size: 32px; font-weight: bold; color: #007bff; }}
        .summary-card .unit {{ color: #666; font-size: 12px; }}
        .charts-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(500px, 1fr)); gap: 20px; margin-bottom: 30px; }}
        .chart-container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
        .chart-container h2 {{ color: #333; font-size: 18px; margin-bottom: 15px; }}
        .table-container {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); overflow-x: auto; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 14px; }}
        th {{ background: #007bff; color: white; padding: 12px; text-align: left; }}
        td {{ padding: 12px; border-bottom: 1px solid #ddd; }}
        tr:hover {{ background: #f9f9f9; }}
        .improvement-high {{ color: #28a745; font-weight: bold; }}
        .improvement-medium {{ color: #ffc107; font-weight: bold; }}
        .improvement-low {{ color: #dc3545; }}
        footer {{ text-align: center; color: #666; margin-top: 40px; padding-top: 20px; border-top: 1px solid #ddd; }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Performance Analysis: V1 vs V2</h1>
        <p class="subtitle">Detaljno poređenje performansi MongoDB upita - originalna verzija (V1) vs optimizovana verzija (V2)</p>
        
        <div class="summary-grid">
            <div class="summary-card">
                <h3>Ukupno poboljšanje</h3>
                <div class="value">{data['total_improvement_percent']:.1f}%</div>
                <div class="unit">brže u V2</div>
            </div>
            <div class="summary-card">
                <h3>Prosečno poboljšanje</h3>
                <div class="value">{avg_improvement:.1f}%</div>
                <div class="unit">po upitu</div>
            </div>
            <div class="summary-card">
                <h3>Ukupno ubrzanje</h3>
                <div class="value">{data['total_speedup']:.2f}x</div>
                <div class="unit">brže u V2</div>
            </div>
            <div class="summary-card">
                <h3>Prosečno ubrzanje</h3>
                <div class="value">{avg_speedup:.2f}x</div>
                <div class="unit">po upitu</div>
            </div>
            <div class="summary-card">
                <h3>V1 Ukupno vreme</h3>
                <div class="value">{data['total_v1_time_ms']:.0f}</div>
                <div class="unit">ms za sve upite</div>
            </div>
            <div class="summary-card">
                <h3>V2 Ukupno vreme</h3>
                <div class="value">{data['total_v2_time_ms']:.0f}</div>
                <div class="unit">ms za sve upite</div>
            </div>
        </div>
        
        <div class="charts-grid">
            <div class="chart-container">
                <h2>Vremenska Poređenja (ms)</h2>
                <canvas id="timingChart"></canvas>
            </div>
            <div class="chart-container">
                <h2>Poboljšanja po Upitima</h2>
                <canvas id="improvementChart"></canvas>
            </div>
        </div>
        
        <div class="table-container">
            <h2>Detaljne Metrike</h2>
            <table>
                <thead>
                    <tr>
                        <th>Query</th>
                        <th>V1 Avg (ms)</th>
                        <th>V2 Avg (ms)</th>
                        <th>Poboljšanje</th>
                        <th>Ubrzanje</th>
                    </tr>
                </thead>
                <tbody>
{table_rows}
                </tbody>
            </table>
        </div>
        
        <footer>
            <p>Izveštaj generishan: {data['execution_time']}</p>
            <p>Korišćeni upiti iz v1/marko/queries.sql i v2/marko/queries.sql</p>
        </footer>
    </div>
    
    <script>
        const timingCtx = document.getElementById('timingChart').getContext('2d');
        const timingChart = new Chart(timingCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(query_labels)},
                datasets: [
                    {{
                        label: 'V1 (ms)',
                        data: {json.dumps(v1_times)},
                        backgroundColor: 'rgba(220, 53, 69, 0.7)',
                        borderColor: 'rgba(220, 53, 69, 1)',
                        borderWidth: 1
                    }},
                    {{
                        label: 'V2 (ms)',
                        data: {json.dumps(v2_times)},
                        backgroundColor: 'rgba(40, 167, 69, 0.7)',
                        borderColor: 'rgba(40, 167, 69, 1)',
                        borderWidth: 1
                    }}
                ]
            }},
            options: {{
                responsive: true,
                scales: {{
                    y: {{ beginAtZero: true, title: {{ display: true, text: 'Vreme (ms)' }} }}
                }},
                plugins: {{ legend: {{ position: 'top' }} }}
            }}
        }});
        
        const improvementCtx = document.getElementById('improvementChart').getContext('2d');
        const improvementChart = new Chart(improvementCtx, {{
            type: 'bar',
            data: {{
                labels: {json.dumps(query_labels)},
                datasets: [{{
                    label: 'Poboljšanje (%)',
                    data: {json.dumps(improvements)},
                    backgroundColor: [
                        'rgba(52, 211, 153, 0.7)',
                        'rgba(52, 211, 153, 0.7)',
                        'rgba(52, 211, 153, 0.7)',
                        'rgba(52, 211, 153, 0.7)',
                        'rgba(52, 211, 153, 0.7)'
                    ],
                    borderColor: 'rgba(16, 185, 129, 1)',
                    borderWidth: 1
                }}]
            }},
            options: {{
                responsive: true,
                indexAxis: 'y',
                scales: {{
                    x: {{ beginAtZero: true, max: 100 }}
                }}
            }}
        }});
    </script>
</body>
</html>
"""
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print(f"HTML izveštaj generishan: {output_file}")


def main():
    json_file = Path('output/performance_comparison_detailed.json')
    
    if not json_file.exists():
        print("Greška: performance_comparison_detailed.json nije pronađen")
        print("Prvo pokrenite: python run_performance_analysis_v2.py")
        sys.exit(1)
    
    with open(json_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    html_file = output_dir / f'performance_report_{timestamp}.html'
    json_copy = output_dir / f'performance_data_{timestamp}.json'
    
    print_performance_chart(data['queries'])
    print_comparison_table(data['queries'])
    print_optimization_guide(data['queries'])
    print_summary_statistics(data)
    
    generate_html_report(data, html_file)
    
    with open(json_copy, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)
    
    print(f"\nSvi izveštaji su generisani u folder: {output_dir}")
    print(f"  - {html_file.name} (interaktivni grafikoni)")
    print(f"  - {json_copy.name} (sirovi podaci)")


if __name__ == "__main__":
    main()
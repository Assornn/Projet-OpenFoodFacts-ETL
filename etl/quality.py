"""
Module de contrôle qualité
"""

from pyspark.sql.functions import *

class DataQualityChecker:
    def __init__(self, spark):
        self.spark = spark
        self.rules = self._define_rules()
    
    def _define_rules(self):
        return {
            'bounds_sugars': {'min': 0, 'max': 100},
            'bounds_salt': {'min': 0, 'max': 25},
            'bounds_fat': {'min': 0, 'max': 100}
        }
    
    def check_bounds(self, df, field, min_val, max_val):
        """Vérifie les bornes numériques"""
        total = df.filter(col(field).isNotNull()).count()
        out_of_bounds = df.filter(
            (col(field) < min_val) | (col(field) > max_val)
        ).count()
        
        return {
            'field': field,
            'passed': out_of_bounds == 0,
            'anomalies': out_of_bounds,
            'total': total
        }
    
    def run_checks(self, df):
        """Exécute tous les contrôles"""
        print("\n=== CONTRÔLES QUALITÉ ===")
        results = []
        
        for rule_name, rule in self.rules.items():
            if 'bounds' in rule_name:
                field = rule_name.replace('bounds_', '') + '_100g'
                result = self.check_bounds(df, field, rule['min'], rule['max'])
                results.append(result)
                status = "✓ PASS" if result['passed'] else "✗ FAIL"
                print(f"{status} - {rule_name}: {result['anomalies']} anomalies")
        
        return results

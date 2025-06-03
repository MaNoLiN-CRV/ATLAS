#!/usr/bin/env python3
"""
Script para probar que la configuración de performance se carga correctamente.
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.utils.performance_config import get_performance_config

def test_performance_config():
    """Prueba la carga de configuración de performance."""
    print("=== Test de carga de configuración de performance ===")
    
    try:
        # Obtener la configuración
        config = get_performance_config()
        print(f"✓ Configuración cargada desde: {config.config_file_path}")
        
        # Verificar que el archivo existe
        if os.path.exists(config.config_file_path):
            print(f"✓ Archivo de configuración existe: {config.config_file_path}")
        else:
            print(f"✗ Archivo de configuración NO existe: {config.config_file_path}")
            return False
        
        # Obtener los thresholds
        thresholds = config.get_thresholds()
        print(f"✓ Thresholds cargados correctamente")
        
        # Mostrar un resumen
        summary = config.get_config_summary()
        print("\n=== Resumen de configuración ===")
        print(f"Archivo: {summary['config_file']}")
        print(f"Modo actual basado en thresholds:")
        print(f"  - Tiempo promedio límite: {thresholds.avg_execution_time_threshold_ms}ms")
        print(f"  - Tiempo total límite: {thresholds.total_execution_time_threshold_ms}ms")
        print(f"  - CPU ratio límite: {thresholds.cpu_efficiency_ratio_threshold}")
        print(f"  - Queries máximas por colección: {thresholds.max_stored_queries_per_collection}")
        
        print("\n✓ Configuración de performance cargada exitosamente!")
        return True
        
    except Exception as e:
        print(f"✗ Error cargando configuración: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_performance_config()
    sys.exit(0 if success else 1)

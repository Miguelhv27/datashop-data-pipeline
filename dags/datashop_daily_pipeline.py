from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from scripts.transform import calculate_daily_metrics
from scripts.data_quality_check import validate_orders_data, validate_customers_data

default_args = {
    'owner': 'datashop',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def check_source_files(**kwargs):
    """Verifica que los archivos fuente existan"""
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    orders_file = f"data/input/orders_{date_str}.csv"
    customers_file = "data/input/customers.csv"
    
    if not os.path.exists(orders_file):
        raise AirflowFailException(f"Archivo de órdenes no encontrado: {orders_file}")
    
    if not os.path.exists(customers_file):
        raise AirflowFailException(f"Archivo de clientes no encontrado: {customers_file}")
    
    print(f" Archivos fuente verificados: {orders_file}, {customers_file}")
    return True

def run_data_quality_checks(**kwargs):
    """Ejecuta controles de calidad de datos"""
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    orders_file = f"data/input/orders_{date_str}.csv"
    customers_file = "data/input/customers.csv"
    
    try:
        validate_orders_data(orders_file)
        validate_customers_data(customers_file)
        return "Controles de calidad pasados exitosamente"
    except Exception as e:
        raise AirflowFailException(f"Fallaron controles de calidad: {str(e)}")

def transform_data(**kwargs):
    """Ejecuta la transformación de datos"""
    execution_date = kwargs['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    orders_file = f"data/input/orders_{date_str}.csv"
    customers_file = "data/input/customers.csv"
    output_path = "data/output/"
    
    try:
        results = calculate_daily_metrics(orders_file, customers_file, output_path)
        print(f"Transformación completada: {results}")
        return results
    except Exception as e:
        raise AirflowFailException(f"Error en transformación: {str(e)}")

def load_results(**kwargs):
    """Simula la carga a data warehouse"""
    ti = kwargs['ti']
    transform_results = ti.xcom_pull(task_ids='transformar_datos')
    
    print("=" * 50)
    print("INFORME DIARIO - DATASHOP")
    print("=" * 50)
    print(f"Ventas totales: ${transform_results['total_sales']:,.2f}")
    print(f"Cliente destacado: {transform_results['biggest_customer']['customer_name']}")
    print(f"Monto gastado: ${transform_results['biggest_customer']['total_spent']:,.2f}")
    print("\nTop 5 productos:")
    for i, product in enumerate(transform_results['top_5_products'], 1):
        print(f"  {i}. Producto {product['product_id']}: {product['quantity']} unidades")
    print("=" * 50)
    
    print(" Resultados 'cargados' a data warehouse (simulado)")
    return "Carga completada exitosamente"

with DAG(
    'datashop_daily_pipeline',
    default_args=default_args,
    description='Pipeline diario de procesamiento para DataShop',
    schedule_interval='0 6 * * *',  
    catchup=False,
    tags=['datashop', 'ecommerce']
) as dag:
    
    verificar_datos_fuente = PythonOperator(
        task_id='verificar_datos_fuente',
        python_callable=check_source_files,
        provide_context=True
    )
    
    ejecutar_control_calidad = PythonOperator(
        task_id='ejecutar_control_calidad',
        python_callable=run_data_quality_checks,
        provide_context=True
    )
    
    transformar_datos = PythonOperator(
        task_id='transformar_datos',
        python_callable=transform_data,
        provide_context=True
    )
    
    cargar_resultado = PythonOperator(
        task_id='cargar_resultado',
        python_callable=load_results,
        provide_context=True
    )
    
    verificar_datos_fuente >> ejecutar_control_calidad >> transformar_datos >> cargar_resultado

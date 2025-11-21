import pytest
import pandas as pd
import sys
import os

# Agregar el directorio padre al path para imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from scripts.transform import calculate_daily_metrics
from scripts.data_quality_check import validate_orders_data


class TestDataTransformations:

    def test_calculate_total_sales(self, tmp_path):
        """Verifica que el cálculo de ventas totales sea correcto"""
        orders_data = {
            'order_id': [1, 2, 3],
            'customer_id': [101, 102, 101],
            'product_id': [1, 2, 1],
            'quantity': [2, 1, 3],
            'unit_price': [10.0, 15.0, 10.0],
            'order_date': ['2024-01-01', '2024-01-01', '2024-01-01']
        }

        customers_data = {
            'customer_id': [101, 102],
            'customer_name': ['Juan Pérez', 'María García'],
            'email': ['juan@email.com', 'maria@email.com']
        }

        orders_file = tmp_path / "test_orders.csv"
        customers_file = tmp_path / "test_customers.csv"
        output_path = tmp_path / "output"

        pd.DataFrame(orders_data).to_csv(orders_file, index=False)
        pd.DataFrame(customers_data).to_csv(customers_file, index=False)

        results = calculate_daily_metrics(orders_file, customers_file, output_path)

        expected_total = (2*10.0) + (1*15.0) + (3*10.0)
        assert results['total_sales'] == expected_total

    def test_find_top_product(self, tmp_path):
        """Verifica que identifique correctamente el producto más vendido"""
        orders_data = {
            'order_id': [1, 2, 3],
            'customer_id': [101, 102, 103],
            'product_id': [1, 2, 1],
            'quantity': [5, 3, 2],
            'unit_price': [10.0, 15.0, 10.0],
            'order_date': ['2024-01-01', '2024-01-01', '2024-01-01']
        }

        customers_data = {
            'customer_id': [101, 102, 103],
            'customer_name': ['A', 'B', 'C'],
            'email': ['a@email.com', 'b@email.com', 'c@email.com']
        }

        orders_file = tmp_path / "test_orders.csv"
        customers_file = tmp_path / "test_customers.csv"
        output_path = tmp_path / "output"

        pd.DataFrame(orders_data).to_csv(orders_file, index=False)
        pd.DataFrame(customers_data).to_csv(customers_file, index=False)

        results = calculate_daily_metrics(orders_file, customers_file, output_path)

        top_product = results['top_5_products'][0]
        assert top_product['product_id'] == 1
        assert top_product['quantity'] == 7

    def test_handle_empty_data(self, tmp_path):
        """Verifica que maneje correctamente datos vacíos"""
        empty_orders_data = {
            'order_id': [], 'customer_id': [], 'product_id': [],
            'quantity': [], 'unit_price': [], 'order_date': []
        }

        customers_data = {
            'customer_id': [101],
            'customer_name': ['Test'],
            'email': ['test@email.com']
        }

        orders_file = tmp_path / "empty_orders.csv"
        customers_file = tmp_path / "test_customers.csv"
        output_path = tmp_path / "output"

        pd.DataFrame(empty_orders_data).to_csv(orders_file, index=False)
        pd.DataFrame(customers_data).to_csv(customers_file, index=False)

        results = calculate_daily_metrics(orders_file, customers_file, output_path)

        assert results['total_sales'] == 0.0


class TestDataQuality:

    def test_validate_orders_data_success(self, tmp_path):
        """Verifica que la validación pase con datos correctos"""
        valid_data = {
            'order_id': [1, 2],
            'customer_id': [101, 102],
            'product_id': [1, 2],
            'quantity': [2, 1],
            'unit_price': [10.0, 15.0],
            'order_date': ['2024-01-01', '2024-01-01']
        }

        file_path = tmp_path / "valid_orders.csv"
        pd.DataFrame(valid_data).to_csv(file_path, index=False)

        assert validate_orders_data(file_path)

    def test_validate_orders_data_missing_columns(self, tmp_path):
        """Verifica que falle con columnas faltantes"""
        invalid_data = {
            'order_id': [1, 2],
            'customer_id': [101, 102]
        }

        file_path = tmp_path / "invalid_orders.csv"
        pd.DataFrame(invalid_data).to_csv(file_path, index=False)

        with pytest.raises(ValueError):
            validate_orders_data(file_path)

    def test_validate_orders_data_negative_quantity(self, tmp_path):
        """Verifica que falle con cantidades negativas"""
        invalid_data = {
            'order_id': [1, 2],
            'customer_id': [101, 102],
            'product_id': [1, 2],
            'quantity': [2, -1],
            'unit_price': [10.0, 15.0],
            'order_date': ['2024-01-01', '2024-01-01']
        }

        file_path = tmp_path / "invalid_orders.csv"
        pd.DataFrame(invalid_data).to_csv(file_path, index=False)

        with pytest.raises(ValueError):
            validate_orders_data(file_path)
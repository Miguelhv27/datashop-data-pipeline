import pandas as pd
import sys


def validate_orders_data(file_path):
    """
    Realiza validaciones de calidad en los datos de órdenes
    """
    try:
        df = pd.read_csv(file_path)

        expected_columns = ['order_id', 'customer_id', 'product_id', 'quantity', 'unit_price', 'order_date']
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Columnas faltantes: {missing_columns}")

        critical_columns = ['order_id', 'customer_id']
        for col in critical_columns:
            if df[col].isnull().any():
                raise ValueError(f"Valores nulos encontrados en {col}")

        if (df['quantity'] <= 0).any():
            raise ValueError("Valores de quantity deben ser positivos")

        if (df['unit_price'] < 0).any():
            raise ValueError("Valores de unit_price no pueden ser negativos")

        if df['order_id'].duplicated().any():
            raise ValueError("order_id duplicados encontrados")

        print("✓ Todas las validaciones de calidad pasaron correctamente")
        return True

    except Exception as e:
        print(f"✗ Error en validación de calidad: {str(e)}")
        raise e


def validate_customers_data(file_path):
    """
    Realiza validaciones de calidad en los datos de clientes
    """
    try:
        df = pd.read_csv(file_path)

        expected_columns = ['customer_id', 'customer_name', 'email']
        missing_columns = set(expected_columns) - set(df.columns)
        if missing_columns:
            raise ValueError(f"Columnas faltantes en customers: {missing_columns}")

        if df['customer_id'].isnull().any():
            raise ValueError("Valores nulos en customer_id")

        print("✓ Validaciones de clientes pasaron correctamente")
        return True

    except Exception as e:
        print(f"✗ Error en validación de clientes: {str(e)}")
        raise e


if __name__ == "__main__":
    orders_file = sys.argv[1] if len(sys.argv) > 1 else "data/input/orders_2024-01-01.csv"
    customers_file = sys.argv[2] if len(sys.argv) > 2 else "data/input/customers.csv"

    try:
        validate_orders_data(orders_file)
        validate_customers_data(customers_file)
        print("Todas las validaciones de calidad fueron exitosas")
    except Exception as e:
        print(f"Validaciones fallaron: {e}")
        sys.exit(1)
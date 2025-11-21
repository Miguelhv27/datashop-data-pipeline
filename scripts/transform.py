import pandas as pd
import os
from datetime import datetime, timedelta

def convert_to_native_types(obj):
    """Convierte tipos numpy/pandas a tipos nativos de Python para JSON"""
    if hasattr(obj, 'item'):
        return obj.item()
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        return obj

def calculate_daily_metrics(orders_file, customers_file, output_path):
    """
    Calcula las métricas diarias requeridas para el informe
    """
    try:
        orders_df = pd.read_csv(orders_file)
        customers_df = pd.read_csv(customers_file)
        
        orders_df['quantity'] = pd.to_numeric(orders_df['quantity'], errors='coerce').fillna(0)
        orders_df['unit_price'] = pd.to_numeric(orders_df['unit_price'], errors='coerce').fillna(0)
        
        orders_df['total_sale'] = orders_df['quantity'] * orders_df['unit_price']
        total_sales = float(orders_df['total_sale'].sum())
        
        product_sales = orders_df.groupby('product_id').agg({
            'quantity': 'sum',
            'total_sale': 'sum'
        }).reset_index()
        
        if len(product_sales) > 0:
            top_5_products = product_sales.nlargest(5, 'quantity')[['product_id', 'quantity', 'total_sale']]
            top_5_products['product_id'] = top_5_products['product_id'].astype(int)
            top_5_products['quantity'] = top_5_products['quantity'].astype(int)
            top_5_products['total_sale'] = top_5_products['total_sale'].astype(float)
        else:
            top_5_products = pd.DataFrame(columns=['product_id', 'quantity', 'total_sale'])
        
        customer_sales = orders_df.groupby('customer_id')['total_sale'].sum().reset_index()
        
        if len(customer_sales) > 0 and customer_sales['total_sale'].sum() > 0:
            biggest_customer_id = int(customer_sales.loc[customer_sales['total_sale'].idxmax(), 'customer_id'])
            biggest_customer_sale = float(customer_sales['total_sale'].max())
            
            customer_match = customers_df[customers_df['customer_id'] == biggest_customer_id]
            if len(customer_match) > 0:
                biggest_customer_name = str(customer_match['customer_name'].iloc[0])
            else:
                biggest_customer_name = "Cliente no encontrado"
        else:
            biggest_customer_id = None
            biggest_customer_sale = 0.0
            biggest_customer_name = "Sin compras"
        
        results = {
            'report_date': datetime.now().strftime('%Y-%m-%d'),
            'total_sales': round(total_sales, 2),
            'biggest_customer': {
                'customer_id': biggest_customer_id,
                'customer_name': biggest_customer_name,
                'total_spent': round(biggest_customer_sale, 2)
            },
            'top_5_products': [
                {
                    'product_id': int(row['product_id']),
                    'quantity': int(row['quantity']),
                    'total_sale': round(float(row['total_sale']), 2)
                }
                for _, row in top_5_products.iterrows()
            ]
        }
        
        os.makedirs(output_path, exist_ok=True)
        
        output_file = os.path.join(output_path, f"daily_report_{datetime.now().strftime('%Y-%m-%d')}.json")
        
        import json
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        if len(top_5_products) > 0:
            top_products_file = os.path.join(output_path, f"top_products_{datetime.now().strftime('%Y-%m-%d')}.csv")
            top_5_products.to_csv(top_products_file, index=False)
        
        return results
        
    except Exception as e:
        print(f"Error en transformación: {str(e)}")
        raise e

if __name__ == "__main__":
    orders_file = "data/input/orders_2024-01-01.csv"
    customers_file = "data/input/customers.csv"
    output_path = "data/output/"
    
    results = calculate_daily_metrics(orders_file, customers_file, output_path)
    print("Transformación completada:")
    print(f"Ventas totales: ${results['total_sales']:,.2f}")
    print(f"Cliente destacado: {results['biggest_customer']['customer_name']}")
    print(f"Monto gastado: ${results['biggest_customer']['total_spent']:,.2f}")
    print("Top 5 productos:")
    for i, product in enumerate(results['top_5_products'], 1):
        print(f"  {i}. Producto {product['product_id']}: {product['quantity']} unidades")
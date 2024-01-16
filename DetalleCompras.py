import requests
import csv
import os
from datetime import datetime, timedelta
import pymysql


cantidades_Portfolio={
    'BTC':0.2501872659,
    'ETH':0.2501872659,
    'SOL':0.125093633,
    'MATIC':0.03745318352,
    'DOT':0.03745318352,
    'LINK':0.03745318352,
    'RNDR':0.03745318352,
    'WOO':0.03745318352,
    'HBAR':0.03745318352,
    'GRT':0.03745318352,
    'INJ':0.03745318352,
    'ROSE':0.03745318352,
    'FET':0.03745318352
}

def obtener_top500(api_key):
    url = 'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
    parameters = {'limit': 500, 'sort': 'market_cap', 'convert': 'USD'}
    headers = {'Accepts': 'application/json', 'X-CMC_PRO_API_KEY': api_key}

    response = requests.get(url, headers=headers, params=parameters)

    if response.status_code == 200:
        data = response.json()
        return data['data']
    else:
        print(f"Error {response.status_code}: {response.text}")
        return None

def filtrar_activos(data, symbols):
    return [crypto for crypto in data if crypto['symbol'] in symbols]

def calcular_cantidad_comprada(cantidad_diaria, precio_actual):
    return cantidad_diaria / precio_actual

def guardar_en_csv(data, filename, cantidad_diaria):
    fieldnames = ['Nombre', 'Símbolo', 'Precio (USD)', 'Market Cap (USD)', 'Cantidad Gastada (USD)', 'Cantidad Comprada']

    with open(filename, 'w', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for crypto in data:
            cantidad_gastada = cantidad_diaria*cantidades_Portfolio[crypto['symbol']]
            cantidad_comprada = calcular_cantidad_comprada(cantidad_gastada, crypto['quote']['USD']['price'])
            
            print('##########################################')
            print('          ','Simbolo:',crypto['symbol'])
            print('          ','Cantidad comprada:',cantidad_comprada)
            print('          ','Cantidad gastada:',cantidad_gastada)
            print('##########################################\n')

            writer.writerow({
                'Nombre': crypto['name'],
                'Símbolo': crypto['symbol'],
                'Precio (USD)': crypto['quote']['USD']['price'],
                'Market Cap (USD)': crypto['quote']['USD']['market_cap'],
                'Cantidad Gastada (USD)': cantidad_gastada,
                'Cantidad Comprada': cantidad_comprada,
            })

def conexion_bd():
    datos_conexion = os.getenv("DB_COMPRAS").split(';')
    conexion = pymysql.connect(
        host=datos_conexion[0],
        user= datos_conexion[1],
        database= datos_conexion[2],
        password=datos_conexion[3]
    )
    
    cursor = conexion.cursor()
    data_conexion=[cursor,conexion,datos_conexion[4]]
    return data_conexion

def insertar_compras(data_conexion,data,cantidad_diaria):

    cursor=data_conexion[0]
    conexion=data_conexion[1]
    tabla = data_conexion[2]
    for crypto in data:
        cantidad_gastada = cantidad_diaria*cantidades_Portfolio[crypto['symbol']]
        cantidad_comprada = calcular_cantidad_comprada(cantidad_gastada, crypto['quote']['USD']['price'])

        consulta = f"""INSERT INTO {tabla} (idCompras, NombreActivo, Simbolo, Precio, MarketCap, CantidadGastada, CantidadComprada) 
                   VALUES (NULL, '{crypto['name']}', '{crypto['symbol']}', {crypto['quote']['USD']['price']}, {crypto['quote']['USD']['market_cap']}, {cantidad_gastada}, {cantidad_comprada})"""

        try:
            cursor.execute(consulta)
            conexion.commit()

        except pymysql.Error as e:
            print(f"Error: {e}")

    cursor.close()
    conexion.close()


def main():
    data_conexion=conexion_bd()
    api_key = os.getenv("API_KEY_COINMARKET")
    symbols = cantidades_Portfolio.keys()

    cantidad_diaria = 5.34

    top500 = obtener_top500(api_key)

    if top500 is not None:
        criptomonedas_filtradas = filtrar_activos(top500, symbols)

        # Guarda la información filtrada en un archivo CSV (OPCION 1)
        #guardar_en_csv(criptomonedas_filtradas, 'criptomonedas_filtradas.csv', cantidad_diaria)

        # Guarda la información filtrada en una tabla (OPCION 2)
        insertar_compras(data_conexion,criptomonedas_filtradas,cantidad_diaria)

if __name__ == "__main__":
    main()
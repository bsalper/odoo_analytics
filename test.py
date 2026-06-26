## Ejercicio 3:

cadena = "Hola, como:x:estas; esto es una prueba. Hola"
palabra_actual = ""
diccionario = {}

for letra in cadena:
    if letra.isalnum():
        palabra_actual += letra
    else:
        if palabra_actual != "":
            palabra_actual = palabra_actual.lower()
            if palabra_actual in diccionario:
                diccionario[palabra_actual] += 1
            else:
                diccionario[palabra_actual] = 1
            palabra_actual = ""

print(diccionario)


## Ejercicio 4: palidromo

cadena = "Anita lava la tina"
cadena_limpia = ""
for letra in cadena:
    if letra.isalnum():
        cadena_limpia += letra
cadena_limpia = cadena_limpia.lower()
es_palindromo = cadena_limpia == cadena_limpia[::-1]
print(es_palindromo)



# Ejercicio 5

cadena = "Anita lava la tina"
cadena_limpia = ""
frase_invertida = ""

for letra in cadena:
    if letra.isalnum():
        letra = letra.lower()
        cadena_limpia += letra
        frase_invertida = letra + frase_invertida

es_palindromo = cadena_limpia == frase_invertida

print(es_palindromo)


#Ejercicio 6

cadena = "Python123 es genial, pero Python3.8 es mejor que Python2!"
cadena_limpia = ""

for letra in cadena:
    if letra.isalnum():
        letra = letra.lower()
        cadena_limpia += letra

print(cadena_limpia)


#Ejercicio 7

palabra= ["rojo", "azul", "rojo", "verde", "azul", "rojo"]
diccionario = {}
mayor = 0

for color in palabra:
    if color in diccionario:
        diccionario[color] += 1
    else:
        diccionario[color] = 1
        
for color in diccionario:
    if diccionario[color] > mayor:
        mayor = diccionario[color]
        color_mayor = color

    print("El color más repetido es:", color_mayor)


#Ejercicio 8

numeros = [10, 15, 10, 20, 15, 10, 30, 20, 20]
diccionario = {}
mayor = 0

for numero in numeros:

    if numero in diccionario:
        diccionario[numero] += 1
    else:
        diccionario[numero] = 1

for numero in diccionario:
    if diccionario[numero] > mayor:
        mayor = diccionario[numero]
        numero_mayor = numero

print("El número más repetido es:", numero_mayor)
print("Números que se repiten")

for numero in diccionario:
    if diccionario[numero] > 1:
        print(numero, "se repite", diccionario[numero], "veces")


#ejercicio 9

cadena = "El sol brilla y el sol calienta la tierra mientras el sol se oculta"
palabra_actual = ""
diccionario = {}
palabra_larga = ""

for letra in cadena:
    if letra.isalpha():
        palabra_actual += letra.lower()
    else:
        if palabra_actual != "":
            if palabra_actual in diccionario:
                diccionario[palabra_actual] += 1
            else:
                diccionario[palabra_actual] = 1
            palabra_actual = ""

print(diccionario)

for palabra in diccionario:
    if diccionario[palabra] > 1:
        if len(palabra) > len(palabra_larga):
            palabra_larga = palabra

print("La palabra más larga que se repite es:", palabra_larga)


## Ejercicio 10
cadena = "casa perro gato casa perro sol luna gato gato"
diccionario = {}
palabra_actual = ""

for letra in cadena:
    if letra.isalpha():
        palabra_actual += letra
    else:
        if palabra_actual != "":
            if palabra_actual in diccionario:
                diccionario[palabra_actual] += 1
            else:
                diccionario[palabra_actual] = 1
            palabra_actual = ""

print(diccionario)

mayor = 0
palabra_maor = ""

for palabra in diccionario:
    if diccionario[palabra] > mayor:
        mayor = diccionario[palabra]
        palabra_mayor = palabra

print("La palabra más repetida es:", palabra_mayor)

## Ejericcio 11

ventas = [
    {"cliente": "Ana", "monto": 1200, "region": "Norte"},
    {"cliente": "Luis", "monto": 800, "region": "Sur"},
    {"cliente": "Pedro", "monto": 1500, "region": "Norte"},
    {"cliente": "Sofia", "monto": 400, "region": "Centro"},
    {"cliente": "Camila", "monto": 2000, "region": "Sur"}
]
contador_regiones = {}

for venta in ventas:
    if venta["monto"] > 1000:
        print(venta["cliente"], venta["monto"])

    region = venta["region"]

    if region in contador_regiones:
        contador_regiones[region] += 1
    else:
        contador_regiones[region] = 1

print(contador_regiones)

## Ejercicio 12

usuarios = [
    {"nombre": "Ana", "edad": 17, "activo": True},
    {"nombre": "Luis", "edad": 25, "activo": False},
    {"nombre": "Camila", "edad": 30, "activo": True},
    {"nombre": "Pedro", "edad": 15, "activo": True},
    {"nombre": "Sofia", "edad": 22, "activo": False}
]

sum_edades = 0
cantidad_usuarios = 0

for usuario in usuarios:
    if usuario["edad"] >= 18:
        print(usuario["nombre"], usuario["edad"])

    if usuario["activo"] == True:
        print(usuario["nombre"], "está activo")

    sum_edades += usuario["edad"]
    cantidad_usuarios += 1
    promedio = sum_edades / cantidad_usuarios


print("el promedio de edad es: ", promedio)

#ejercicio 13

pedidos = [
    {"cliente": "Ana", "total": 15000, "estado": "entregado"},
    {"cliente": "Luis", "total": 8000, "estado": "pendiente"},
    {"cliente": "Camila", "total": 22000, "estado": "entregado"},
    {"cliente": "Pedro", "total": 12000, "estado": "cancelado"},
    {"cliente": "Sofia", "total": 30000, "estado": "entregado"}
]
print ("==========================")
print ("Pedidos mayores a 10.000")
print ("==========================")

for pedido in pedidos:
    if pedido["total"] > 10000:
        print(pedido["cliente"], pedido["total"])

print ("==========================")
print ("Pedidos entregados")
print ("==========================")

for pedido in pedidos:
    if pedido["estado"] == "entregado":
        print("El pedido de", pedido["cliente"], "esta entregado")

print ("==========================")
print ("Pedidos por estado")
print ("==========================")
        
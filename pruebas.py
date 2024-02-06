texto = "Hola_Mundo"
caracter = "_"

posicion_desde_el_final = texto.rfind(caracter)

if posicion_desde_el_final != -1:
    print(f"La posición del carácter '{caracter}' desde el final es: {posicion_desde_el_final}")
else:
    print(f"El carácter '{caracter}' no se encontró en la cadena.")
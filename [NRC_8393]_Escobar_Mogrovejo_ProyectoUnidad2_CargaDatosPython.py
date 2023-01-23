#-----------------CARGAR DE DATOS PYTHON-----------------
# Para la conexión con la base de datos
import oracledb
# Para lectura de los CSV
import csv
# Datos necesarios para la conexión
p_username = "PROYECTODB"
p_password = "password"
p_dns = "localhost/xe"
p_port = "1521"

# Función de conexión y comprobación
con = oracledb.connect(user=p_username, password=p_password, dsn=p_dns, port=p_port)

#---Eliminaciones--- 
# Para evitar conflictos al ejecutar varias veces el código
# Iniciamos la variable para ejecutar la sentencia
cur = con.cursor()
# Borramos cada una de las tablas
cur.execute('delete from "TBClubJugador"')
cur.execute('delete from "TBJugConvocado"')
cur.execute('delete from "TBDirectivo"')
cur.execute('delete from "TBUsuario"')
cur.execute('delete from "TBAgFinanciero"')
cur.execute('delete from "TBEmpleado"')
cur.execute('delete from "TBClub"')
cur.execute('delete from "TBConfederacion"')
cur.execute('delete from "TBSeleccion"')
cur.execute('delete from "TBPais"')
cur.execute('delete from "TBContinente"')
cur.execute('delete from "TBParametro"')
cur.execute('delete from "TBPersona"')
cur.execute('delete from "TBMundial"')
cur.execute('delete from TBFIFA')
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION FIFA------------------------
# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_FIFA.csv", "r"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Lazo for que insertará el dato a la tabla en el que se encuentre
print("Insercion 1")
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = "insert into TBFIFA values (:1, :2)"
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga FIFA---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION MUNDIAL------------------------
# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
# Guardamos y unimos en una cadena el dato a insertar
Mundial = 'INSERT INTO "TBMundial" VALUES'
datos1 = "('2022Qatar',TO_DATE('20/Nov/2022'),TO_DATE('18/Dec/2022'))"
final = Mundial + datos1
print("Insercion data 2")
# Ejecutamos la sentencia
cur.execute(final)
# Cerramos la función de conexión
cur.close()

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
# Guardamos y unimos en una cadena el dato a insertar
Mundial = 'INSERT INTO "TBMundial" VALUES'
datos1 = "('2018Russia',TO_DATE('14/Jun/2018'),TO_DATE('15/Jul/2018'))"
final = Mundial + datos1
# Ejecutamos la sentencia
cur.execute(final)
print("---Carga Mundial 2---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION CONTINENTE------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Continentes.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion data 3")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBContinente" values (:1, :2, :3, :4)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Continentes---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION PAIS------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Paises.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
cur.execute('delete from "TBPais"')
print("Insercion 4")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBPais" values (:1, :2, :3, :4, :5)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Paises---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION SELECCION------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Seleccion.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 5")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBSeleccion" values (:1, :2, :3, :4, :5)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Seleccion---")
# Cerramos la función de conexión
cur.close()


# ------------------------CREACION CONFEDERACION------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Confederaciones.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 6")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBConfederacion" values (:1, :2, :3)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Confederacion---")
# Cerramos la función de conexión
cur.close()


# ------------------------CREACION CLUB------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Club.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 7")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBClub" values (:1, :2, :3, :4, :5)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Club---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION PARAMETRO------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Parametros.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 8")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBParametro" values (:1, :2, :3, :4)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Parametro---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION PERSONA------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Personas.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 9")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBPersona" values (:1, :2, :3, :4)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Persona---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION EMPLEADO------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Empleados.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 10")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBEmpleado" values (:1, :2, :3)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Empleado---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION AGENTE FINANCIERO------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_AgentesFinanciero.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 11")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBAgFinanciero" values (:1, :2, :3)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga AgFinanciero---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION USUARIO------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Usuarios.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 12")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBUsuario" values (:1, :2)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Usuario---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION DIRECTIVO------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_Directivos.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 13")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBDirectivo" values (:1, :2)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga Directivo---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION JUGCONVOCADO------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_JugadorConvocado.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 14")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBJugConvocado" values (:1, :2, :3, :4)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga JugConvocado---")
# Cerramos la función de conexión
cur.close()

# ------------------------CREACION CLUBJUGADOR------------------------
# En una variable guardamos todo lo que contenga el archivo csv
reader = csv.reader(open("C:\sqlite\db\CsvProyect\dataset_ClubJugador.csv", errors="ignore"))
# Guardaremos en un arreglo las columnas
columns = []
# Lazo for que recorrerá cada parte de la variable que leyó el archivo
for line in reader:
    columns.append(line)
print("Leido")

# Iniciamos la variable para ejecutar la Insercion
cur = con.cursor()
print("Insercion 15")
# Lazo for que insertará el dato a la tabla en el que se encuentre
for line in columns:
    # Guardamos en una cadena la línea de código que insertará el dato
    insrt_stmt = 'insert into "TBClubJugador" values (:1, :2, :3, :4, :5, :6)'
    # Ejecutamos la sentencia
    cur.execute(insrt_stmt, line)
#Guardamos la carga del dato
con.commit()
print("---Carga ClubJugador---")
# Cerramos la función de conexión
cur.close()

print("CARGA COMPLETA")
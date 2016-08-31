# Rest-Spark
REST + Apache Spark

Para generar el proyecto, entrar en una terminal en la carpeta del proyecto y utlizar el comando:

*mvn package*

##App

###main

Genera las clases '_LinReg_' y '_LoadDriver_'
Levanta el servidor (SparkFramework, no tiene nada que ver con Apache Spark) y espera peticiones en en http://localhost:4567/pred 
Cuando recibe una peticion ejecuta '_LoadDriver_' y '_LinReg_'

##LinReg  
####Apache Spark

###start

Configura Apache Spark Core con nombre de la aplicacion, uso local y cuantas hebras puede utilizar, y configura el PATH local de la app para coger los ficheros.
Configura Apache Spark SQL.

###stop

Detiene el proceso de Apache Spark.

###makePrediction(String nombreFichero)


1. Lee el fichero que se le pasa en formato JSON y genera un Dataset. (spark.read())
2. Selecciona las columnas que va a utilizar y filtra los resultados para eliminar los resultados nulos, vacios del Dataset. (select().filter())
3. Convierte el Dataset a un array. (collectAsList())
4. Selecciona el numero de comentarios para utilizar en la regresion, prediccion. (numComment -> ahora mismo en la mitad, 1/2) 
5. Crea un nuevo Dataset a partir del array modificado con el formato para realizar la prediccion (LabeledPoint -> Vector[], Double, Double...) (RowFactory.create())
6. Divide el nuevo Dataset aleatoriamente en 2 partes. Una 70% y otra 30%. 70 para entrenar el modelo, y 30 para validar los datos. (randomSplit(0.70,0.30))
7. Convierte el Dataset de validacion de datos en un array y le anade los puntos para predicir (commentsToAdd) y vuelve a generar el Dataset.
8. Genera la Regresion Lineal utilizando un modelo Gaussiano con una penalizacion del 0.05. (new GeneralizedLinearRegression())
9. Define 3 modelos, utilizando en cada modelo una columna de datos diferente. Positiva, Neutral y Negativa. (ParamMap)
10. Entrena el modelo. (fit)
11. Valida los datos y genera la prediccion. (transform)
12. Anade una columna nueva con la suma de las predicciones, y genera el Dataset final con todas las columnas filtrando para obtener solo las predicciones.
13. Escribe los resultados en la base de datos (write)


##LoadDriver

1. Conecta con la base de datos (DriverManager.getConnection)
2. Ejecuta la primera 'query' para identificar los diferentes usuarios de la BBDD.
3. Para cada usuario se ejecuta una nueva 'query' con los datos filtrados, se formatea en formato JSON y se genera un fichero .json
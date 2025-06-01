---
applyTo: '**'
---
La idea es que el collector vaya recogiendo datos de la bd , estos datos se pasan al data analyzer , que parsea los datos a objetos custom y se los pasa al snapshot repository , dichos objetos se guardaran en una sqlite para usarse posteriormente. El core se suscribira al data analyzer con un patron observer , cuando pasen nuevos datos y los guarde en la sqlite se mandara una notificacion con los nuevos datos al core . 

## Qué función va a tener el core?

El core va a ser el punto central del sistema. Será capaz de enviar eventos a otros subsistemas , como una cola de mesajes email , el GUI ...


## Cual será el flujo ?

Al inciarse el programa , el core le pedira todos los datos existentes de la sqlite al data analyzer y se los pasará al adaptador de la GUI para que los cargue y los muestre . Mi idea es que la gui tenga varios metodos , una para recibir todos los datos (carga inicial ) y otro para añadir datos nuevos a los que ya estaban cargados , así evito pasar todos los datos cada vez que ser refresque algo .


import discord
from discord.ext import commands
from discord import app_commands
import json
import re
import logging
from datetime import datetime
import os
import sys
import dotenv
from dotenv import load_dotenv
import asyncio
import time
import sqlite3
from sqlite3 import Error

# Configuración de logging
logging.basicConfig(level=logging.DEBUG, filename='bot.log', filemode='a', format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

# Lista de claves requeridas
required_keys = [
    "DISCORD_TOKEN", "APPLICATION_ID", "CHANNEL_DEFAULT", "CHANNEL_ADMIN", "THREAD_STATS_NAME", "ROL_JUGADORES",
    "ROL_ADMINS", "VOICE_CHANNEL_ID","VOICE_CHANNEL_RESERVAS_ID", "MAX_PLAYERS", "MAX_TIME", "SEND_MESSAGES"
]
# Verificar que todas las claves existen y tienen valor
missing_keys = [key for key in required_keys if not os.environ.get(key)]
if missing_keys:
    print(f"Error: Faltan los siguientes parametros por configurar: {', '.join(missing_keys)}")
    sys.exit(1)  # Salir del script con error

# Si todas las claves están presentes, cargar la configuración
config = {
    'token': os.environ['DISCORD_TOKEN'],
    'application_id': os.environ['APPLICATION_ID'],
    'channel_default': os.environ['CHANNEL_DEFAULT'],
    'channel_admin': os.environ['CHANNEL_ADMIN']
}

discord.VoiceClient.warn_nacl = False
intents = discord.Intents.default()
intents.members = True  # Necesario para fetch_members()
intents.message_content = True
client = discord.Client(intents=intents)
application_id = int(config['application_id'])
bot = commands.Bot(command_prefix='/', intents=intents, application_id=application_id)

# Variables globales
ROL_ID_JUGADORES = int(os.environ['ROL_JUGADORES'])
ROL_ID_ADMINS = int(os.environ['ROL_ADMINS'])
VOICE_CH_ID = int(os.environ['VOICE_CHANNEL_ID'])
VOICE_CHR_ID = int(os.environ['VOICE_CHANNEL_RESERVAS_ID'])
MAX_JUGADORES = int(os.environ['MAX_PLAYERS'])
MAX_JUGADORES_LISTAS = MAX_JUGADORES * 2
MAX_TIME_LIST = int(os.environ['MAX_TIME'])
THREAD_STATS_NAME = os.environ['THREAD_STATS_NAME']
miembros_lista = {}
miembros_objetos = {}
embed_main_message = None
embed_reservas_message = None
lista_cerrada = True
tarea_cerrar_lista = None
tiempo_inicio_lista = None
send_messages = os.getenv("SEND_MESSAGES", "true").lower() == "true"
adding_players = set()
adding_lock = asyncio.Lock()
ultima_actualizacion_embed = 0  # Timestamp de la última actualización del embed
embed_update_lock = asyncio.Lock()  # Bloqueo para controlar actualizaciones del embed

#################################################################################################

# Obtén la ruta del directorio donde se encuentra el script
current_directory = os.path.dirname(os.path.abspath(__file__))
# Ruta completa del archivo de base de datos
database_file = os.path.join(current_directory, "control_eventos.db")

def sql_connection():
    try:
        con = sqlite3.connect(database_file)
        return con
    except sqlite3.Error as error:
        print(f"Error al conectar a la base de datos: {error}")
        return None

def sql_fetch(query, params=None):
    con = None
    try:
        con = sql_connection()
        if not con:
            raise sqlite3.Error("No se pudo establecer la conexión a la base de datos")
        cursor = con.cursor()
        cursor.execute(query, params or ())
        result = cursor.fetchall()
        cursor.close()
        return result
    except sqlite3.Error as error:
        print(f"Error al ejecutar consulta de lectura: {error}")
        return []
    finally:
        if con:
            con.close()

def sql_update(query, params=None):
    con = None
    try:
        con = sql_connection()
        if not con:
            raise sqlite3.Error("No se pudo establecer la conexión a la base de datos")
        cursor = con.cursor()
        cursor.execute(query, params or ())
        con.commit()
        cursor.close()
    except sqlite3.Error as error:
        print(f"Error al ejecutar consulta de actualización: {error}")
    finally:
        if con is not None:  # Añadir esta verificación
            con.close()

#################################################################################################

@bot.event
async def on_ready():
    print('We have logged in as {0.user}'.format(bot))
    # Iniciar el check periódico para actualizar miembros conectados
    bot.loop.create_task(comprobar_conectados_periodicamente())
    bot.loop.create_task(borrar_mensajes_sin_embed())

#################################################################################################

@bot.event
async def on_message(message):
    global ROL_ID_ADMINS

    if message.author == bot.user:
        return

    # Obtener el canal desde la configuración
    channel = bot.get_channel(int(config['channel_default']))
    
    # Verificar si el mensaje no es del canal correcto
    if message.channel != channel:
        return

    # Verificar si el autor tiene el rol necesario
    if ROL_ID_ADMINS not in [role.id for role in message.author.roles]:
        print(f"{message.author.display_name} no tiene el rol necesario. Mensaje ignorado.")
        return  # No hacer nada si el autor no tiene el rol

    # Si el mensaje empieza con 'ping', responder 'Pong'
    if message.content.startswith('ping'):
        await message.channel.send('Pong')

    # Procesar otros comandos si los hay
    await bot.process_commands(message)

#################################################################################################

async def eliminar_jugadores_no_en_lista(channel_or_ctx):
    global miembros_lista, miembros_objetos

    # Obtener el guild dependiendo del tipo de entrada
    guild = channel_or_ctx.guild if hasattr(channel_or_ctx, 'guild') else channel_or_ctx
    miembros_objetos = {member.display_name: member for member in guild.members if ROL_ID_JUGADORES in [role.id for role in member.roles]}

    jugadores_en_lista = set(miembros_lista.keys())
    jugadores_a_eliminar = [jugador for jugador in miembros_objetos if jugador not in jugadores_en_lista]

    for jugador in jugadores_a_eliminar:
        del miembros_objetos[jugador]

#################################################################################################

@bot.command()
async def NewList(ctx):
    global miembros_lista, miembros_objetos, embed_main_message, embed_reservas_message, lista_cerrada, tiempo_inicio_lista, tarea_cerrar_lista, send_messages

    miembros_lista = {}
    miembros_objetos = {}
    embed_main_message = None
    embed_reservas_message = None
    lista_cerrada = False
    tiempo_inicio_lista = time.time()
    tarea_cerrar_lista = None  # Reiniciar tarea_cerrar_lista aquí
    
    await ctx.send("✍ Introduce los elementos de la lista. Escribe `FIN` para terminar.")
    bot.loop.create_task(cerrar_lista(MAX_TIME_LIST))  # Esto asignará tarea_cerrar_lista

    connected_members = {member.display_name: member for guild in bot.guilds for channel in guild.voice_channels for member in channel.members}
    print(f"Miembros conectados en voz: {connected_members}")

    while True:
        try:
            msg = await bot.wait_for('message', check=lambda m: m.author == ctx.author, timeout=60)
            if msg.content.strip().upper() == "FIN":
                break
            nuevos_miembros = set()
            lista_temporal = {}
            for miembro in msg.content.splitlines():
                miembro = miembro.strip()
                if miembro:
                    if miembro in miembros_lista or miembro in nuevos_miembros:
                        await ctx.send(f"⚠️ `{miembro}` ya está en la lista. Envía la lista nuevamente sin duplicados.")
                        break
                    nuevos_miembros.add(miembro)
                    if miembro in connected_members:
                        lista_temporal[miembro] = "si"
                        miembros_objetos[miembro] = connected_members[miembro]
                        print(f"Se ha añadido correctamente el jugador: {miembro}")
                    else:
                        lista_temporal[miembro] = "no"
            else:
                miembros_lista.update(lista_temporal)
                await msg.delete()
                continue
        except asyncio.TimeoutError:
            await ctx.send("⏳ Tiempo de espera agotado. Operación cancelada.")
            return

    if not miembros_lista:
        await ctx.send("⚠️ No has introducido ningún miembro. Por favor, vuelve a intentarlo con `/NewList`.")
        return

    await eliminar_jugadores_no_en_lista(ctx)
    print(f"miembros_objetos con ROL: `{miembros_objetos}`")
    await actualizar_embeds(ctx)
    if send_messages:
        await enviar_mensajes_privados(ctx)
    else:
        print("Los mensajes privados están desactivados.")

#################################################################################################

async def actualizar_embeds(channel_or_ctx):
    global embed_main_message, embed_reservas_message
    
    # Determinar el canal para enviar mensajes
    if isinstance(channel_or_ctx, discord.TextChannel):
        channel = channel_or_ctx
    elif isinstance(channel_or_ctx, discord.Thread):
        channel = channel_or_ctx.parent  # Obtener el canal padre del hilo
    elif isinstance(channel_or_ctx, commands.Context):
        channel = channel_or_ctx.channel  # Para contextos de comandos
    elif isinstance(channel_or_ctx, discord.ForumChannel):
        # Opcional: Si el canal es un foro, intentar usar un hilo predeterminado
        # Aquí asumimos que quieres usar un hilo específico; ajusta según tu caso
        threads = channel_or_ctx.threads
        if threads:
            channel = threads[0]  # Usar el primer hilo del foro (ajusta según tu lógica)
        else:
            print("Error: No se encontraron hilos en el ForumChannel.")
            return
    else:
        print(f"Error: Tipo de canal no soportado: {type(channel_or_ctx)}")
        channel = bot.get_channel(int(config['channel_default']))
    
    embed_main, embed_reservas = generar_embeds()
    
    if embed_main_message:
        await embed_main_message.edit(embed=embed_main)
    else:
        embed_main_message = await channel.send(embed=embed_main)
    
    if embed_reservas:
        if embed_reservas_message:
            await embed_reservas_message.edit(embed=embed_reservas)
        else:
            embed_reservas_message = await channel.send(embed=embed_reservas)

#################################################################################################

def generar_embeds(miembros_lista=None, max_jugadores=None, fecha_lista=None, is_historico=False):
    """
    Genera embeds para listas de jugadores, tanto en tiempo real como historicas.
    Usa 'si' y 'no' para estados de conexion.

    Args:
        miembros_lista (dict, optional): Diccionario con jugadores y estados.
        max_jugadores (int, optional): Numero maximo de jugadores principales.
        fecha_lista (str, optional): Fecha de la lista (YYYY-MM-DD HH:MM:SS).
        is_historico (bool): Si True, muestra lista cerrada sin tiempo restante.

    Returns:
        tuple: (embed_main, embed_reservas) o (embed_main, None).
    """
    global MAX_JUGADORES, MAX_TIME_LIST, tiempo_inicio_lista

    # Usar valores globales si no se proporcionan
    miembros_lista = miembros_lista or globals().get('miembros_lista', {})
    max_jugadores = max_jugadores or MAX_JUGADORES

    # Crear los embeds
    miembros_principales = list(miembros_lista.items())[:max_jugadores]
    miembros_reservas = list(miembros_lista.items())[max_jugadores:]
    
    embed_main = discord.Embed(title="📋 Lista de Jugadores", color=discord.Color.blue())
    embed_reservas = discord.Embed(title="📝 Reservas", color=discord.Color.orange()) if miembros_reservas else None
    
    # Procesar jugadores principales
    if miembros_principales:
        numeros, nombres, estados = [], [], []
        for index, (miembro, estado) in enumerate(miembros_principales, 1):
            numeros.append(str(index))
            nombres.append(miembro)
            is_connected = estado == "si"
            estados.append(f"{'🟢' if is_connected else '🔴'} {'Conectado' if is_connected else 'Desconectado'}")
        
        embed_main.add_field(name="#️⃣ Nº", value="\n".join(numeros) or "N/A", inline=True)
        embed_main.add_field(name="👤 Nombre", value="\n".join(nombres) or "N/A", inline=True)
        embed_main.add_field(name="🔹 Estado", value="\n".join(estados) or "N/A", inline=True)
    
    # Procesar reservas
    if miembros_reservas:
        numeros, nombres, estados = [], [], []
        for index, (miembro, estado) in enumerate(miembros_reservas, max_jugadores + 1):
            numeros.append(str(index))
            nombres.append(miembro)
            is_connected = estado == "si"
            estados.append(f"{'🟢' if is_connected else '🔴'} {'Conectado' if is_connected else 'Desconectado'}")
        
        embed_reservas.add_field(name="#️⃣ Nº", value="\n".join(numeros) or "N/A", inline=True)
        embed_reservas.add_field(name="👤 Nombre", value="\n".join(nombres) or "N/A", inline=True)
        embed_reservas.add_field(name="🔹 Estado", value="\n".join(estados) or "N/A", inline=True)
    
    # Contar jugadores conectados y desconectados
    total_si = sum(1 for estado in miembros_lista.values() if estado == "si")
    total_no = sum(1 for estado in miembros_lista.values() if estado == "no")

    # Configurar el pie de pagina
    if is_historico:
        try:
            fecha_formateada = datetime.strptime(fecha_lista, "%Y-%m-%d %H:%M:%S").strftime("%H:%M %d-%m-%Y")
        except (ValueError, TypeError):
            fecha_formateada = fecha_lista or "Desconocida"
        footer_text = f"⛔ Lista Cerrada\n📅 Fecha de la partida: {fecha_formateada}\n🟢 Conectados: {total_si} | 🔴 Desconectados: {total_no}"
    else:
        if tiempo_inicio_lista is None:
            tiempo_restante_texto = "⏳ Tiempo no disponible"
        else:
            tiempo_transcurrido = time.time() - tiempo_inicio_lista
            tiempo_restante = max(0, MAX_TIME_LIST - tiempo_transcurrido)
            minutos_restantes = int(tiempo_restante) // 60
            segundos_restantes = int(tiempo_restante) % 60
            tiempo_restante_texto = f"⏳ La lista se cerrará en {minutos_restantes}m {segundos_restantes}s"
        fecha_formateada = datetime.now().strftime("%H:%M %d-%m-%Y")
        footer_text = f"{tiempo_restante_texto}\n📅 Fecha de la partida: {fecha_formateada}\n🟢 Conectados: {total_si} | 🔴 Desconectados: {total_no}"
    
    embed_main.set_footer(text=footer_text)
    
    return embed_main, embed_reservas

#################################################################################################

async def enviar_mensajes_privados(ctx):
    global miembros_lista, miembros_objetos, ROL_ID_JUGADORES  # Asegúrate de incluir ROL_ID_JUGADORES en las variables globales
    
    # Lista de miembros desconectados
    desconectados = [miembro for miembro, estado in miembros_lista.items() if estado == "no"]
    
    for miembro in desconectados:
        try:
            # Verificar si el miembro está desconectado en cada iteración
            if miembros_lista.get(miembro) != "no":  # Si el miembro ya está conectado, saltar al siguiente
                print(f"{miembro} ya está conectado. No se enviará mensaje.")
                continue  # Saltar al siguiente miembro

            # Verificar si el miembro está en la lista de miembros con rol
            member_obj = miembros_objetos.get(miembro)
            
            if not member_obj:  # Si el miembro no tiene el rol, no enviar el mensaje
                print(f"{miembro} no tiene el rol adecuado. No se enviará mensaje.")
                continue  # Saltar al siguiente miembro si no tiene el rol adecuado

            # Intentar enviar el mensaje privado solo si el miembro tiene el rol
            print(f"Intentando enviar mensaje privado a {miembro}")  # Mensaje de depuración

            embed_msg = discord.Embed(
                title=f"**{ctx.guild.name}**",
                description="**¡Atención!**\n📢 La reunión para la partida ha comenzado.\n"
                            "⏳ Conéctate cuanto antes para no quedarte fuera.\n\n"
                            ":boom: Únete al canal de voz lo antes posible para no perderte la acción. ¡Te esperamos! :boom:",
                color=discord.Color.red()
            )
            embed_msg.add_field(name="🎤 Canal de voz:", value=f"<#{VOICE_CH_ID}>", inline=False)  # Canal de voz
            embed_msg.add_field(name="🔔 Tu estado actual:", value="Desconectado :x:", inline=False)
            embed_msg.add_field(name="📌 Consulta tu estado en:", value=f"<#{int(config['channel_default'])}>", inline=False)  # Nueva línea con el canal
            embed_msg.set_footer(text=f"Enviado a las {datetime.now().strftime('%H:%M:%S del %d-%m-%Y')}")
            
            await member_obj.send(embed=embed_msg)  # Enviar mensaje privado
            print(f"Mensaje privado enviado a {miembro}")  # Confirmación de envío
            await asyncio.sleep(5)  # Esperar 5 segundos entre cada mensaje

        except discord.Forbidden:
            print(f"⚠️ No se pudo enviar mensaje privado a {miembro}. Permisos denegados.")

#################################################################################################

@bot.command(name="AddPlayers")
async def add_players(ctx, member=None, modo="manual"):
    global miembros_lista, miembros_objetos

    # Determinar si ctx es un canal, un hilo o un contexto de comando
    try:
        if isinstance(ctx, discord.TextChannel):
            channel = ctx
        elif isinstance(ctx, discord.Thread):
            channel = ctx.parent  # Usar el canal padre del hilo
        elif isinstance(ctx, commands.Context):
            channel = ctx.channel
        else:
            channel = bot.get_channel(int(config['channel_default']))
    except Exception as e:
        print(f"Error al determinar el canal: {e}")
        return

    print(f"Tipo de ctx: {type(ctx)}, Canal: {channel}")  # Depuración

    # Obtener los miembros conectados a los canales de voz en ese momento
    connected_members = {m.display_name: m for guild in bot.guilds for channel in guild.voice_channels for m in channel.members}

    if modo == "automatico":
        if member is None:
            print("Error: Modo automático requiere un miembro.")
            return
        miembro = member.display_name
        if miembro not in miembros_lista:  # Evitar duplicados
            miembros_lista[miembro] = "si"
            miembros_objetos[miembro] = connected_members[miembro]  # Usar nombre como clave
            print(f"Se ha añadido correctamente el jugador: {miembro}")

    elif modo == "manual":
        if not isinstance(ctx, commands.Context):
            print("Error: Modo manual requiere un contexto de comando.")
            return
        if not miembros_lista:
            await ctx.send(":red_circle: No hay una lista activa para agregar jugadores.")
            return

        await ctx.send("✍ Escribe los nombres de los jugadores uno por uno. Escribe `FIN` para confirmar.")

        def check(m):
            return m.author == ctx.author and m.channel == ctx.channel

        while True:
            try:
                msg = await bot.wait_for("message", check=check, timeout=60.0)
            except asyncio.TimeoutError:
                await ctx.send("⏳ Tiempo de espera agotado. No se añadieron jugadores.")
                return

            if msg.content.strip().upper() == "FIN":
                break

            jugadores = msg.content.strip().splitlines()
            for jugador in jugadores:
                jugador = jugador.strip()
                if jugador:
                    if jugador in miembros_lista:
                        await ctx.send(f"⚠️ `{jugador}` ya está en la lista.")
                    else:
                        if jugador in connected_members:
                            miembros_lista[jugador] = "si"
                            miembros_objetos[jugador] = connected_members[jugador]
                            print(f"Se ha añadido correctamente el jugador: {jugador}")
                        else:
                            miembros_lista[jugador] = "no"
            await msg.delete()

    await eliminar_jugadores_no_en_lista(channel)
    await actualizar_embeds(channel)
    
#################################################################################################

@bot.command()
async def CancelList(ctx):
    global miembros_lista, miembros_objetos, embed_main_message, embed_reservas_message, lista_cerrada, tarea_cerrar_lista
    
    if lista_cerrada:
        await ctx.send("⚠️ No puedes cancelar la lista porque aún no está abierta.")
        return
    
    await ctx.send("❗ ¿Estás seguro de que quieres cancelar la lista? Responde 'CONFIRMAR' para proceder.")
    
    def check(m):
        return m.author == ctx.author and m.content.upper() == "CONFIRMAR"
    
    try:
        await bot.wait_for("message", check=check, timeout=30)
    except asyncio.TimeoutError:
        await ctx.send("⚠️ Cancelación abortada. No se recibió confirmación a tiempo.")
        return
    
    try:
        if embed_reservas_message:
            await embed_reservas_message.delete()
            embed_reservas_message = None
        if embed_main_message:  # Cambié elif por if para asegurar que ambos se borren si existen
            await embed_main_message.delete()
            embed_main_message = None
    except discord.NotFound:
        await ctx.send("⚠️ No se encontró el mensaje del embed, pero se reiniciará la lista igualmente.")
    
    if tarea_cerrar_lista and not tarea_cerrar_lista.done():
        tarea_cerrar_lista.cancel()
        try:
            await tarea_cerrar_lista
        except asyncio.CancelledError:
            print("Tarea de cierre automático cancelada en CancelList")
    tarea_cerrar_lista = None
    
    miembros_lista.clear()
    miembros_objetos.clear()
    lista_cerrada = True
    await ctx.send("✅ La lista ha sido cancelada correctamente.")

#################################################################################################

@bot.command()
async def FinishList(ctx):
    global MAX_TIME_LIST, tarea_cerrar_lista

    if lista_cerrada:
        await ctx.send("No hay ninguna lista que cerrar.")
        return

    await ctx.send("¿Estás seguro de que quieres cerrar la lista? Escribe `CONFIRMAR` para proceder.")
    
    def check(message):
        return message.author == ctx.author and message.content.upper() == "CONFIRMAR"
    
    try:
        await bot.wait_for('message', check=check, timeout=30.0)
    except asyncio.TimeoutError:
        await ctx.send("No se recibió confirmación a tiempo. La operación ha sido cancelada.")
        return
    
    print("Estado antes de cerrar_lista:", "tarea_cerrar_lista =", tarea_cerrar_lista, "lista_cerrada =", lista_cerrada)
    await cerrar_lista(1)
    print("Estado después de cerrar_lista:", "tarea_cerrar_lista =", tarea_cerrar_lista, "lista_cerrada =", lista_cerrada)
    MAX_TIME_LIST = int(os.environ['MAX_TIME'])

#################################################################################################

@bot.command()
async def ShowPastLists(ctx):
    """
    Muestra todas las listas pasadas almacenadas en la base de datos, ordenadas de mas viejo a mas nuevo.
    """
    # Verificar permisos de admin
    if ROL_ID_ADMINS not in [role.id for role in ctx.author.roles]:
        await ctx.send("⚠️ No tienes permisos para usar este comando.")
        return

    # Obtener el canal por defecto
    channel = bot.get_channel(int(config['channel_default']))
    if not channel:
        await ctx.send("⚠️ No se pudo encontrar el canal por defecto.")
        return

    # Consultar listas, ordenadas de mas viejo a mas nuevo
    query = "SELECT FechaLista, DatosLista, NumJugadores FROM Listas ORDER BY FechaLista ASC;"
    listas = sql_fetch(query)
    
    if not listas:
        await ctx.send("📪 No hay listas registradas en la base de datos.")
        return

    await ctx.send(f"📋 Mostrando {len(listas)} listas pasadas...")

    def fix_json(json_str):
        """Repara JSON invalido reemplazando comillas simples."""
        try:
            json_str = re.sub(r"(?<!\\)'", '"', json_str)
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            logging.error(f"Error al reparar JSON: {json_str}, Error: {str(e)}")
            raise

    # Procesar cada lista
    for fecha_lista, datos_lista, num_jugadores in listas:
        try:
            miembros_lista = fix_json(datos_lista)
            embed_main, embed_reservas = generar_embeds(
                miembros_lista=miembros_lista,
                max_jugadores=num_jugadores,
                fecha_lista=fecha_lista,
                is_historico=True
            )
            await channel.send(embed=embed_main)
            if embed_reservas:
                await channel.send(embed=embed_reservas)
            await asyncio.sleep(1)
        except json.JSONDecodeError:
            await channel.send(f"⚠️ Error al procesar la lista del {fecha_lista}: Datos inválidos.")
            logging.error(f"JSON inválido en lista {fecha_lista}: {datos_lista}")
            continue
        except Exception as e:
            await channel.send(f"⚠️ Error al mostrar la lista del {fecha_lista}: {str(e)}")
            logging.error(f"Error inesperado en lista {fecha_lista}: {str(e)}")
            continue

#################################################################################################

async def cerrar_lista(tiempo_espera):
    global lista_cerrada, tarea_cerrar_lista
    
    print("Iniciando cerrar_lista")
    if lista_cerrada:
        print("Lista ya cerrada, saliendo")
        return
    
    if tarea_cerrar_lista is not None:
        if not tarea_cerrar_lista.done():
            print("Cancelando tarea anterior")
            tarea_cerrar_lista.cancel()
            try:
                await tarea_cerrar_lista  # Intentar esperar a que termine
            except asyncio.CancelledError:
                print("Tarea anterior cancelada exitosamente")
        else:
            print("Tarea anterior ya estaba terminada")
        tarea_cerrar_lista = None  # Limpiar la tarea después de cancelarla
    
    print("Creando nueva tarea de cierre")
    tarea_cerrar_lista = asyncio.create_task(proceso_cierre_lista(tiempo_espera))
    await tarea_cerrar_lista
    print("Cierre completado")

#################################################################################################

async def proceso_cierre_lista(tiempo_espera):
    global lista_cerrada, embed_main_message, embed_reservas_message
    
    print(f"Esperando {tiempo_espera} segundos en proceso_cierre_lista")
    await asyncio.sleep(tiempo_espera)
    
    channel = bot.get_channel(int(config['channel_default']))
    embed_main, embed_reservas = generar_embeds()
    
    if embed_main:
        embed_main.set_footer(text=f"⛔ Lista Cerrada\n{embed_main.footer.text.split('\n')[1]}\n🟢 Conectados: {sum(1 for estado in miembros_lista.values() if estado == 'si')} | 🔴 Desconectados: {sum(1 for estado in miembros_lista.values() if estado == 'no')}")
        if embed_main_message:
            await embed_main_message.edit(embed=embed_main)
        else:
            embed_main_message = await channel.send(embed=embed_main)
    
    if embed_reservas:
        if embed_reservas_message:
            await embed_reservas_message.edit(embed=embed_reservas)
        else:
            embed_reservas_message = await channel.send(embed=embed_reservas)
    
    await insert_lista(embed_main_message, embed_reservas_message)
    await actualizar_jugadores_db()
    await UpdateStatsPlayers(None)
    
    lista_cerrada = True
    await channel.send("⛔ La lista se ha cerrado. Ya no se pueden hacer cambios.")

#################################################################################################

async def insert_lista(embed_main_message, embed_reservas_message):
    """
    Inserta una lista en la base de datos con estados 'si'/'no' y nombres sin escapar.
    """
    global miembros_lista, MAX_JUGADORES

    embed_main_message_id = embed_main_message.id if embed_main_message else 0
    embed_reservas_message_id = embed_reservas_message.id if embed_reservas_message else 0
    fecha_lista = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    normalized_miembros_lista = {k: "si" if v == "sí" else "no" if v == "no" else v for k, v in miembros_lista.items()}
    datos_lista = json.dumps(normalized_miembros_lista, ensure_ascii=False)

    query = '''
    INSERT INTO Listas (FechaLista, DatosLista, NumJugadores, EmbedID, EmbedReservasID)
    VALUES (?, ?, ?, ?, ?)
    '''
    sql_update(query, (fecha_lista, datos_lista, MAX_JUGADORES, embed_main_message_id, embed_reservas_message_id))
    print(f"Lista guardada en la base de datos: {fecha_lista}")

#################################################################################################

async def actualizar_jugadores_db():
    global miembros_lista, miembros_objetos

    # Obtener el total de partidas desde sqlite_sequence
    query_total_partidas = "SELECT seq FROM sqlite_sequence WHERE name='Listas';"
    total_partidas = sql_fetch(query_total_partidas)
    total_partidas = total_partidas[0][0] if total_partidas else 0

    for jugador, estado in miembros_lista.items():
        member_obj = miembros_objetos.get(jugador)
        if not member_obj:
            print(f"⚠️ El jugador `{jugador}` no tiene el rol adecuado. No se procesará.")
            continue

        user_discord = member_obj.name
        # Usar global_name si está disponible, de lo contrario usar name
        apodo = member_obj.global_name if member_obj.global_name else member_obj.name

        query = "SELECT * FROM Jugadores WHERE IdDiscord = ?;"
        existing_player = sql_fetch(query, (member_obj.id,))

        if existing_player:
            partidas_inscrito = existing_player[0][3] + 1
            partidas_conectado = existing_player[0][4] + 1 if estado == "si" else existing_player[0][4]
            partidas_desconectado = existing_player[0][5] + 1 if estado == "no" else existing_player[0][5]
            ultima_partida = datetime.now().strftime("%d-%m-%Y") if estado == "si" else (existing_player[0][8] if existing_player[0][8] else "Nunca ha jugado")
            query_update = '''
                UPDATE Jugadores
                SET PartidasInscrito = ?,
                    PartidasConectado = ?,
                    PartidasDesconectado = ?,
                    UltimaPartida = ?
                WHERE IdDiscord = ?;
            '''
            sql_update(query_update, (partidas_inscrito, partidas_conectado, partidas_desconectado, ultima_partida, member_obj.id))
        else:
            partidas_inscrito = 1
            partidas_conectado = 1 if estado == "si" else 0
            partidas_desconectado = 1 if estado == "no" else 0
            ultima_partida = datetime.now().strftime("%d-%m-%Y") if estado == "si" else "Nunca ha jugado"
            query_update = '''
                INSERT INTO Jugadores (IdDiscord, UserDiscord, Apodo, PartidasInscrito, PartidasConectado, PartidasDesconectado, UltimaPartida)
                VALUES (?, ?, ?, ?, ?, ?, ?);
            '''
            sql_update(query_update, (member_obj.id, user_discord, apodo, partidas_inscrito, partidas_conectado, partidas_desconectado, ultima_partida))

    # Actualizar porcentajes
    query_all_players = "SELECT IdDiscord, PartidasInscrito FROM Jugadores;"
    all_players = sql_fetch(query_all_players)
    for player_id, partidas_inscrito in all_players:
        query_desconectado = "SELECT PartidasDesconectado FROM Jugadores WHERE IdDiscord = ?;"
        desconectados = sql_fetch(query_desconectado, (player_id,))
        partidas_desconectado = desconectados[0][0] if desconectados else 0
        porcentaje_inscrito = round((partidas_inscrito / total_partidas) * 100, 2) if total_partidas > 0 else 0
        porcentaje_ausencias = round((partidas_desconectado / partidas_inscrito) * 100, 2) if partidas_inscrito > 0 else 0
        query_update_percentage = '''
            UPDATE Jugadores
            SET PorcentajeInscrito = ?,
                PorcentajeAusencias = ?
            WHERE IdDiscord = ?;
        '''
        sql_update(query_update_percentage, (porcentaje_inscrito, porcentaje_ausencias, player_id))

    print("✅ Los jugadores han sido actualizados en la base de datos.")

#################################################################################################

@bot.command()
async def UpdateStatsPlayers(ctx):
    global THREAD_STATS_NAME

    query_total_partidas = "SELECT seq FROM sqlite_sequence WHERE name='Listas';"
    total_partidas = sql_fetch(query_total_partidas)
    total_partidas = total_partidas[0][0] if total_partidas else 0
    query = "SELECT Apodo, PartidasInscrito, PartidasConectado, PartidasDesconectado, PorcentajeInscrito, PorcentajeAusencias, UltimaPartida FROM Jugadores"
    jugadores = sql_fetch(query)
    
    channel = bot.get_channel(int(config['channel_admin']))
    if not channel:
        print("⚠️ No se encontró el canal especificado.")
        return
    
    if not jugadores:
        await channel.send("No hay jugadores registrados en la base de datos.")
    else:
        # Aumentamos el ancho de la columna "Apodo" para que no desplace las demás columnas
        widths = [20, 18, 18, 20, 10, 10, 19]  # Se amplió el primer valor de 15 a 20

        def get_color_from_percentage(percentage, is_inscription):
            if is_inscription:
                return "🔵" if percentage >= 90 else "🟢" if percentage >= 60 else "🟡" if percentage >= 40 else "🟠" if percentage >= 20 else "🔴"
            else:
                return "🔵" if percentage <= 10 else "🟢" if percentage <= 39 else "🟡" if percentage <= 59 else "🟠" if percentage <= 79 else "🔴"

        # Buscar si ya existe un hilo con el nombre
        thread = None
        for t in channel.threads:
            if t.name == THREAD_STATS_NAME:
                thread = t
                break
        
        # Si existe, eliminarlo
        if thread:
            await thread.delete()
        
        # Crear un nuevo hilo público visible para los miembros del canal
        thread = await channel.create_thread(name=THREAD_STATS_NAME, type=discord.ChannelType.public_thread)

        for i in range(0, len(jugadores), 5):
            group = jugadores[i:i+5]
            
            message = f"📋 **Partidas totales jugadas: {total_partidas}**\n\n"
            message += f"{'Apodo':<{widths[0]}} | {'Part. Inscritas':>{widths[1]}} | {'Part. Conectado':>{widths[2]}} | {'Part. Desconectado':>{widths[3]}} | {'% Inscr.':>{widths[4]}} | {'% Aus.':>{widths[5]}} | {'Última Partida':>{widths[6]}}\n"
            message += "─" * (sum(widths) + 6*3) + "\n"

            for user in group:
                apodo, inscritos, conectados, desconectados, porcentaje_inscrito, porcentaje_ausencias, ultima_partida = user
                # Manejar caso donde apodo es None
                display_name = apodo if apodo is not None else "Sin apodo"
                color_inscripcion = get_color_from_percentage(porcentaje_inscrito, is_inscription=True)
                color_ausencias = get_color_from_percentage(porcentaje_ausencias, is_inscription=False)

                line = (f"{display_name:<{widths[0]}} | {inscritos:>{widths[1]}} | {conectados:>{widths[2]}} | "
                        f"{desconectados:>{widths[3]}} | {color_inscripcion} {porcentaje_inscrito:>{widths[4]}.2f}% | "
                        f"{color_ausencias} {porcentaje_ausencias:>{widths[5]}.2f}% | "
                        f"{ultima_partida if ultima_partida else 'No disponible':>{widths[6]}}")

                message += line + "\n"
            
            await thread.send(message)
            await asyncio.sleep(1)

#################################################################################################

@bot.command()
async def SetMP(ctx):

    if not lista_cerrada:  # Si la lista está abierta
        await ctx.send("No se puede modificar este parámetro hasta que la lista esté cerrada.")
        return  # Salir de la función sin hacer nada

    await ctx.send("Introduce el número máximo de jugadores (entre 1 y 50):")
    
    while True:
        try:
            msg = await bot.wait_for('message', check=lambda m: m.author == ctx.author, timeout=30)
            if msg.content.isdigit():
                new_value = int(msg.content)
                if 1 <= new_value <= 50:
                    global MAX_JUGADORES, MAX_JUGADORES_LISTAS
                    
                    # Cargar el archivo .env
                    dotenv_file = dotenv.find_dotenv()
                    dotenv.load_dotenv(dotenv_file)

                    # Actualizar el valor en el archivo .env
                    os.environ["MAX_PLAYERS"] = str(new_value)
                    dotenv.set_key(dotenv_file, "MAX_PLAYERS", os.environ["MAX_PLAYERS"])

                    # Actualizar las variables globales directamente
                    MAX_JUGADORES = new_value
                    MAX_JUGADORES_LISTAS = MAX_JUGADORES * 2
                    
                    # Imprimir para verificar
                    print(f"MAX_JUGADORES actualizado a: {MAX_JUGADORES}")
                    print(f"MAX_JUGADORES_LISTAS calculado a: {MAX_JUGADORES_LISTAS}")

                    # Confirmación
                    await ctx.send(f"✅ Se ha actualizado MAX_PLAYERS a {new_value}.")
                    break
                else:
                    await ctx.send("⚠️ Ingresa un número válido entre 1 y 50.")
            else:
                await ctx.send("⚠️ Ingresa un número válido entre 1 y 50.")
        except asyncio.TimeoutError:
            await ctx.send("⏳ Tiempo de espera agotado. Operación cancelada.")
            return

#################################################################################################

async def borrar_mensajes_sin_embed():
    while True:
        await asyncio.sleep(101)  # Esperar los segundos entre limpiezas

        # Obtener el canal específico usando el ID desde la configuración
        channel = bot.get_channel(int(config['channel_default']))

        if channel:
            # Obtener los últimos X mensajes del canal
            async for msg in channel.history(limit=50):
                # Verificar si el mensaje no tiene un embed
                if not msg.embeds:  # Si el mensaje no tiene un embed
                    try:
                        # Borrar el mensaje
                        await msg.delete()
                        #print(f"Mensaje borrado: {msg.content}")
                        # Esperar 5 segundos antes de borrar el siguiente mensaje
                        await asyncio.sleep(2)
                    except discord.errors.NotFound:
                        # El mensaje ya no existe (por ejemplo, si se eliminó previamente)
                        continue
        else:
            print("Canal no encontrado o el bot no tiene acceso a él.")

#################################################################################################

@bot.event
async def on_voice_state_update(member, before, after):
    global embed_main_message, embed_reservas_message, miembros_lista, adding_players, adding_lock, ultima_actualizacion_embed, embed_update_lock
    if lista_cerrada:
        return

    # Caso 1: Añadir automáticamente desde el canal de reservas
    if member.display_name not in miembros_lista and after.channel and after.channel.id == VOICE_CHR_ID:
        async with adding_lock:
            if member.display_name not in adding_players:
                adding_players.add(member.display_name)
                modo = "automatico"
                # Obtener el canal predeterminado
                channel = bot.get_channel(int(config['channel_default']))
                print(f"Añadiendo automáticamente a {member.display_name} desde canal de reservas")  # Depuración
                await add_players(channel, member, modo)
                adding_players.remove(member.display_name)

    # Caso 2: Actualizar estado de miembros en la lista
    if member.display_name in miembros_lista:
        if before.channel is None and after.channel is not None:
            miembros_lista[member.display_name] = "si"
            print(f"{member.display_name} se ha conectado. Estado actualizado a 'si'.")
        elif before.channel is not None and after.channel is None:
            miembros_lista[member.display_name] = "no"
            print(f"{member.display_name} se ha desconectado. Estado actualizado a 'no'.")

        # Limitar la actualización del embed a una vez cada 2 segundos
        if embed_main_message or embed_reservas_message:
            async with embed_update_lock:
                tiempo_actual = time.time()
                if tiempo_actual - ultima_actualizacion_embed >= 2:  # 2 segundos de intervalo mínimo
                    channel = bot.get_channel(int(config['channel_default']))
                    print(f"Actualizando embeds para {member.display_name}")  # Depuración
                    await actualizar_embeds(channel)
                    ultima_actualizacion_embed = tiempo_actual

#################################################################################################

async def comprobar_conectados_periodicamente():
    global VOICE_CHR_ID

    while True:
        await asyncio.sleep(60)  # Espera 60 segundos

        if lista_cerrada:
            continue  # Saltar iteración si la lista está cerrada

        # Obtener miembros conectados
        connected_members = {member.display_name for guild in bot.guilds for channel in guild.voice_channels for member in channel.members}
        #print(f"Miembros conectados actualizados: {connected_members}")  # Mostrar miembros conectados actualizados

        # Actualizar la lista de miembros conectados
        for miembro in miembros_lista:
            if miembro in connected_members:
                miembros_lista[miembro] = "si"
            else:
                miembros_lista[miembro] = "no"
            #print(miembros_lista)  # Se imprime cada vez que se modifica la lista

        # Actualizar el embed
        if embed_main_message or embed_reservas_message:
            await actualizar_embeds(bot.get_channel(int(config['channel_default'])))

#################################################################################################

bot.run(config['token'], reconnect=True)

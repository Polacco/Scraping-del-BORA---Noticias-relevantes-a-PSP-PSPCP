import requests
import json
import logging
import argparse
import time
from datetime import datetime, date
from bs4 import BeautifulSoup
from dataclasses import dataclass, field
from typing import Optional
from langchain_groq import ChatGroq
from langchain_core.messages import SystemMessage, HumanMessage

CONFIG = {
    # Token de integración de Notion
    "NOTION_TOKEN": "ntn_....",

    # ID de tu base de datos de Notion
    "NOTION_DATABASE_ID": "...",

    # API Key de Groq
    "GROQ_API_KEY": "gsk_....",

    # Secciones del BORA a monitorear
    # "primera" = Leyes, Decretos, Resoluciones (BCRA, UIF, ARCA)
    # "segunda" = Avisos judiciales, concursos, quiebras
    "SECCIONES": ["primera", "segunda"],

    # Máximo de ítems a procesar por sección
    "MAX_ITEMS_POR_SECCION": 300,

    # Pausa entre requests al BORA
    "PAUSA_REQUESTS": 0.5,
}


PALABRAS_CLAVE = {
    "🔴 Crítico — PSP/PSPCP": [
        "PSP", "PSPCP", "proveedor de servicios de pago",
        "proveedora de servicios de pago",
        "billetera virtual", "billetera electrónica",
        "cuenta de pago", "transferencia inmediata",
        "código QR", "pagos con QR",
    ],
    "🔴 Crítico — UIF / AML": [
        "UIF", "unidad de información financiera",
        "lavado de activos", "financiamiento del terrorismo",
        "resolución UIF", "sujeto obligado",
        "sanción UIF", "sancionado", "inhabilitado UIF",
        "reportes de operaciones sospechosas",
    ],
    "🔴 Crítico — BCRA": [
        "BCRA", "banco central", "comunicación A",
        "comunicación B", "CREBAN", "entidad financiera",
        "sistema de pagos", "cámara compensadora",
    ],
    "🟠 Importante — ARCA / Impositivo": [
        "ARCA", "AFIP", "inhibición general",
        "embargo fiscal", "clausura", "ejecución fiscal",
        "deuda tributaria", "incumplimiento impositivo",
    ],
    "🟠 Importante — Sanciones y Concursos": [
        "concurso preventivo", "quiebra", "quiebra decretada",
        "inhibición general de bienes", "embargo",
        "medida cautelar", "sindicato concursal",
        "apertura de concurso",
    ],
    "🟡 Informativo — Fintech general": [
        "fintech", "criptoactivos", "activos digitales",
        "moneda digital", "DEBIN", "CVU", "CBU",
        "interoperabilidad", "pagos electrónicos",
        "infraestructura del mercado financiero",
    ],
}


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)


@dataclass
class ItemBORA:
    numero_tramite: str
    titulo: str
    seccion: str
    organismo: str = ""
    texto: str = ""
    url: str = ""
    categorias_match: list = field(default_factory=list)
    analisis_ia: Optional[dict] = field(default=None)
    relevancia_ia: str = ""

    def tiene_match(self) -> bool:
        return len(self.categorias_match) > 0

    def prioridad(self) -> str:
        """Usa la prioridad de la IA si está disponible, sino la de keywords"""
        if self.analisis_ia and self.analisis_ia.get("prioridad"):
            return self.analisis_ia["prioridad"]
        for cat in self.categorias_match:
            if "🔴" in cat:
                return "🔴 Crítico"
        for cat in self.categorias_match:
            if "🟠" in cat:
                return "🟠 Importante"
        return "🟡 Informativo"

    def resumen_corto(self) -> str:
        """Usa el resumen de la IA si está disponible, sino el texto truncado"""
        if self.analisis_ia and self.analisis_ia.get("resumen"):
            return self.analisis_ia["resumen"]
        texto_limpio = " ".join(self.texto.split())
        return texto_limpio[:500] + "..." if len(texto_limpio) > 500 else texto_limpio

    def categoria_principal(self) -> str:
        """Usa la categoría de la IA si está disponible"""
        if self.analisis_ia and self.analisis_ia.get("categoria"):
            return self.analisis_ia["categoria"]
        return self.categorias_match[0] if self.categorias_match else "Sin categoría"

class BORAScraper:
    BASE_URL = "https://www.boletinoficial.gob.ar"
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (compatible; PSP-Compliance-Monitor/1.0)",
        "Accept": "application/json, text/html",
        "Referer": "https://www.boletinoficial.gob.ar/",
    }

    def __init__(self, fecha: str):
        self.fecha = fecha
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)

    def obtener_indice_seccion(self, seccion: str) -> list[dict]:
        url = f"{self.BASE_URL}/busqueda/publicaciones"
        params = {"fecha": self.fecha, "seccion": seccion}
        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code == 200:
                try:
                    data = resp.json()
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict) and "publicaciones" in data:
                        return data["publicaciones"]
                except json.JSONDecodeError:
                    pass
            return self._scraping_seccion(seccion)
        except requests.RequestException as e:
            logger.error(f"Error obteniendo índice de sección {seccion}: {e}")
            return self._scraping_seccion(seccion)

    def _scraping_seccion(self, seccion: str) -> list[dict]:
        try:
            dt = datetime.strptime(self.fecha, "%d/%m/%Y")
            fecha_url = dt.strftime("%Y%m%d")
        except ValueError:
            fecha_url = self.fecha.replace("/", "")

        url = f"{self.BASE_URL}/seccion/{seccion}/{fecha_url}"
        items = []
        try:
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                logger.warning(f"Sección {seccion} devolvió {resp.status_code}")
                return []
            soup = BeautifulSoup(resp.text, "html.parser")
            for link in soup.find_all("a", href=True):
                href = link["href"]
                if "/detalleAviso/" in href:
                    partes = href.strip("/").split("/")
                    if len(partes) >= 3:
                        numero = partes[-2]
                        titulo = link.get_text(strip=True) or f"Aviso {numero}"
                        items.append({
                            "numeroTramite": numero,
                            "titulo": titulo,
                            "url": f"{self.BASE_URL}{href}",
                        })
            logger.info(f"Sección {seccion}: {len(items)} ítems por scraping")
        except requests.RequestException as e:
            logger.error(f"Error de scraping en sección {seccion}: {e}")
        return items

    def obtener_detalle(self, numero_tramite: str, seccion: str) -> Optional[str]:
        try:
            dt = datetime.strptime(self.fecha, "%d/%m/%Y")
            fecha_url = dt.strftime("%Y%m%d")
        except ValueError:
            fecha_url = self.fecha.replace("/", "")

        url = f"{self.BASE_URL}/detalleAviso/{seccion}/{numero_tramite}/{fecha_url}"
        try:
            time.sleep(CONFIG["PAUSA_REQUESTS"])
            resp = self.session.get(url, timeout=15)
            if resp.status_code != 200:
                return None
            soup = BeautifulSoup(resp.text, "html.parser")
            contenido = (
                soup.find("div", {"id": "cuerpoAviso"}) or
                soup.find("div", class_="aviso-cuerpo") or
                soup.find("article") or
                soup.find("main")
            )
            if contenido:
                return contenido.get_text(separator=" ", strip=True)
            return soup.get_text(separator=" ", strip=True)[:2000]
        except requests.RequestException as e:
            logger.warning(f"No se pudo obtener detalle de {numero_tramite}: {e}")
            return None

class FiltroKeywords:
    def __init__(self, keywords: dict):
        self.keywords = keywords

    def analizar(self, item: ItemBORA) -> ItemBORA:
        texto_completo = f"{item.titulo} {item.organismo} {item.texto}".upper()
        matches = []
        for categoria, palabras in self.keywords.items():
            for palabra in palabras:
                if palabra.upper() in texto_completo:
                    matches.append(categoria)
                    break
        item.categorias_match = matches
        return item


SISTEMA_PROMPT_IA = """Eres un experto en regulación financiera argentina, especializado en PSP y PSPCP (Proveedores de Servicios de Pago con Cuenta de Pago) regulados por el BCRA.

Tu tarea es analizar publicaciones del Boletín Oficial de Argentina (BORA) y determinar si son relevantes para una empresa PSP/PSPCP.

Responde SIEMPRE con un JSON válido con exactamente estas claves:
- es_relevante: true o false
- prioridad: "🔴 Crítico", "🟠 Importante" o "🟡 Informativo"
- categoria: string corto (ej: "Regulacion PSP", "Sancion UIF", "Marco Impositivo", "Concurso Preventivo")
- resumen: string de 2-3 oraciones explicando de qué trata la publicación
- relevancia: string explicando concretamente por qué le importa (o no) a una PSP/PSPCP

Criterios de relevancia:
- 🔴 Crítico: normas que modifican habilitaciones PSP, sanciones BCRA/UIF directas, cambios en sistema de pagos
- 🟠 Importante: normas impositivas que afectan fintech, sanciones a proveedores/socios, quiebras de entidades relacionadas
- 🟡 Informativo: tendencias regulatorias, noticias del ecosistema fintech/cripto, marcos generales
- es_relevante=false: publicaciones que trigger palabras clave por coincidencia pero no impactan a una PSP/PSPCP

No incluyas texto fuera del JSON."""


class AnalizadorIA:
    """Analiza ítems del BORA usando Groq + llama-3.3-70b-versatile para confirmar relevancia real"""

    def __init__(self, api_key: str):
        self.llm = ChatGroq(
            api_key=api_key,
            model="llama-3.3-70b-versatile",
            temperature=0.1,
        )

    def analizar(self, item: ItemBORA) -> Optional[dict]:
        """Llama a la IA y retorna el dict de análisis, o None si falla"""
        texto_item = (
            f"Título: {item.titulo}\n"
            f"Organismo: {item.organismo}\n"
            f"Sección BORA: {item.seccion}\n"
            f"Categorías keyword: {', '.join(item.categorias_match)}\n"
            f"Texto: {item.texto[:3000] if item.texto else 'No disponible'}"
        )
        try:
            respuesta = self.llm.invoke([
                SystemMessage(content=SISTEMA_PROMPT_IA),
                HumanMessage(content=texto_item),
            ])
            contenido = respuesta.content.strip()
            if "```" in contenido:
                inicio = contenido.find("{")
                fin = contenido.rfind("}") + 1
                contenido = contenido[inicio:fin]
            return json.loads(contenido)
        except json.JSONDecodeError as e:
            logger.warning(f"IA devolvió JSON inválido para {item.numero_tramite}: {e}")
            return None
        except Exception as e:
            logger.error(f"Error llamando a Groq para {item.numero_tramite}: {e}")
            return None


class NotionPublisher:
    """Sube los resultados del día a una base de datos de Notion"""

    API_URL = "https://api.notion.com/v1"
    NOTION_VERSION = "2022-06-28"

    def __init__(self, token: str, database_id: str):
        self.database_id = database_id
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": self.NOTION_VERSION,
        }

    def _truncar(self, texto: str, limite: int = 2000) -> str:
        """Notion tiene límite de 2000 caracteres por campo de texto"""
        if not texto:
            return ""
        return texto[:limite] + "..." if len(texto) > limite else texto

    def crear_entrada_resumen(self, fecha: str, stats: dict) -> bool:
        """Crea una fila de resumen del día (aparece primero)"""
        if stats["total_match"] == 0:
            titulo = f"✅ {fecha} — Sin novedades relevantes"
        else:
            titulo = f"📊 Resumen {fecha} — {stats['total_match']} publicaciones encontradas"

        resumen = (
            f"Procesados: {stats['total_procesados']} | "
            f"Match: {stats['total_match']} | "
            f"🔴 Críticos: {stats['criticos']} | "
            f"🟠 Importantes: {stats['importantes']} | "
            f"🟡 Informativos: {stats['informativos']}"
        )

        payload = {
            "parent": {"database_id": self.database_id},
            "properties": {
                "Título": {
                    "title": [{"text": {"content": titulo}}]
                },
                "Fecha": {
                    "date": {"start": datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")}
                },
                "Prioridad": {
                    "select": {"name": "📊 Resumen del día"}
                },
                "Sección": {
                    "select": {"name": "Resumen"}
                },
                "Resumen": {
                    "rich_text": [{"text": {"content": resumen}}]
                },
            }
        }

        try:
            resp = requests.post(
                f"{self.API_URL}/pages",
                headers=self.headers,
                json=payload,
                timeout=15
            )
            if resp.status_code == 200:
                logger.info("✅ Resumen del día subido a Notion")
                return True
            else:
                logger.error(f"Error subiendo resumen: {resp.status_code} — {resp.text[:300]}")
                return False
        except requests.RequestException as e:
            logger.error(f"Error de conexión con Notion: {e}")
            return False

    def crear_entrada_item(self, item: ItemBORA, fecha: str) -> bool:
        """Crea una fila por cada ítem del BORA que hizo match"""
        titulo    = self._truncar(item.titulo or f"Aviso {item.numero_tramite}", 100)
        resumen   = self._truncar(item.resumen_corto(), 2000)
        categoria = self._truncar(item.categoria_principal(), 100)
        organismo = self._truncar(item.organismo or "No especificado", 200)
        relevancia = self._truncar(item.relevancia_ia or "", 2000)

        payload = {
            "parent": {"database_id": self.database_id},
            "properties": {
                "Título": {
                    "title": [{"text": {"content": titulo}}]
                },
                "Fecha": {
                    "date": {"start": datetime.strptime(fecha, "%d/%m/%Y").strftime("%Y-%m-%d")}
                },
                "Prioridad": {
                    "select": {"name": item.prioridad()}
                },
                "Sección": {
                    "select": {"name": item.seccion.capitalize()}
                },
                "Categoría": {
                    "select": {"name": categoria}
                },
                "Organismo": {
                    "rich_text": [{"text": {"content": organismo}}]
                },
                "Resumen": {
                    "rich_text": [{"text": {"content": resumen}}]
                },
                "Link BORA": {
                    "url": item.url or None
                },
                "Relevancia": {
                    "rich_text": [{"text": {"content": relevancia}}]
                },
            }
        }

        try:
            resp = requests.post(
                f"{self.API_URL}/pages",
                headers=self.headers,
                json=payload,
                timeout=15
            )
            if resp.status_code == 200:
                return True
            else:
                logger.warning(
                    f"Error subiendo ítem {item.numero_tramite}: "
                    f"{resp.status_code} — {resp.text[:200]}"
                )
                return False
        except requests.RequestException as e:
            logger.error(f"Error de conexión subiendo ítem: {e}")
            return False

    def subir_resultados(self, items_match: list[ItemBORA], fecha: str, stats: dict):
        """Sube el resumen del día + todos los ítems con match a Notion"""
        logger.info(f"Subiendo resultados a Notion ({len(items_match)} ítems)...")

        self.crear_entrada_resumen(fecha, stats)
        time.sleep(0.3)

        ok = 0
        for item in items_match:
            if self.crear_entrada_item(item, fecha):
                ok += 1
            time.sleep(0.3)  

        logger.info(f"✅ Notion: {ok}/{len(items_match)} ítems subidos")


def correr_monitor(fecha_str: str, usar_ia: bool = True):
    logger.info(f"=== Iniciando Monitor BORA — {fecha_str} ===")
    if not usar_ia:
        logger.info("[Modo: solo keywords — IA desactivada]")
    else:
        logger.info("[Modo: keywords + análisis IA con Groq]")

    # --- Paso 1: Scraping del BORA ---
    logger.info("--- Paso 1: Scraping del BORA ---")
    scraper = BORAScraper(fecha=fecha_str)
    filtro  = FiltroKeywords(PALABRAS_CLAVE)
    notion  = NotionPublisher(
        token=CONFIG["NOTION_TOKEN"],
        database_id=CONFIG["NOTION_DATABASE_ID"]
    )

    todos_los_items: list[ItemBORA] = []

    for seccion in CONFIG["SECCIONES"]:
        logger.info(f"Procesando sección: {seccion}...")
        indice = scraper.obtener_indice_seccion(seccion)

        if not indice:
            logger.warning(f"Sin ítems en sección {seccion}")
            continue

        indice = indice[:CONFIG["MAX_ITEMS_POR_SECCION"]]
        logger.info(f"Sección {seccion}: procesando {len(indice)} ítems")

        for raw in indice:
            numero    = str(raw.get("numeroTramite") or raw.get("numero") or raw.get("id") or "")
            titulo    = raw.get("titulo") or raw.get("title") or raw.get("denominacion") or ""
            organismo = raw.get("organismo") or raw.get("emisor") or ""
            url_item  = raw.get("url") or f"https://www.boletinoficial.gob.ar/detalleAviso/{seccion}/{numero}"

            item = ItemBORA(
                numero_tramite=numero,
                titulo=titulo,
                seccion=seccion,
                organismo=organismo,
                url=url_item,
            )
            todos_los_items.append(item)

    # --- Paso 2: Filtro rápido por keywords ---
    logger.info("--- Paso 2: Filtro por keywords ---")
    for item in todos_los_items:
        item = filtro.analizar(item)

        if not item.tiene_match() and item.numero_tramite:
            texto = scraper.obtener_detalle(item.numero_tramite, item.seccion)
            if texto:
                item.texto = texto
                item = filtro.analizar(item)

    items_keyword = [i for i in todos_los_items if i.tiene_match()]
    logger.info(f"Total procesados: {len(todos_los_items)} | Match keywords: {len(items_keyword)}")

    # --- Paso 3: Análisis IA con Groq (opcional) ---
    items_match: list[ItemBORA]
    if usar_ia and items_keyword:
        logger.info(f"--- Paso 3: Análisis IA ({len(items_keyword)} ítems) ---")
        analizador = AnalizadorIA(api_key=CONFIG["GROQ_API_KEY"])
        items_match = []
        for item in items_keyword:
            logger.info(f"  Analizando con IA: {item.titulo[:60]}...")
            analisis = analizador.analizar(item)
            if analisis is None:
                logger.warning(f"  IA falló para {item.numero_tramite}, incluyendo igual")
                items_match.append(item)
                continue
            item.analisis_ia = analisis
            item.relevancia_ia = analisis.get("relevancia", "")
            if analisis.get("es_relevante", True):
                items_match.append(item)
                logger.info(f"  ✅ Relevante [{analisis.get('prioridad', '?')}]: {analisis.get('categoria', '?')}")
            else:
                logger.info(f"  ⏭️  Descartado por IA: {analisis.get('relevancia', '')[:80]}")
            time.sleep(0.5)  
        logger.info(f"IA confirmó: {len(items_match)}/{len(items_keyword)} relevantes")
    else:
        if not usar_ia:
            logger.info("--- Paso 3: IA omitida (--sin-ia) ---")
        items_match = items_keyword

    # --- Paso 4: Subir a Notion ---
    logger.info("--- Paso 4: Subiendo a Notion ---")
    stats = {
        "total_procesados": len(todos_los_items),
        "total_match":      len(items_match),
        "criticos":    sum(1 for i in items_match if "🔴" in i.prioridad()),
        "importantes": sum(1 for i in items_match if "🟠" in i.prioridad()),
        "informativos": sum(1 for i in items_match if "🟡" in i.prioridad()),
    }

    notion.subir_resultados(items_match, fecha_str, stats)

    logger.info("=== Monitor BORA finalizado ===")
    return items_match


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor diario del Boletín Oficial → Notion")
    parser.add_argument(
        "--fecha",
        default=date.today().strftime("%d/%m/%Y"),
        help="Fecha a procesar en formato DD/MM/YYYY. Default: hoy"
    )
    parser.add_argument(
        "--sin-ia",
        action="store_true",
        default=False,
        help="Omitir análisis IA de Groq y subir todo lo que pase keywords. Útil para pruebas."
    )
    args = parser.parse_args()
    correr_monitor(args.fecha, usar_ia=not args.sin_ia)
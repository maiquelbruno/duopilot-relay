"""
=============================================================
  DUOPILOT BR — Servidor Relay
  Deploy: Render.com (free tier)
  Usa aiohttp — responde health checks HTTP e WebSocket
=============================================================
"""

import asyncio
import json
import logging
import os
from aiohttp import web

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("DuoPilotRelay")

PORT = int(os.environ.get("PORT", 10000))

sessions = {}
stats = {"total_sessions": 0, "total_packets": 0}


async def health(request):
    """Health check para o Render — responde GET e HEAD em /"""
    return web.Response(text="DuoPilot BR Relay OK")


async def websocket_handler(request):
    """Handler principal WebSocket"""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    addr = request.remote
    log.info(f"Nova conexão: {addr}")
    pin_atual   = None
    papel_atual = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    obj = json.loads(msg.data)
                except json.JSONDecodeError:
                    continue

                tipo = obj.get("tipo")

                if tipo == "RELAY_JOIN":
                    pin   = obj.get("pin")
                    papel = obj.get("papel")
                    aviao = obj.get("aviao", "")

                    if not pin or not papel:
                        await ws.send_str(json.dumps({
                            "tipo": "ERRO",
                            "motivo": "PIN e papel obrigatórios"}))
                        continue

                    if papel == "CAPTAIN":
                        # Captain cria a sessão
                        if pin not in sessions:
                            sessions[pin] = {"captain": None, "fo": None, "aviao": aviao}
                            stats["total_sessions"] += 1
                        sessao = sessions[pin]
                        sessao["captain"] = ws
                        sessao["aviao"]   = aviao
                        pin_atual   = pin
                        papel_atual = papel
                        log.info(f"[{pin}] Captain ({aviao})")
                        await ws.send_str(json.dumps({
                            "tipo": "RELAY_OK",
                            "papel": "CAPTAIN",
                            "mensagem": "Aguardando First Officer..."}))

                    elif papel == "FIRST_OFFICER":
                        # FO só pode entrar em sessão já existente com Captain ativo
                        sessao = sessions.get(pin)
                        if not sessao or not sessao.get("captain"):
                            await ws.send_str(json.dumps({
                                "tipo": "ERRO",
                                "motivo": "PIN inválido ou Captain não encontrado."}))
                            continue  # pin_atual permanece None — sessão não registrada

                        if aviao and sessao["aviao"] and aviao != sessao["aviao"]:
                            await ws.send_str(json.dumps({
                                "tipo": "ERRO",
                                "motivo": f"Avião incompatível: {aviao} vs {sessao['aviao']}"}))
                            continue

                        sessao["fo"] = ws
                        pin_atual   = pin
                        papel_atual = papel
                        log.info(f"[{pin}] First Officer conectado — sessão completa!")

                        await ws.send_str(json.dumps({
                            "tipo": "RELAY_OK",
                            "papel": "FIRST_OFFICER",
                            "aviao": sessao["aviao"],
                            "mensagem": "Conectado ao Captain."}))

                        if sessao["captain"]:
                            await sessao["captain"].send_str(json.dumps({
                                "tipo": "FO_CONECTADO",
                                "mensagem": "First Officer conectado."}))

                elif tipo in ("DR", "CMD") and pin_atual:
                    sessao = sessions.get(pin_atual)
                    if not sessao:
                        continue
                    stats["total_packets"] += 1
                    parceiro = sessao.get("fo") if papel_atual == "CAPTAIN" \
                               else sessao.get("captain")
                    if parceiro and not parceiro.closed:
                        await parceiro.send_str(json.dumps(obj))

                elif tipo == "PING":
                    await ws.send_str(json.dumps({"tipo": "PONG"}))

            elif msg.type == web.WSMsgType.ERROR:
                log.error(f"Erro WS: {ws.exception()}")

    except Exception as e:
        log.error(f"Erro: {e}")
    finally:
        if pin_atual and pin_atual in sessions:
            sessao = sessions[pin_atual]
            parceiro = None
            msg_saiu = ""
            if papel_atual == "CAPTAIN":
                sessao["captain"] = None
                parceiro = sessao.get("fo")
                msg_saiu = "Captain desconectou."
            else:
                sessao["fo"] = None
                parceiro = sessao.get("captain")
                msg_saiu = "First Officer desconectou."
            if parceiro and not parceiro.closed:
                await parceiro.send_str(json.dumps({
                    "tipo": "PARCEIRO_SAIU",
                    "mensagem": msg_saiu}))
            if not sessao["captain"] and not sessao["fo"]:
                del sessions[pin_atual]
                log.info(f"[{pin_atual}] Sessão encerrada.")
        log.info(f"Conexão encerrada: {addr}")

    return ws


async def status_periodico():
    while True:
        await asyncio.sleep(300)
        log.info(f"STATUS — Sessões: {len(sessions)} | "
                 f"Total: {stats['total_sessions']} | "
                 f"Pacotes: {stats['total_packets']}")


async def main():
    app = web.Application()
    app.router.add_get("/",        health)
    app.router.add_get("/health",  health)
    app.router.add_get("/ws",      websocket_handler)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    log.info(f"DuoPilot BR Relay pronto na porta {PORT}")
    asyncio.create_task(status_periodico())
    await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())

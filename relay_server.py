"""
=============================================================
  DUOPILOT BR — Servidor Relay WebSocket
  Deploy: Render.com (free tier)

  Usa WebSocket sobre HTTP — compatível com Render free.
  Instalar: pip install websockets
=============================================================
"""

import asyncio
import json
import logging
import os
import websockets

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("DuoPilotRelay")

PORT = int(os.environ.get("PORT", 10000))

# Sessões ativas: { pin: {"captain": ws, "fo": ws, "aviao": str} }
sessions = {}
stats = {"total_sessions": 0, "total_packets": 0}


async def handle_client(websocket):
    addr = websocket.remote_address
    log.info(f"Nova conexão: {addr}")
    pin_atual  = None
    papel_atual = None

    try:
        async for message in websocket:
            try:
                msg = json.loads(message)
            except json.JSONDecodeError:
                continue

            tipo = msg.get("tipo")

            # ── JOIN — cliente apresenta PIN e papel ───────────────
            if tipo == "RELAY_JOIN":
                pin   = msg.get("pin")
                papel = msg.get("papel")
                aviao = msg.get("aviao", "")

                if not pin or not papel:
                    await websocket.send(json.dumps({
                        "tipo": "ERRO", "motivo": "PIN e papel obrigatórios"}))
                    continue

                pin_atual   = pin
                papel_atual = papel

                if pin not in sessions:
                    sessions[pin] = {"captain": None, "fo": None, "aviao": aviao}
                    stats["total_sessions"] += 1

                sessao = sessions[pin]

                if papel == "CAPTAIN":
                    sessao["captain"] = websocket
                    sessao["aviao"]   = aviao
                    log.info(f"[{pin}] Captain conectado ({aviao})")
                    await websocket.send(json.dumps({
                        "tipo": "RELAY_OK",
                        "papel": "CAPTAIN",
                        "mensagem": "Aguardando First Officer..."}))

                elif papel == "FIRST_OFFICER":
                    if sessao["captain"] is None:
                        await websocket.send(json.dumps({
                            "tipo": "ERRO",
                            "motivo": "Nenhum Captain nesta sessão. Verifique o PIN."}))
                        continue

                    if aviao and sessao["aviao"] and aviao != sessao["aviao"]:
                        await websocket.send(json.dumps({
                            "tipo": "ERRO",
                            "motivo": f"Avião incompatível: {aviao} vs {sessao['aviao']}"}))
                        continue

                    sessao["fo"] = websocket
                    log.info(f"[{pin}] First Officer conectado — sessão completa!")

                    await websocket.send(json.dumps({
                        "tipo": "RELAY_OK",
                        "papel": "FIRST_OFFICER",
                        "aviao": sessao["aviao"],
                        "mensagem": "Conectado ao Captain."}))

                    if sessao["captain"]:
                        await sessao["captain"].send(json.dumps({
                            "tipo": "FO_CONECTADO",
                            "mensagem": "First Officer conectado."}))

            # ── DADOS — bridge para o parceiro ────────────────────
            elif tipo in ("DR", "CMD") and pin_atual:
                sessao = sessions.get(pin_atual)
                if not sessao:
                    continue

                stats["total_packets"] += 1
                parceiro = sessao.get("fo") if papel_atual == "CAPTAIN" else sessao.get("captain")

                if parceiro:
                    try:
                        await parceiro.send(json.dumps(msg))
                    except Exception:
                        log.warning(f"[{pin_atual}] Parceiro desconectou.")

            # ── PING ──────────────────────────────────────────────
            elif tipo == "PING":
                await websocket.send(json.dumps({"tipo": "PONG"}))

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        log.error(f"Erro com {addr}: {e}")
    finally:
        if pin_atual and pin_atual in sessions:
            sessao = sessions[pin_atual]
            parceiro = None

            if papel_atual == "CAPTAIN":
                sessao["captain"] = None
                parceiro = sessao.get("fo")
                msg_saiu = "Captain desconectou."
            else:
                sessao["fo"] = None
                parceiro = sessao.get("captain")
                msg_saiu = "First Officer desconectou."

            if parceiro:
                try:
                    await parceiro.send(json.dumps({
                        "tipo": "PARCEIRO_SAIU",
                        "mensagem": msg_saiu}))
                except Exception:
                    pass

            if sessao["captain"] is None and sessao["fo"] is None:
                del sessions[pin_atual]
                log.info(f"[{pin_atual}] Sessão encerrada.")

        log.info(f"Conexão encerrada: {addr}")


async def status_periodico():
    while True:
        await asyncio.sleep(300)
        log.info(f"STATUS — Sessões: {len(sessions)} | "
                 f"Total: {stats['total_sessions']} | Pacotes: {stats['total_packets']}")


async def main():
    log.info(f"DuoPilot BR Relay WebSocket iniciando na porta {PORT}...")
    async with websockets.serve(handle_client, "0.0.0.0", PORT):
        log.info(f"Relay pronto na porta {PORT}")
        asyncio.create_task(status_periodico())
        await asyncio.Future()  # roda para sempre


if __name__ == "__main__":
    asyncio.run(main())

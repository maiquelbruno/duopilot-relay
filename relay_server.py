"""
=============================================================
  DUOPILOT BR — Servidor Relay
  Deploy: Render.com (free tier)

  Como funciona:
  1. Captain conecta com PIN gerado
  2. FO conecta com o mesmo PIN
  3. Servidor identifica os dois pelo PIN e faz bridge
  4. Tudo que Captain envia, FO recebe e vice-versa
=============================================================
"""

import asyncio
import json
import logging
import os
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("DuoPilotRelay")

# Porta — Render injeta PORT automaticamente
PORT = int(os.environ.get("PORT", 8765))

# Sessões ativas: { pin: {"captain": writer, "fo": writer, "aviao": str} }
sessions = {}

# Estatísticas
stats = {"total_sessions": 0, "total_packets": 0}


async def handle_client(reader, writer):
    addr = writer.get_extra_info("peername")
    log.info(f"Nova conexão: {addr}")
    pin_desta_conexao = None
    papel_desta_conexao = None

    try:
        while True:
            # Lê linha (cada mensagem é um JSON terminado em \n)
            data = await reader.readline()
            if not data:
                break

            try:
                msg = json.loads(data.decode("utf-8").strip())
            except json.JSONDecodeError:
                continue

            tipo = msg.get("tipo")

            # ── HANDSHAKE — cliente apresenta PIN e papel ──────────────
            if tipo == "RELAY_JOIN":
                pin   = msg.get("pin")
                papel = msg.get("papel")    # "CAPTAIN" ou "FIRST_OFFICER"
                aviao = msg.get("aviao", "")

                if not pin or not papel:
                    await _send(writer, {"tipo": "ERRO", "motivo": "PIN e papel obrigatórios"})
                    continue

                pin_desta_conexao   = pin
                papel_desta_conexao = papel

                if pin not in sessions:
                    sessions[pin] = {"captain": None, "fo": None, "aviao": aviao}
                    stats["total_sessions"] += 1

                sessao = sessions[pin]

                if papel == "CAPTAIN":
                    sessao["captain"] = writer
                    sessao["aviao"]   = aviao
                    log.info(f"[{pin}] Captain conectado ({aviao})")
                    await _send(writer, {
                        "tipo": "RELAY_OK",
                        "papel": "CAPTAIN",
                        "mensagem": "Aguardando First Officer..."
                    })

                elif papel == "FIRST_OFFICER":
                    # Valida PIN — precisa existir uma sessão com Captain
                    if sessao["captain"] is None:
                        await _send(writer, {
                            "tipo": "ERRO",
                            "motivo": "Nenhum Captain nesta sessão. Verifique o PIN."
                        })
                        continue

                    # Valida avião — deve ser o mesmo do Captain
                    if aviao and sessao["aviao"] and aviao != sessao["aviao"]:
                        await _send(writer, {
                            "tipo": "ERRO",
                            "motivo": f"Avião incompatível: {aviao} vs {sessao['aviao']}"
                        })
                        log.warning(f"[{pin}] Avião incompatível: {aviao} vs {sessao['aviao']}")
                        continue

                    sessao["fo"] = writer
                    log.info(f"[{pin}] First Officer conectado — sessão completa!")

                    # Avisa FO que conectou
                    await _send(writer, {
                        "tipo": "RELAY_OK",
                        "papel": "FIRST_OFFICER",
                        "aviao": sessao["aviao"],
                        "mensagem": "Conectado ao Captain."
                    })

                    # Avisa Captain que FO entrou
                    await _send(sessao["captain"], {
                        "tipo": "FO_CONECTADO",
                        "mensagem": "First Officer conectado."
                    })

            # ── PACOTE DE DADOS — faz bridge para o parceiro ───────────
            elif tipo in ("DR", "CMD") and pin_desta_conexao:
                sessao = sessions.get(pin_desta_conexao)
                if not sessao:
                    continue

                stats["total_packets"] += 1

                # Envia para o parceiro (não para si mesmo)
                if papel_desta_conexao == "CAPTAIN":
                    parceiro = sessao.get("fo")
                else:
                    parceiro = sessao.get("captain")

                if parceiro:
                    try:
                        await _send(parceiro, msg)
                    except Exception:
                        log.warning(f"[{pin_desta_conexao}] Parceiro desconectado.")

            # ── PING — mantém conexão viva (evita sleep do Render) ─────
            elif tipo == "PING":
                await _send(writer, {"tipo": "PONG"})

            # ── DISCONNECT ─────────────────────────────────────────────
            elif tipo == "RELAY_LEAVE":
                break

    except asyncio.IncompleteReadError:
        pass
    except Exception as e:
        log.error(f"Erro com {addr}: {e}")
    finally:
        # Remove da sessão
        if pin_desta_conexao and pin_desta_conexao in sessions:
            sessao = sessions[pin_desta_conexao]
            if papel_desta_conexao == "CAPTAIN":
                sessao["captain"] = None
                # Avisa FO
                if sessao["fo"]:
                    await _send(sessao["fo"], {
                        "tipo": "PARCEIRO_SAIU",
                        "mensagem": "Captain desconectou."
                    })
            elif papel_desta_conexao == "FIRST_OFFICER":
                sessao["fo"] = None
                if sessao["captain"]:
                    await _send(sessao["captain"], {
                        "tipo": "PARCEIRO_SAIU",
                        "mensagem": "First Officer desconectou."
                    })

            # Remove sessão se ambos saíram
            if sessao["captain"] is None and sessao["fo"] is None:
                del sessions[pin_desta_conexao]
                log.info(f"[{pin_desta_conexao}] Sessão encerrada.")

        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        log.info(f"Conexão encerrada: {addr}")


async def _send(writer, obj):
    """Envia um objeto JSON para o cliente."""
    if writer and not writer.is_closing():
        data = json.dumps(obj, separators=(",", ":")) + "\n"
        writer.write(data.encode("utf-8"))
        await writer.drain()


async def status_periodico():
    """Loga estatísticas a cada 5 minutos."""
    while True:
        await asyncio.sleep(300)
        log.info(f"STATUS — Sessões ativas: {len(sessions)} | "
                 f"Total sessões: {stats['total_sessions']} | "
                 f"Pacotes: {stats['total_packets']}")


async def main():
    server = await asyncio.start_server(handle_client, "0.0.0.0", PORT)
    log.info(f"DuoPilot BR Relay iniciado na porta {PORT}")
    log.info("Aguardando conexões...")

    asyncio.create_task(status_periodico())

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

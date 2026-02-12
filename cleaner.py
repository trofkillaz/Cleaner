import os
import asyncio
import redis.asyncio as redis
import json


REDIS_1_URL = os.getenv("REDIS_1")
REDIS_2_URL = os.getenv("REDIS_2")


async def cleanup():
    if not REDIS_1_URL or not REDIS_2_URL:
        print("❌ REDIS_1 или REDIS_2 не указаны")
        return

    redis1 = redis.from_url(REDIS_1_URL, decode_responses=True)
    redis2 = redis.from_url(REDIS_2_URL, decode_responses=True)

    print("🔍 Начинаем очистку...")

    # BOOKING
    async for key in redis1.scan_iter("booking:*"):
        try:
            key_type = await redis1.type(key)

            if key_type == "string":
                raw = await redis1.get(key)
                if raw:
                    data = json.loads(raw)
                    if data.get("status") in ["confirmed", "rejected"]:
                        await redis1.delete(key)
                        print(f"🗑 Удалена заявка {key}")

            else:
                # если вдруг это hash или что-то другое
                await redis1.delete(key)

        except Exception as e:
            print(f"Ошибка с ключом {key}: {e}")

    # EVENTS
    async for key in redis2.scan_iter("event:*"):
        try:
            await redis2.delete(key)
            print(f"🗑 Удалён event {key}")
        except Exception as e:
            print(f"Ошибка event {key}: {e}")

    print("✅ Очистка завершена")


if __name__ == "__main__":
    asyncio.run(cleanup())
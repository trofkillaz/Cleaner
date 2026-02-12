import os
import asyncio
import redis.asyncio as redis


REDIS_1_URL = os.getenv("REDIS_1")
REDIS_2_URL = os.getenv("REDIS_2")


async def cleanup():
    if not REDIS_1_URL or not REDIS_2_URL:
        print("❌ REDIS_1 или REDIS_2 не указаны в Variables")
        return

    redis1 = redis.from_url(REDIS_1_URL, decode_responses=True)
    redis2 = redis.from_url(REDIS_2_URL, decode_responses=True)

    print("🔍 Начинаем очистку...")

    # --------------------------
    # Очистка booking заявок
    # --------------------------

    booking_keys = await redis1.keys("booking:*")

    for key in booking_keys:
        data = await redis1.hgetall(key)

        if not data:
            continue

        status = data.get("status")

        # Удаляем только завершённые
        if status in ["confirmed", "rejected"]:
            await redis1.delete(key)
            print(f"🗑 Удалена заявка {key}")

    # --------------------------
    # Очистка event ключей
    # --------------------------

    event_keys = await redis2.keys("event:*")

    for key in event_keys:
        await redis2.delete(key)
        print(f"🗑 Удалён event {key}")

    print("✅ Очистка завершена")


if __name__ == "__main__":
    asyncio.run(cleanup())
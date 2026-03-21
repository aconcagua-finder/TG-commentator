"""Account rebranding — batch identity generation, real profile parsing,
and full rebrand workflow.

Extracted from commentator.py.
"""

import ast
import base64
import logging
import os
import random
import urllib.parse
from datetime import datetime, timezone

import httpx
import openai
from google import genai
from google.genai import types as genai_types
from telethon.tl.functions.channels import JoinChannelRequest, GetFullChannelRequest

from app_paths import SETTINGS_FILE
from app_storage import save_json
from services.account_utils import get_model_setting, gemini_model_candidates
from services.profile import update_account_profile

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DB helper (local, like other service modules)
# ---------------------------------------------------------------------------

def _db_connect():
    from db.connection import get_connection
    return get_connection()


# ---------------------------------------------------------------------------
# Generate batch identities via AI
# ---------------------------------------------------------------------------

async def generate_batch_identities(topic, count, provider, api_key, *, current_settings: dict):
    """Generate a list of unique names via AI provider.

    Parameters
    ----------
    current_settings : dict
        Settings dict for model name resolution.
    """
    system_prompt = (
        f"Ты — генератор профилей для Telegram. Тематика: '{topic}'.\n"
        f"Сгенерируй список из {count} УНИКАЛЬНЫХ имен и фамилий.\n"
        f"Имена должны быть разными, креативными, реалистичными или сленговыми.\n"
        f"Верни ответ ТОЛЬКО в формате чистого списка Python, без лишних слов:\n"
        f"['Имя Фамилия', 'Имя Фамилия', ...]"
    )

    try:
        content = ""
        if provider in {"openai", "openrouter", "deepseek"}:
            base_url = None
            default_headers = None
            if provider == "deepseek":
                base_url = "https://api.deepseek.com"
            elif provider == "openrouter":
                base_url = "https://openrouter.ai/api/v1"
                default_headers = {
                    "HTTP-Referer": os.getenv("OPENROUTER_REFERRER", "http://localhost"),
                    "X-Title": os.getenv("OPENROUTER_TITLE", "AI-Центр"),
                }

            model_key = "deepseek_chat" if provider == "deepseek" else ("openrouter_chat" if provider == "openrouter" else "openai_chat")
            model_name = get_model_setting(current_settings, model_key)
            client = openai.AsyncOpenAI(api_key=api_key, base_url=base_url, default_headers=default_headers)
            create_kwargs = {
                "messages": [{"role": "user", "content": system_prompt}],
                "model": model_name,
                "temperature": 1.4,
            }
            if provider == "openai":
                create_kwargs["max_completion_tokens"] = 512
            else:
                create_kwargs["max_tokens"] = 512
            completion = await client.chat.completions.create(**create_kwargs)
            content = completion.choices[0].message.content.strip()
        elif provider == 'gemini':
            for model_name in gemini_model_candidates(current_settings, "gemini_names"):
                try:
                    async with genai.Client(api_key=api_key).aio as aclient:
                        response = await aclient.models.generate_content(
                            model=model_name,
                            contents=system_prompt,
                            config=genai_types.GenerateContentConfig(
                                temperature=1.4,
                                max_output_tokens=256,
                            ),
                        )
                    content = (response.text or "").strip()
                    break
                except Exception:
                    content = ""
                    continue

        content = content.replace("```python", "").replace("```json", "").replace("```", "")
        names_list = ast.literal_eval(content)
        if isinstance(names_list, list):
            return names_list
        return []
    except Exception as e:
        logger.error(f"Ошибка массовой генерации имен: {e}")
        return []


# ---------------------------------------------------------------------------
# Collect real identities from a Telegram channel
# ---------------------------------------------------------------------------

async def get_real_identities_from_channel(client, source_channel, limit=200):
    identities = []
    seen_ids = set()

    try:
        with _db_connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user_id FROM used_identities")
            rows = cursor.fetchall()
            for row in rows:
                seen_ids.add(row[0])

    except Exception as e:
        logger.error(f"Ошибка загрузки использованных ID из БД: {e}")

    try:
        if not client.is_connected():
            await client.connect()

        async for message in client.iter_messages(source_channel, limit=limit * 20):
            if not message.sender or not hasattr(message.sender, 'id'):
                continue

            user_id = message.sender.id
            if user_id in seen_ids:
                continue

            if hasattr(message.sender, 'bot') and message.sender.bot:
                continue

            if hasattr(message.sender, 'scam') and message.sender.scam:
                continue

            seen_ids.add(user_id)

            first_name = getattr(message.sender, 'first_name', '') or ''
            last_name = getattr(message.sender, 'last_name', '') or ''

            if not first_name.strip():
                continue

            identities.append({
                'user_id': user_id,
                'first_name': first_name,
                'last_name': last_name,
                'user_entity': message.sender,
                'has_photo': getattr(message.sender, 'photo', None) is not None
            })

            if len(identities) >= limit * 4:
                break

    except Exception as e:
        logger.error(f"Ошибка при сборе реальных профилей: {e}")

    random.shuffle(identities)
    return identities[:limit]


# ---------------------------------------------------------------------------
# Full rebrand workflow
# ---------------------------------------------------------------------------

async def run_rebrand_logic(api_id, api_hash, *, current_settings: dict, active_clients: dict):
    """Run the full rebrand process for all active clients.

    Parameters
    ----------
    current_settings : dict
        Global settings dict (mutated in-place to update rebrand_task status).
    active_clients : dict
        session_name -> CommentatorClient mapping.
    """
    task = current_settings.get('rebrand_task')
    if not task or task.get('status') != 'pending':
        return

    raw_source = task.get('source_value', task.get('source_channel'))
    is_channel = task.get('is_channel', True)

    if raw_source:
        source_val = raw_source.strip().replace('https://t.me/', '').replace('http://t.me/', '').replace('t.me/', '').replace('@', '')
        if '+' in source_val and not source_val.startswith('+'):
            pass
    else:
        source_val = "abstract"

    logger.info(f"🚀 Запуск процесса ребрендинга: {task['topic']} | Источник: {source_val}")

    provider = current_settings.get('ai_provider', 'gemini')
    api_key = current_settings.get('api_keys', {}).get(provider)
    openai_key = current_settings.get('api_keys', {}).get('openai')

    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    clients_list = list(active_clients.values())
    real_identities = []
    generated_names = []

    using_real_profiles = False
    successful_parser = None

    if is_channel and clients_list:
        logger.info(f"📥 Начинаем поиск донора для парсинга канала @{source_val}...")

        for parser_wrapper in clients_list:
            try:
                parser_client = parser_wrapper.client
                if not parser_client.is_connected():
                    await parser_client.connect()

                try:
                    entity = await parser_client.get_entity(source_val)
                except ValueError:
                    try:
                        await parser_client(JoinChannelRequest(source_val))
                        entity = await parser_client.get_entity(source_val)
                    except Exception:
                        raise Exception(f"Не удалось найти канал {source_val}")

                target_entity = entity
                try:
                    full_channel = await parser_client(GetFullChannelRequest(entity))
                    if full_channel.full_chat.linked_chat_id:
                        linked_id = full_channel.full_chat.linked_chat_id
                        target_entity = await parser_client.get_entity(linked_id)
                        logger.info(f"💬 У канала есть чат с обсуждениями: {target_entity.title}. Будем парсить его.")
                    else:
                        logger.warning("⚠️ У канала НЕТ привязанного чата. Попробуем парсить сам канал (если это группа).")
                except Exception as e:
                    logger.warning(f"Не удалось получить инфо о привязанном чате: {e}")

                logger.info(f"🔄 Пробуем парсить через аккаунт: {parser_wrapper.session_name}")

                needed_count = len(clients_list) + 10
                real_identities = await get_real_identities_from_channel(parser_client, target_entity, limit=needed_count)

                if real_identities:
                    logger.info(f"✅ Успешно собрано {len(real_identities)} профилей через {parser_wrapper.session_name}")
                    using_real_profiles = True
                    successful_parser = parser_client
                    break
                else:
                    logger.warning(f"⚠️ Аккаунт {parser_wrapper.session_name} зашел, но не нашел сообщений с людьми.")
            except Exception as e:
                logger.warning(f"⚠️ Аккаунт {parser_wrapper.session_name} не смог спарсить канал: {e}")
                continue

        if not using_real_profiles:
            logger.error("❌ Ни один аккаунт не смог получить доступ к каналу-донору. Переключаюсь на генерацию AI.")
            is_channel = False

    if not using_real_profiles:
        needed_count = len(clients_list) + 5
        generated_names = await generate_batch_identities(
            task['topic'], needed_count, provider, api_key,
            current_settings=current_settings,
        )
        logger.info(f"✅ Сгенерировано {len(generated_names)} имен через AI.")

    identity_index = 0

    for client_wrapper in clients_list:
        try:
            photo_path = os.path.join(base_dir, f"avatar_{client_wrapper.session_name}.jpg")
            got_photo = False
            first_name = ""
            last_name = ""
            current_identity_user_id = None

            if using_real_profiles and successful_parser:
                identity = real_identities[identity_index % len(real_identities)]
                first_name = identity['first_name']
                last_name = identity['last_name']
                current_identity_user_id = identity.get('user_id')

                if identity['has_photo']:
                    try:
                        await successful_parser.download_profile_photo(identity['user_entity'], file=photo_path)
                        if os.path.exists(photo_path):
                            got_photo = True
                    except Exception as e:
                        logger.error(f"Ошибка скачивания фото реального юзера: {e}")

                identity_index += 1
            else:
                if generated_names:
                    full_name = generated_names.pop(0)
                    parts = full_name.split(' ', 1)
                    first_name = parts[0].replace('"', '').replace("'", "")
                    last_name = (parts[1] if len(parts) > 1 else "").replace('"', '').replace("'", "")
                else:
                    first_name = f"User{random.randint(100, 999)}"
                    last_name = ""

                if not got_photo and openai_key:
                    try:
                        image_model = get_model_setting(current_settings, "openai_image")
                        logger.info(f"🎨 Генерирую аватар через OpenAI ({image_model}) для {client_wrapper.session_name}...")
                        openai_client = openai.AsyncOpenAI(api_key=openai_key)
                        dalle_prompt = f"Avatar profile picture for social media, topic: {raw_source}, style: realistic, high quality, professional headshot"

                        for model_to_try in [image_model, "dall-e-3"]:
                            if not model_to_try:
                                continue
                            try:
                                params = {"model": model_to_try, "prompt": dalle_prompt, "size": "1024x1024", "n": 1}
                                if model_to_try == "dall-e-3":
                                    params["quality"] = "standard"
                                response = await openai_client.images.generate(**params)

                                image_item = response.data[0]
                                b64_json = getattr(image_item, "b64_json", None)
                                image_url = getattr(image_item, "url", None)

                                if b64_json:
                                    image_bytes = base64.b64decode(b64_json)
                                    with open(photo_path, 'wb') as f:
                                        f.write(image_bytes)
                                    got_photo = True
                                    break

                                if image_url:
                                    async with httpx.AsyncClient(timeout=30.0) as http_client:
                                        resp = await http_client.get(image_url)
                                        if resp.status_code == 200:
                                            with open(photo_path, 'wb') as f:
                                                f.write(resp.content)
                                            got_photo = True
                                            break
                            except Exception:
                                continue
                    except Exception:
                        pass

                if not got_photo:
                    encoded_query = urllib.parse.quote(source_val)
                    seed = random.randint(1, 9999999)
                    url = f"https://image.pollinations.ai/prompt/avatar%20{encoded_query}%20{seed}?width=500&height=500&nologo=true&model=flux"

                    async with httpx.AsyncClient(timeout=60.0) as http_client:
                        resp = await http_client.get(url)
                        if resp.status_code == 200:
                            with open(photo_path, 'wb') as f:
                                f.write(resp.content)
                            got_photo = True

                    import asyncio
                    await asyncio.sleep(3)

            await update_account_profile(
                client_wrapper.client,
                first_name=first_name,
                last_name=last_name,
                avatar_path=photo_path if got_photo else None,
            )
            success = True

            if success and current_identity_user_id:
                try:
                    with _db_connect() as conn:
                        conn.execute("INSERT INTO used_identities (user_id, date_used) VALUES (?, ?) ON CONFLICT DO NOTHING",
                                     (current_identity_user_id, datetime.now(timezone.utc).isoformat()))
                        conn.commit()
                except Exception as db_e:
                    logger.error(f"Ошибка сохранения использованного ID {current_identity_user_id}: {db_e}")

            if got_photo and os.path.exists(photo_path):
                os.remove(photo_path)

            logger.info(f"✅ Аккаунт {client_wrapper.session_name} обновлен: {first_name} {last_name}")

            import asyncio
            await asyncio.sleep(random.randint(5, 12))

        except Exception as e:
            logger.error(f"Ошибка ребрендинга для {client_wrapper.session_name}: {repr(e)}")

    current_settings['rebrand_task']['status'] = 'completed'
    save_json(SETTINGS_FILE, current_settings)
    logger.info("🏁 Задача по ребрендингу завершена.")

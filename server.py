import asyncio
import websockets
import json
import os
import requests
import html
import random
from tiktok_live_events import TikTokLive
from aiohttp import web

# دالة لجلب الأسئلة من مصدر عربي مباشر (بدون ترجمة)
def fetch_questions():
    # استخدام واجهة برمجية عربية مفتوحة للأسئلة
    url = "https://opentdb.com/api.php?amount=10&category=17&type=multiple"
    questions = []
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        if data['response_code'] == 0:
            for item in data['results']:
                # نستخدم النص الإنجليزي كما هو، ولكننا سنضيف قاموس ترجمة يدوي للأسئلة الشائعة
                # أو نستخدم أسئلة عربية مبرمجة مسبقاً لتجنب مشاكل الترجمة
                pass
    except Exception as e:
        print(f"Error: {e}")
    
    # قائمة أسئلة عربية جاهزة ومضمونة (يمكنك إضافة المزيد هنا)
    arabic_questions = [
        {"q": "ما هي عاصمة المملكة العربية السعودية؟", "opts": ["A) جدة", "B) الرياض", "C) مكة", "D) الدمام"], "ans": "B"},
        {"q": "كم عدد أركان الإسلام؟", "opts": ["A) 3", "B) 4", "C) 5", "D) 6"], "ans": "C"},
        {"q": "ما هو أطول نهر في العالم؟", "opts": ["A) النيل", "B) الأمازون", "C) المسيسيبي", "D) اليانغتسي"], "ans": "A"},
        {"q": "ما هو العنصر الكيميائي الذي رمزه O؟", "opts": ["A) الذهب", "B) الأكسجين", "C) الحديد", "D) الكربون"], "ans": "B"},
        {"q": "في أي عام هبط الإنسان على سطح القمر لأول مرة؟", "opts": ["A) 1965", "B) 1969", "C) 1972", "D) 1959"], "ans": "B"},
        {"q": "ما هي أكبر قارة في العالم من حيث المساحة؟", "opts": ["A) أفريقيا", "B) أوروبا", "C) آسيا", "D) أمريكا الشمالية"], "ans": "C"},
        {"q": "ما هو الحيوان الذي يُعرف باسم 'سفينة الصحراء'؟", "opts": ["A) الحصان", "B) الجمل", "C) الفيل", "D) الحمار الوحشي"], "ans": "B"},
        {"q": "كم عدد ألوان قوس قزح؟", "opts": ["A) 5", "B) 6", "C) 7", "D) 8"], "ans": "C"},
        {"q": "ما هو أقسى مادة طبيعية على وجه الأرض؟", "opts": ["A) الحديد", "B) الألماس", "C) الذهب", "D) الجرانيت"], "ans": "B"},
        {"q": "ما هي العملة الرسمية في اليابان؟", "opts": ["A) اليوان", "B) الوون", "C) الين", "D) الروبية"], "ans": "C"}
    ]
    
    # خلط الأسئلة وإرجاعها
    random.shuffle(arabic_questions)
    return arabic_questions

QUESTIONS = fetch_questions()
current_q_index = 0
clients = set()
main_loop = None
timer_task = None
winner_found = False
live = TikTokLive("7_upu")
scores = {}

async def question_timer():
    global current_q_index, winner_found
    await asyncio.sleep(30)
    if not winner_found:
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(json.dumps({"type": "timeout"})), main_loop)
        await next_q()

async def next_q():
    global current_q_index, winner_found, timer_task, QUESTIONS
    winner_found = False
    current_q_index += 1
    await asyncio.sleep(3)
    
    # إذا انتهت الأسئلة، نعيد خلطها ونبدأ من جديد
    if current_q_index >= len(QUESTIONS):
        print("🔄 إعادة خلط الأسئلة...")
        random.shuffle(QUESTIONS)
        current_q_index = 0
        
    if len(QUESTIONS) > 0:
        new_q = QUESTIONS[current_q_index]
        q_json = json.dumps({"type": "new_question", "question": new_q["q"], "options": new_q["opts"]})
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(q_json), main_loop)
        
        if timer_task:
            timer_task.cancel()
        timer_task = asyncio.create_task(question_timer())
    else:
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(json.dumps({"type": "end"})), main_loop)

async def process_answer(user_name, answer):
    global winner_found
    if winner_found or current_q_index >= len(QUESTIONS):
        return
        
    current_ans = QUESTIONS[current_q_index]["ans"]
    if answer == current_ans:
        winner_found = True
        if timer_task:
            main_loop.call_soon_threadsafe(timer_task.cancel)
            
        scores[user_name] = scores.get(user_name, 0) + 10
        current_score = scores[user_name]
        
        winner_msg = f"{user_name} | Coins: {current_score} 🪙"
        
        msg_json = json.dumps({"type": "winner", "name": winner_msg})
        for ws in clients.copy():
            asyncio.run_coroutine_threadsafe(ws.send_str(msg_json), main_loop)
        
        await next_q()

@live.on('chat')
def on_chat(e):
    comment = e['comment'].strip().upper()
    user = e['user'].get('nickname', e['user']['uniqueId'])
    asyncio.run_coroutine_threadsafe(process_answer(user, comment), main_loop)

async def websocket_handler(request):
    global main_loop, timer_task
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients.add(ws)
    
    if current_q_index < len(QUESTIONS):
        q = QUESTIONS[current_q_index]
        await ws.send_str(json.dumps({"type": "new_question", "question": q["q"], "options": q["opts"]}))
        if timer_task is None:
            timer_task = asyncio.create_task(question_timer())
    else:
        await ws.send_str(json.dumps({"type": "end"}))
        
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("type") == "app_answer":
                    asyncio.create_task(process_answer(data.get("user"), data.get("answer")))
    except:
        pass
    finally:
        clients.remove(ws)
    return ws

async def main():
    global main_loop
    main_loop = asyncio.get_running_loop()
    asyncio.create_task(live.run())
    
    app = web.Application()
    app.router.add_get('/', websocket_handler)
    
    port = int(os.environ.get("PORT", 10000))
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    print(f"🚀 الخادم جاهز على المنفذ {port} بأسئلة عربية")
    await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())